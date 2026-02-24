package repository

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	rsock "github.com/ttab/elephant-api/repositorysocket"
	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
	"github.com/twitchtv/twirp"
	"github.com/viccon/sturdyc"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 10 * 1024
)

func NewSocketHandler(
	ctx context.Context,
	logger *slog.Logger,
	metricsRegisterer prometheus.Registerer,
	store DocStore,
	cache *DocCache,
	auth elephantine.AuthInfoParser,
	socketKey *ecdsa.PublicKey,
	corsHosts []string,
) (*SocketHandler, error) {
	docStream, err := NewDocumentStream(
		ctx, logger, metricsRegisterer, store)
	if err != nil {
		return nil, fmt.Errorf("create document stream: %w", err)
	}

	rateLimiterCache := sturdyc.New[*rate.Limiter](
		5000, 1,
		1*time.Minute, 10,
		sturdyc.WithEvictionInterval(30*time.Second))

	h := SocketHandler{
		runCtx: ctx,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 10240,
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")

				switch origin {
				case "":
					return true
				case "null":
					return false
				}

				originURL, err := url.Parse(origin)
				if err != nil {
					return false
				}

				hostname := originURL.Hostname()
				secureOrigin := originURL.Scheme == "https"

				// Require https for everything that isn't
				// localhost.
				if hostname != "localhost" && !secureOrigin {
					return false
				}

				return slices.Contains(corsHosts, hostname)
			},
		},
		log:       logger,
		store:     store,
		cache:     cache,
		stream:    docStream,
		auth:      auth,
		socketKey: socketKey,
		rate:      rateLimiterCache,
	}

	prom := elephantine.NewMetricsHelper(metricsRegisterer)

	prom.CounterVec(&h.socketRejected, prometheus.CounterOpts{
		Name: "repository_websocket_rate_limited_total",
	}, []string{"reason"})

	prom.Gauge(&h.openSockets, prometheus.GaugeOpts{
		Name: "repository_open_sockets",
	})

	prom.CounterVec(&h.socketCall, prometheus.CounterOpts{
		Name: "repository_websocket_call_total",
	}, []string{"method"})

	prom.CounterVec(&h.socketResponse, prometheus.CounterOpts{
		Name: "repository_websocket_response_total",
	}, []string{"method", "status", "response"})

	if err := prom.Err(); err != nil {
		return nil, fmt.Errorf("register metrics: %w", err)
	}

	return &h, nil
}

type SocketHandler struct {
	runCtx    context.Context
	log       *slog.Logger
	upgrader  websocket.Upgrader
	store     DocStore
	cache     *DocCache
	stream    *DocumentStream
	auth      elephantine.AuthInfoParser
	socketKey *ecdsa.PublicKey
	rate      *sturdyc.Client[*rate.Limiter]

	openSockets    prometheus.Gauge
	socketRejected *prometheus.CounterVec
	socketCall     *prometheus.CounterVec
	socketResponse *prometheus.CounterVec
}

func (h *SocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.URL.Path, "/websocket/")
	if !ok {
		http.Error(w, "no socket token", http.StatusUnauthorized)

		h.socketRejected.WithLabelValues("no_token").Inc()

		return
	}

	tok, err := VerifySocketToken(token, h.socketKey)
	if err != nil {
		http.Error(w, "invalid socket token", http.StatusUnauthorized)

		h.socketRejected.WithLabelValues("invalid_token").Inc()

		return
	}

	rate, err := h.rate.GetOrFetch(
		r.Context(), strconv.FormatUint(tok.ID, 10),
		func(_ context.Context) (*rate.Limiter, error) {
			return rate.NewLimiter(rate.Every(5*time.Second), 1), nil
		})
	if err != nil {
		http.Error(w, "internal error: rate limiting", http.StatusInternalServerError)
	}

	if !rate.Allow() {
		http.Error(w, "rate limited", http.StatusTooManyRequests)

		h.socketRejected.WithLabelValues("rate_limit").Inc()

		return
	}

	if tok.Expires.Before(time.Now()) {
		http.Error(w, "expired socket token", http.StatusUnauthorized)

		h.socketRejected.WithLabelValues("expired_token").Inc()

		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// An error response has already been sent to the client by the
		// Upgrader at this point.
		h.log.Warn("failed to upgrade to websocket connection",
			elephantine.LogKeyError, err,
			"remote_addr", r.RemoteAddr,
		)

		h.socketRejected.WithLabelValues("upgrade_failed").Inc()

		return
	}

	sess := NewSocketSession(
		conn, h.log, h.store, h.cache, h.stream, h.auth,
		h.socketCall, h.socketResponse,
	)

	h.openSockets.Inc()
	sess.Run(r.Context())
	h.openSockets.Dec()
}

func NewSocketSession(
	conn *websocket.Conn,
	log *slog.Logger,
	store DocStore,
	cache *DocCache,
	stream *DocumentStream,
	authParser elephantine.AuthInfoParser,
	socketCall *prometheus.CounterVec,
	socketResponse *prometheus.CounterVec,
) *SocketSession {
	return &SocketSession{
		conn:           conn,
		log:            log,
		store:          store,
		cache:          cache,
		stream:         stream,
		authParser:     authParser,
		calls:          make(chan *CallHandle, 8),
		responses:      make(chan *responseHandle, 16),
		sets:           make(map[string]*documentSetHandle),
		eventlogs:      make(map[string]*eventlogHandle),
		socketCall:     socketCall,
		socketResponse: socketResponse,
	}
}

type SocketSession struct {
	conn *websocket.Conn

	m        sync.RWMutex
	auth     *elephantine.AuthInfo
	identity []string

	socketCall     *prometheus.CounterVec
	socketResponse *prometheus.CounterVec

	authExpired *time.Ticker

	log        *slog.Logger
	store      DocStore
	cache      *DocCache
	stream     *DocumentStream
	authParser elephantine.AuthInfoParser

	calls     chan *CallHandle
	responses chan *responseHandle

	sets      map[string]*documentSetHandle
	eventlogs map[string]*eventlogHandle
}

func (s *SocketSession) setAuth(auth *elephantine.AuthInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	s.auth = auth

	s.identity = []string{auth.Claims.Subject}
	s.identity = append(s.identity, auth.Claims.Units...)

	for _, h := range s.sets {
		h.Set.IdentityUpdated(s.identity)
	}

	// Reset the auth expiry timer.
	s.authExpired.Reset(time.Until(auth.Claims.ExpiresAt.Time))
}

func (s *SocketSession) getAuth() (*elephantine.AuthInfo, []string) {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.auth, s.identity
}

func (s *SocketSession) Run(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.authExpired = time.NewTicker(5 * time.Second)

	group, gCtx := errgroup.WithContext(runCtx)

	group.Go(func() error {
		err := elephantine.CallWithRecover(gCtx, s.readLoop)
		if err != nil {
			return fmt.Errorf("run read loop: %w", err)
		}

		return nil
	})

	group.Go(func() error {
		err := elephantine.CallWithRecover(gCtx, s.handlerLoop)
		if err != nil {
			return fmt.Errorf("run handler loop: %w", err)
		}

		return nil
	})

	group.Go(func() error {
		err := elephantine.CallWithRecover(gCtx, s.writeLoop)
		if err != nil {
			return fmt.Errorf("run write loop: %w", err)
		}

		return nil
	})

	err := group.Wait()
	if err != nil && !errors.Is(err, errCloseSocket) {
		s.log.Warn("socket session closed",
			elephantine.LogKeyError, err)
	}
}

func (s *SocketSession) readLoop(ctx context.Context) (outErr error) {
	defer elephantine.Close("websocket", s.conn, &outErr)

	s.conn.SetReadLimit(maxMessageSize)

	err := s.conn.SetReadDeadline(time.Now().Add(pongWait))
	if err != nil {
		return fmt.Errorf("set initial read deadline: %w", err)
	}

	// Move the read deadline for every pong we receive.
	s.conn.SetPongHandler(func(string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := s.conn.SetReadDeadline(time.Now().Add(pongWait))
			if err != nil {
				return fmt.Errorf("update read deadline on pong: %w", err)
			}
		}

		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var (
			fatal  errSockFatal
			closed *websocket.CloseError
		)

		callHandle, err := s.readCall()

		switch {
		case errors.As(err, &closed):
			return nil
		case errors.As(err, &fatal):
			return err
		case err != nil:
			s.log.Warn("failed to read message from websocket",
				elephantine.LogKeyError, err)

			continue
		}

		if callHandle.Call.CallId == "" {
			s.Respond(ctx, callHandle, &rsock.Response{
				Error: &rsock.Error{
					ErrorCode:    string(twirp.InvalidArgument),
					ErrorMessage: "call_id is required",
				},
			}, true)

			return nil
		}

		auth, _ := s.getAuth()

		if auth != nil && auth.Claims.ExpiresAt.Before(time.Now()) {
			auth = nil
		}

		if auth == nil && callHandle.Call.Authenticate == nil {
			s.Respond(ctx, callHandle, &rsock.Response{
				Error: &rsock.Error{
					ErrorCode:    string(twirp.Unauthenticated),
					ErrorMessage: "Missing or expired authentication token",
				},
			}, true)

			return nil
		}

		s.calls <- callHandle
	}
}

func (s *SocketSession) handlerLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case call := <-s.calls:
			final := s.runHandler(ctx, call)
			if final {
				return nil
			}
		}
	}
}

var (
	handlerAuthenticate     = "Authenticate"
	handlerGetDocuments     = "GetDocuments"
	handlerCloseDocumentSet = "CloseDocumentSet"
	handlerGetEventlog      = "GetEventlog"
	handlerCloseEventlog    = "CloseEventlog"
)

func (s *SocketSession) runHandler(ctx context.Context, call *CallHandle) bool {
	var (
		resp *rsock.Response
		rErr *rsock.Error
	)

	var (
		handler     func(context.Context, *CallHandle) (*rsock.Response, *rsock.Error)
		handlerName string
	)

	switch {
	case call.Call.Authenticate != nil:
		handlerName = handlerAuthenticate
		handler = s.handleAuthenticate
	case call.Call.GetDocuments != nil:
		handlerName = handlerGetDocuments
		handler = s.handleGetDocuments
	case call.Call.CloseDocumentSet != nil:
		handlerName = handlerCloseDocumentSet
		handler = s.handleCloseDocumentSet
	case call.Call.GetEventlog != nil:
		handlerName = handlerGetEventlog
		handler = s.handleGetEventlog
	case call.Call.CloseEventlog != nil:
		handlerName = handlerCloseEventlog
		handler = s.handleCloseEventlog
	default:
		s.Respond(ctx, call, &rsock.Response{
			Error: SockErrorf(string(twirp.InvalidArgument), "unknown call type"),
		}, false)

		return false
	}

	call.Method = handlerName

	s.socketCall.WithLabelValues(call.Method).Inc()

	resp, rErr = handler(ctx, call)
	if rErr != nil {
		// We always bail immediately on authentication errors.
		final := rErr.ErrorCode == string(twirp.Unauthenticated)

		s.Respond(ctx, call, &rsock.Response{
			Error: rErr,
		}, final)

		return final
	}

	resp.Handled = true

	s.Respond(ctx, call, resp, false)

	return false
}

func SockErrorf(code string, format string, a ...any) *rsock.Error {
	return &rsock.Error{
		ErrorCode:    code,
		ErrorMessage: fmt.Sprintf(format, a...),
	}
}

func (s *SocketSession) handleAuthenticate(
	_ context.Context, callHandle *CallHandle,
) (*rsock.Response, *rsock.Error) {
	req := callHandle.Call.Authenticate

	auth, err := s.authParser.AuthInfoFromToken(req.Token)
	if err != nil {
		return nil, SockErrorf(
			string(twirp.Unauthenticated),
			"Invalid token")
	}

	s.setAuth(auth)

	return &rsock.Response{}, nil
}

func (s *SocketSession) handleGetDocuments(
	ctx context.Context, callHandle *CallHandle,
) (*rsock.Response, *rsock.Error) {
	req := callHandle.Call.GetDocuments

	if req.SetName == "" {
		return nil, SockErrorf(string(twirp.InvalidArgument),
			"set_name is required")
	}

	if req.Type == "" {
		return nil, SockErrorf(string(twirp.InvalidArgument),
			"type is required")
	}

	var timespan *Timespan

	if req.Timespan != nil {
		ts, err := TimespanFromRPC(req.Timespan)
		if err != nil {
			return nil, SockErrorf(string(twirp.InvalidArgument),
				"invalid timespan: %v", err)
		}

		timespan = &ts
	}

	var includeExtractors []*newsdoc.ValueExtractor

	for _, extr := range req.Include {
		inc, err := newsdoc.ValueExtractorFromString(extr)
		if err != nil {
			return nil, SockErrorf(string(twirp.InvalidArgument),
				"invalid include extractor %q: %v", extr, err)
		}

		includeExtractors = append(includeExtractors, inc)
	}

	var subsetExtractors []*newsdoc.ValueExtractor

	for i, expr := range req.Subset {
		ve, err := newsdoc.ValueExtractorFromString(expr)
		if err != nil {
			return nil, SockErrorf(string(twirp.InvalidArgument),
				"invalid subset expression subset[%d]: %v", i, err)
		}

		subsetExtractors = append(subsetExtractors, ve)
	}

	var inclusionSubsets map[string][]*newsdoc.ValueExtractor

	for docType, expr := range req.InclusionSubsets {
		ve, err := newsdoc.ValueExtractorFromString(expr)
		if err != nil {
			return nil, SockErrorf(string(twirp.InvalidArgument),
				"invalid inclusion subset expression for %q: %v",
				docType, err)
		}

		if inclusionSubsets == nil {
			inclusionSubsets = make(map[string][]*newsdoc.ValueExtractor)
		}

		inclusionSubsets[docType] = append(
			inclusionSubsets[docType], ve)
	}

	previous, ok := s.sets[req.SetName]
	if ok {
		previous.Close()
	}

	handle := newDocumentSetHandle(ctx, callHandle, s)

	_, identity := s.getAuth()

	docSet := newDocumentSet(
		req.Type, req.IncludeAcls, req.SetName,
		timespan, req.Labels,
		includeExtractors,
		subsetExtractors,
		inclusionSubsets,
		identity, s.cache, s.store, handle)

	handle.Set = docSet

	err := docSet.Initialise(handle.ctx, s.stream)
	if err != nil {
		handle.Close()

		return nil, SockErrorf(string(twirp.Internal),
			"initialise document set: %v", err)
	}

	go docSet.ProcessingLoop(handle.ctx)

	s.sets[req.SetName] = handle

	return &rsock.Response{}, nil
}

func (s *SocketSession) handleCloseDocumentSet(
	_ context.Context, callHandle *CallHandle,
) (*rsock.Response, *rsock.Error) {
	req := callHandle.Call.CloseDocumentSet

	if req.SetName == "" {
		return nil, SockErrorf(string(twirp.InvalidArgument),
			"set_name is required")
	}

	set, ok := s.sets[req.SetName]
	if ok {
		set.Close()

		delete(s.sets, req.SetName)
	}

	return &rsock.Response{}, nil
}

func (s *SocketSession) handleGetEventlog(
	ctx context.Context, callHandle *CallHandle,
) (*rsock.Response, *rsock.Error) {
	req := callHandle.Call.GetEventlog

	auth, _ := s.getAuth()
	if !auth.Claims.HasScope(ScopeEventlogRead) {
		return nil, SockErrorf(string(twirp.PermissionDenied),
			"the %s scope is required", ScopeEventlogRead)
	}

	if req.Name == "" {
		return nil, SockErrorf(string(twirp.InvalidArgument),
			"name is required")
	}

	var subsets map[string][]*newsdoc.ValueExtractor

	for docType, expr := range req.TypeSubsets {
		ve, err := newsdoc.ValueExtractorFromString(expr)
		if err != nil {
			return nil, SockErrorf(string(twirp.InvalidArgument),
				"invalid type subset expression for %q: %v",
				docType, err)
		}

		if subsets == nil {
			subsets = make(map[string][]*newsdoc.ValueExtractor)
		}

		subsets[docType] = append(subsets[docType], ve)
	}

	previous, ok := s.eventlogs[req.Name]
	if ok {
		previous.Close()
	}

	docTypes := make(map[string]bool, len(req.DocumentTypes))

	for _, dt := range req.DocumentTypes {
		docTypes[dt] = true
	}

	languages := make(map[string]bool, len(req.Languages))

	for _, l := range req.Languages {
		languages[l] = true
	}

	handleCtx, handleCancel := context.WithCancel(ctx)

	handle := &eventlogHandle{
		call:      callHandle,
		ctx:       handleCtx,
		cancel:    handleCancel,
		responder: s,
		store:     s.store,
		getAuth:   s.getAuth,
		docTypes:  docTypes,
		languages: languages,
		subsets:   subsets,
		process:   make(chan []DocumentStreamItem, 128),
		oosErr:    make(chan struct{}),
	}

	if req.After != nil {
		ok := s.stream.SubscribeFrom(
			handleCtx, *req.After+1, handle.handleStreamItems)
		if !ok {
			handleCancel()

			return nil, SockErrorf("eventlog_resume_oob",
				"resume position is out of bounds")
		}
	} else {
		s.stream.Subscribe(handleCtx, handle.handleStreamItems)
	}

	go handle.processingLoop()

	s.eventlogs[req.Name] = handle

	return &rsock.Response{}, nil
}

func (s *SocketSession) handleCloseEventlog(
	_ context.Context, callHandle *CallHandle,
) (*rsock.Response, *rsock.Error) {
	req := callHandle.Call.CloseEventlog

	if req.Name == "" {
		return nil, SockErrorf(string(twirp.InvalidArgument),
			"name is required")
	}

	handle, ok := s.eventlogs[req.Name]
	if ok {
		handle.Close()

		delete(s.eventlogs, req.Name)
	}

	return &rsock.Response{}, nil
}

type eventlogHandle struct {
	call      *CallHandle
	ctx       context.Context
	cancel    func()
	responder SocketResponder

	store   DocStore
	getAuth func() (*elephantine.AuthInfo, []string)

	docTypes  map[string]bool
	languages map[string]bool
	subsets   map[string][]*newsdoc.ValueExtractor

	process chan []DocumentStreamItem
	oosErr  chan struct{}
	oosOnce sync.Once
}

func (h *eventlogHandle) Close() {
	h.cancel()
}

func (h *eventlogHandle) handleStreamItems(items []DocumentStreamItem) {
	select {
	case <-h.oosErr:
		return
	case h.process <- items:
	default:
		h.oosOnce.Do(func() {
			close(h.oosErr)
		})
	}
}

func (h *eventlogHandle) processingLoop() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.oosErr:
			h.responder.Respond(h.ctx, h.call, &rsock.Response{
				Error: SockErrorf("oos", "processing buffer overflow"),
			}, false)

			return
		case batch := <-h.process:
			h.processBatch(batch)
		}
	}
}

func (h *eventlogHandle) processBatch(items []DocumentStreamItem) {
	var responseItems []*rsock.EventlogItem

	for _, item := range items {
		if len(h.docTypes) > 0 && !h.docTypes[item.Event.Type] {
			continue
		}

		if len(h.languages) > 0 {
			if !h.languages[item.Event.Language] &&
				!h.languages[item.Event.OldLanguage] {
				continue
			}
		}

		rpcItem := &rsock.EventlogItem{
			Event: EventToRPC(item.Event),
		}

		extractors, hasSubsets := h.subsets[item.Event.Type]

		if hasSubsets &&
			item.Event.Event == TypeDocumentVersion &&
			item.Data != nil &&
			h.hasReadPermission(item.Event.UUID) {
			rpcItem.Subset = collectSubset(
				item.Data.Document, extractors)
		}

		responseItems = append(responseItems, rpcItem)
	}

	if len(responseItems) == 0 {
		return
	}

	h.responder.Respond(h.ctx, h.call, &rsock.Response{
		Events: &rsock.EventlogResponse{
			Items: responseItems,
		},
	}, false)
}

func (h *eventlogHandle) hasReadPermission(docUUID uuid.UUID) bool {
	auth, identity := h.getAuth()
	if auth == nil {
		return false
	}

	if auth.Claims.HasScope(ScopeDocumentReadAll) {
		return true
	}

	result, err := h.store.CheckPermissions(h.ctx, CheckPermissionRequest{
		UUID:        docUUID,
		GranteeURIs: identity,
		Permissions: []Permission{ReadPermission},
	})
	if err != nil {
		return false
	}

	return result == PermissionCheckAllowed
}

func (s *SocketSession) Respond(
	ctx context.Context, handle *CallHandle, resp *rsock.Response, final bool,
) {
	if resp == nil {
		resp = &rsock.Response{}
	}

	rh := &responseHandle{
		CallHandle: handle,
		Response:   resp,
		Final:      final,
	}

	status := "ok"

	var response string

	switch {
	case resp.Error != nil:
		status = resp.Error.ErrorCode
		response = "Error"
	case resp.DocumentBatch != nil:
		response = "DocumentBatch"
	case resp.InclusionBatch != nil:
		response = "InclusionBatch"
	case resp.DocumentUpdate != nil:
		response = "DocumentUpdate"
	case resp.Removed != nil:
		response = "Removed"
	case resp.Events != nil:
		response = "Events"
	case resp.Handled:
		response = "Handled"
	}

	var method string

	if handle != nil {
		method = handle.Method
	}

	s.socketResponse.WithLabelValues(method, status, response).Inc()

	select {
	case <-ctx.Done():
	case s.responses <- rh:
	}
}

func (s *SocketSession) writeLoop(ctx context.Context) error {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.authExpired.C:
			// If the current authentication expires we immediately
			// send a final error message to the socket. This
			// message isn't tied to a specific call.
			s.Respond(ctx, nil, &rsock.Response{
				Error: &rsock.Error{
					ErrorCode:    string(twirp.Unauthenticated),
					ErrorMessage: "Authentication expired",
				},
			}, true)
		case message := <-s.responses:
			var fErr errSockFatal

			err := s.writeResponse(message)

			switch {
			case errors.Is(err, errCloseSocket):
				return err
			case errors.As(err, &fErr):
				return err
			case err != nil:
				s.log.Warn("failed to write message to websocket",
					elephantine.LogKeyError, err)

				continue
			}

			if message.Final {
				err := s.writeMessage(websocket.CloseMessage, nil)
				if err != nil {
					s.log.Warn("failed to write close message after final message",
						elephantine.LogKeyError, err)
				}

				return errCloseSocket
			}

		case <-ticker.C:
			err := s.writeMessage(websocket.PingMessage, nil)
			if err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}
		}
	}
}

func (s *SocketSession) writeMessage(messageType int, data []byte) error {
	err := s.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	return s.conn.WriteMessage(messageType, data) //nolint: wrapcheck
}

type CallHandle struct {
	Protobuf bool
	Method   string
	Call     *rsock.Call
}

func (s *SocketSession) readCall() (*CallHandle, error) {
	msgType, r, err := s.conn.NextReader()
	if err != nil {
		return nil, sockFatalf("create reader for next message: %w", err)
	}

	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}

	var (
		call     rsock.Call
		useProto bool
	)

	switch msgType {
	case websocket.BinaryMessage:
		useProto = true

		err := proto.Unmarshal(body, &call)
		if err != nil {
			return nil, fmt.Errorf("unmarshal protobuf message: %w", err)
		}
	case websocket.TextMessage:
		err := protojson.Unmarshal(body, &call)
		if err != nil {
			return nil, fmt.Errorf("unmarshal json message: %w", err)
		}
	}

	return &CallHandle{
		Protobuf: useProto,
		Call:     &call,
	}, nil
}

type responseHandle struct {
	CallHandle *CallHandle
	Response   *rsock.Response
	Final      bool
}

func (s *SocketSession) writeResponse(
	r *responseHandle,
) error {
	useProtobuf := false
	msgType := websocket.TextMessage

	resp := r.Response

	if r.CallHandle != nil {
		useProtobuf = r.CallHandle.Protobuf
		resp.CallId = r.CallHandle.Call.CallId
	}

	var data []byte

	switch useProtobuf {
	case true:
		msgType = websocket.BinaryMessage

		d, err := proto.Marshal(resp)
		if err != nil {
			return fmt.Errorf("marshal protobuf message: %w", err)
		}

		data = d
	case false:
		d, err := protojson.Marshal(resp)
		if err != nil {
			return fmt.Errorf("marshal json message: %w", err)
		}

		data = d
	}

	err := s.writeMessage(msgType, data)
	if err != nil {
		return sockFatalf("write message to socket: %w", err)
	}

	if r.Final {
		return errCloseSocket
	}

	return nil
}

func sockFatalf(format string, a ...any) error {
	we := fmt.Errorf(format, a...)

	return errSockFatal{
		msg:   "fatal socket error: " + we.Error(),
		cause: errors.Unwrap(we),
	}
}

type errSockFatal struct {
	msg   string
	cause error
}

func (e errSockFatal) Error() string {
	return e.msg
}

func (e errSockFatal) Unwrap() error {
	return e.cause
}

var errCloseSocket = errors.New("close socket")
