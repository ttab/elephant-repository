package repository

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	rsock "github.com/ttab/elephant-api/repositorysocket"
	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/errgroup"
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
	maxMessageSize = 512
)

func NewSocketHandler(
	ctx context.Context,
	logger *slog.Logger,
	store DocStore,
	cache *DocCache,
	auth elephantine.AuthInfoParser,
	socketKey *ecdsa.PublicKey,
) *SocketHandler {
	docStream := NewDocumentStream(ctx, logger, store)

	h := SocketHandler{
		runCtx: ctx,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		log:       logger,
		store:     store,
		cache:     cache,
		stream:    docStream,
		auth:      auth,
		socketKey: socketKey,
	}

	return &h
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
}

func (h *SocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.URL.Path, "/websocket/")
	if !ok {
		http.Error(w, "no socket token", http.StatusUnauthorized)

		return
	}

	tok, err := VerifySocketToken(token, h.socketKey)
	if err != nil {
		http.Error(w, "invalid socket token", http.StatusUnauthorized)

		return
	}

	if tok.Expires.Before(time.Now()) {
		http.Error(w, "expired socket token", http.StatusUnauthorized)

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

		return
	}

	sess := NewSocketSession(conn, h.log, h.store, h.cache, h.stream, h.auth)

	sess.Run(r.Context())
}

func NewSocketSession(
	conn *websocket.Conn,
	log *slog.Logger,
	store DocStore,
	cache *DocCache,
	stream *DocumentStream,
	authParser elephantine.AuthInfoParser,
) *SocketSession {
	return &SocketSession{
		conn:       conn,
		log:        log,
		store:      store,
		cache:      cache,
		stream:     stream,
		authParser: authParser,
		calls:      make(chan *CallHandle, 8),
		responses:  make(chan *responseHandle, 16),
		sets:       make(map[string]*documentSetHandle),
	}
}

type SocketSession struct {
	conn *websocket.Conn

	m        sync.RWMutex
	auth     *elephantine.AuthInfo
	identity []string

	authExpired *time.Ticker

	log        *slog.Logger
	store      DocStore
	cache      *DocCache
	stream     *DocumentStream
	authParser elephantine.AuthInfoParser

	calls     chan *CallHandle
	responses chan *responseHandle

	sets map[string]*documentSetHandle
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
		err := s.readLoop(gCtx)
		if err != nil {
			return fmt.Errorf("run read loop: %w", err)
		}

		return nil
	})

	group.Go(func() error {
		err := s.handlerLoop(gCtx)
		if err != nil {
			return fmt.Errorf("run handler loop: %w", err)
		}

		return nil
	})

	group.Go(func() error {
		err := s.writeLoop(gCtx)
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

		callHandle, err := s.readCall()
		if errors.Is(err, errSockFatal{}) {
			return err
		} else if err != nil {
			s.log.Warn("failed to read message from websocket",
				elephantine.LogKeyError, err)

			continue
		}

		auth, _ := s.getAuth()

		if auth.Claims.ExpiresAt.Before(time.Now()) {
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

func (s *SocketSession) runHandler(ctx context.Context, call *CallHandle) bool {
	var (
		resp *rsock.Response
		rErr *rsock.Error
	)

	switch {
	case call.Call.Authenticate != nil:
		resp, rErr = s.handleAuthenticate(ctx, call, call.Call.Authenticate)
	case call.Call.GetDocuments != nil:
		resp, rErr = s.handleGetDocuments(ctx, call, call.Call.GetDocuments)
	case call.Call.CloseDocumentSet != nil:
		resp, rErr = s.handleCloseDocumentSet(ctx, call, call.Call.CloseDocumentSet)
	default:
		rErr = SockErrorf(string(twirp.InvalidArgument), "unknown call type")
	}

	if rErr != nil {
		// We always bail immediately on authentication errors.
		final := rErr.ErrorCode == string(twirp.Unauthenticated)

		s.Respond(ctx, call, &rsock.Response{
			Error: rErr,
		}, final)

		return final
	}

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
	_ context.Context, _ *CallHandle, req *rsock.Authenticate,
) (*rsock.Response, *rsock.Error) {
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
	ctx context.Context, callHandle *CallHandle, req *rsock.GetDocuments,
) (*rsock.Response, *rsock.Error) {
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

	previous, ok := s.sets[req.SetName]
	if ok {
		previous.Close()
	}

	handle := newDocumentSetHandle(ctx, callHandle, s)

	_, identity := s.getAuth()

	docSet := newDocumentSet(
		req.Type, req.SetName,
		timespan, req.Labels,
		includeExtractors,
		identity, s.cache, s.store, handle)

	handle.Set = docSet

	err := docSet.Initialise(handle.ctx, s.stream)
	if err != nil {
		handle.Close()

		return nil, SockErrorf(string(twirp.Internal),
			"initialise document set: %v", err)
	}

	s.sets[req.SetName] = handle

	return &rsock.Response{}, nil
}

func (s *SocketSession) handleCloseDocumentSet(
	_ context.Context, _ *CallHandle, req *rsock.CloseDocumentSet,
) (*rsock.Response, *rsock.Error) { //nolint: unparam
	set, ok := s.sets[req.SetName]
	if ok {
		set.Close()

		delete(s.sets, req.SetName)
	}

	return &rsock.Response{}, nil
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
			err := s.writeMessage(websocket.CloseMessage, nil)
			if err != nil {
				s.log.Warn("failed to write close message",
					elephantine.LogKeyError, err)
			}

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
	msgType := websocket.TextMessage

	resp := r.Response

	if r.CallHandle != nil {
		resp.CallId = r.CallHandle.Call.CallId
	}

	var data []byte

	switch r.CallHandle.Protobuf {
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
