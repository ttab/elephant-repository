package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	rsock "github.com/ttab/elephant-api/repositorysocket"
	"github.com/ttab/elephantine"
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
	auth elephantine.AuthInfoParser,
) *SocketHandler {
	h := SocketHandler{
		runCtx: ctx,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		log:   logger,
		store: store,
		auth:  auth,
	}

	return &h
}

type SocketHandler struct {
	runCtx   context.Context
	log      *slog.Logger
	upgrader websocket.Upgrader
	store    DocStore
	auth     elephantine.AuthInfoParser
}

func (h *SocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	sess := NewSocketSession(conn, h.log, h.store, h.auth)

	go sess.Run(h.runCtx)
}

func NewSocketSession(
	conn *websocket.Conn,
	log *slog.Logger,
	store DocStore,
	authParser elephantine.AuthInfoParser,
) *SocketSession {
	return &SocketSession{
		conn:       conn,
		log:        log,
		store:      store,
		authParser: authParser,
		calls:      make(chan *CallHandle, 8),
		responses:  make(chan *responseHandle, 16),
	}
}

type SocketSession struct {
	conn *websocket.Conn

	m    sync.RWMutex
	auth *elephantine.AuthInfo

	log        *slog.Logger
	store      DocStore
	authParser elephantine.AuthInfoParser

	calls     chan *CallHandle
	responses chan *responseHandle
}

func (s *SocketSession) setAuth(auth *elephantine.AuthInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	s.auth = auth
}

func (s *SocketSession) getAuth() *elephantine.AuthInfo {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.auth
}

func (s *SocketSession) Run(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, gCtx := errgroup.WithContext(runCtx)

	group.Go(func() error {
		err := s.readLoop(gCtx)
		if err != nil {
			return fmt.Errorf("run read loop: %w", err)
		}

		return nil
	})

	group.Go(func() error {
		err := s.writeLoop(gCtx)
		if err != nil {
			return fmt.Errorf("run read loop: %w", err)
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

		auth := s.getAuth()

		if auth.Claims.ExpiresAt.Before(time.Now()) {
			auth = nil
		}

		switch {
		case auth == nil && callHandle.Call.Authenticate == nil:
			s.Respond(ctx, callHandle, &rsock.Response{
				Error: &rsock.Error{
					ErrorCode:    string(twirp.Unauthenticated),
					ErrorMessage: "Missing or expired authentication token",
				},
			}, false)

			continue
		case callHandle.Call.Authenticate != nil:
			auth, err = s.authParser.AuthInfoFromToken(
				callHandle.Call.Authenticate.Token)
			if err != nil {
				s.Respond(ctx, callHandle, &rsock.Response{
					Error: &rsock.Error{
						ErrorCode:    string(twirp.Unauthenticated),
						ErrorMessage: "Invalid token",
					},
				}, false)
			}

			s.setAuth(auth)

			continue
		}

		s.calls <- callHandle
	}
}

func (s *SocketSession) Respond(
	ctx context.Context, handle *CallHandle, resp *rsock.Response, final bool,
) {
	resp.CallId = handle.Call.CallId

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

	resp.CallId = r.CallHandle.Call.CallId

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
