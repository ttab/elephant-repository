package repository

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tmaxmax/go-sse"
	"github.com/ttab/elephantine"
	"google.golang.org/protobuf/encoding/protojson"
)

type SSE struct {
	logger *slog.Logger
	server *sse.Server
	store  DocStore
	stop   chan struct{}

	// Highest ID that we have sent.
	lastID int64
}

const replayLimit = 200

func NewSSE(ctx context.Context, logger *slog.Logger, store DocStore) (*SSE, error) {
	fin, err := sse.NewFiniteReplayProvider(replayLimit, false)
	if err != nil {
		return nil, fmt.Errorf("create SSE replay provider: %w", err)
	}

	sseProvider := &sse.Joe{
		ReplayProvider: fin,
	}

	lastID, err := store.GetLastEventID(ctx)
	if err != nil && !IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, fmt.Errorf(
			"get last event ID to prepopulate replay buffer: %w", err)
	}

	events, err := store.GetEventlog(ctx, max(lastID-replayLimit, 0), replayLimit)
	if err != nil {
		return nil, fmt.Errorf(
			"get eventlog to prepopulate replay buffer: %w", err)
	}

	for _, evt := range events {
		msg, topics, err := eventMessage(evt)
		if err != nil {
			// Just ignore, the only way for this to fail is if the
			// json.Marshal failed, highly unlikely.
			continue
		}

		sseProvider.ReplayProvider.Put(msg, topics)
	}

	sseServer := &sse.Server{
		Provider: sseProvider,
		OnSession: func(sess *sse.Session) (sse.Subscription, bool) {
			_, err := RequireAnyScope(sess.Req.Context(),
				ScopeEventlogRead,
				ScopeDocumentAdmin,
			)
			if err != nil {
				code := elephantine.TwirpErrorToHTTPStatusCode(err)

				sess.Res.WriteHeader(code)
				_, _ = sess.Res.Write([]byte(err.Error()))

				return sse.Subscription{}, false
			}

			query := sess.Req.URL.Query()

			return sse.Subscription{
				Client:      sess,
				LastEventID: sess.LastEventID,
				Topics:      query["topic"],
			}, true
		},
	}

	return &SSE{
		logger: logger,
		lastID: lastID,
		store:  store,
		server: sseServer,
		stop:   make(chan struct{}),
	}, nil
}

func (s *SSE) Run(ctx context.Context) {
	var (
		ch   = make(chan int64, 1)
		eval = make(chan bool, 1)
		// Highest observed ID that we need to send. The on OnLogEvent
		// handler will write to this value, and the send event loop
		// will read from it.
		highWaterMark int64
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()

		err := s.server.Shutdown(ctx)
		if err != nil {
			s.logger.Error("shutdown failed")
		}
	}()

	s.store.OnEventlog(ctx, ch)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case id := <-ch:
				c := atomic.LoadInt64(&highWaterMark)
				atomic.StoreInt64(&highWaterMark, max(id, c))

				select {
				case eval <- true:
				default:
				}
			}
		}
	}()

	const tickerDelay = 5 * time.Second

	// We don't have delivery guarantees on the OnEventlog messages, so
	// start a timer for polling the database for the latest event ID. This
	// will guarantee that our worst case delay is less that tickerDelay.
	ticker := time.NewTicker(tickerDelay)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-eval:
			// Do a ticker reset so that we don't poll unnecessarily
			// when we are getting regular events.
			ticker.Reset(tickerDelay)

			s.sendUntil(ctx, atomic.LoadInt64(&highWaterMark))
		case <-ticker.C:
			s.checkLatestEvent(ctx, ch)
		}
	}
}

func (s *SSE) Stop() {
	close(s.stop)
}

func (s *SSE) HTTPHandler() http.Handler {
	return s.server
}

// Checks what the last event ID in the database is and submits it for
// evaluation.
func (s *SSE) checkLatestEvent(ctx context.Context, eval chan int64) {
	id, err := s.store.GetLastEventID(ctx)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return
	} else if err != nil {
		s.logger.ErrorContext(ctx, "failed to get last event id",
			elephantine.LogKeyError, err)

		return
	}

	eval <- id
}

func (s *SSE) sendUntil(ctx context.Context, id int64) bool {
	if id <= s.lastID {
		return false
	}

	events, err := s.store.GetEventlog(ctx, s.lastID, 20)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to get events to send",
			elephantine.LogKeyError, err)

		return false
	}

	for _, evt := range events {
		msg, topics, err := eventMessage(evt)
		if err != nil {
			// If we can't marshal the event we don't want to retry
			// it, just skip it and continue.
			s.lastID = evt.ID

			continue
		}

		err = s.server.Publish(msg, topics...)
		if err != nil {
			// Basically ignore the error here as the only possible
			// source of errors is that the SSE server has been
			// closed.
			s.logger.Error("failed to publish event",
				elephantine.LogKeyEventID, evt.ID)
		}

		s.lastID = evt.ID
	}

	// We have sent all events if the lastID has reached or exceeded the ID
	// we were asked to evaluate.
	return s.lastID >= id
}

func eventMessage(evt Event) (*sse.Message, []string, error) {
	topics := []string{
		"firehose",
		evt.Type,
		string(evt.Event),
		string(evt.Event) + "." + evt.Type,
	}

	data, err := protojson.Marshal(EventToRPC(evt))
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to marshal event to send: %w", err)
	}

	msg := sse.Message{
		ID: sse.ID(strconv.FormatInt(evt.ID, 10)),
	}

	msg.AppendData(string(data))

	return &msg, topics, nil
}
