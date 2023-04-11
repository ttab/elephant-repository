package sinks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant/internal"
	repo "github.com/ttab/elephant/repository"
	"github.com/ttab/elephant/rpc/repository"
	"golang.org/x/exp/slog"
)

type IncrementSkipMetricFunc func(eventType string, reason string)

type EventSink interface {
	SinkName() string

	// SendEvents to the sink. Returns the number events that were sent and
	// an error if the send failed. Due to batching behaviours some events
	// might have been sent before the processing fails, so the number of
	// sent events should not be ignored when an error is returned.
	SendEvents(
		ctx context.Context, evts []EventDetail,
		skipMetric IncrementSkipMetricFunc,
	) (int, error)
}

type SinkStateStore interface {
	GetSinkPosition(ctx context.Context, name string) (int64, error)
	SetSinkPosition(ctx context.Context, name string, pos int64) error
}

type EventForwarderOptions struct {
	Logger            *slog.Logger
	Documents         repository.Documents
	MetricsRegisterer prometheus.Registerer
	Sink              EventSink
	StateStore        SinkStateStore
}

type EventForwarder struct {
	logger    *slog.Logger
	documents repository.Documents
	sink      EventSink
	state     SinkStateStore

	restarts *prometheus.CounterVec
	skips    *prometheus.CounterVec
	latency  *prometheus.HistogramVec

	cancel  func()
	stopped chan struct{}
}

func NewEventForwarder(opts EventForwarderOptions) (*EventForwarder, error) {
	if opts.MetricsRegisterer == nil {
		opts.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	restarts := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_event_forwarder_restarts_total",
			Help: "Number of times the event forwarder has restarted.",
		}, []string{"name"})
	if err := opts.MetricsRegisterer.Register(restarts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	skips := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_event_forwarder_skipped_total",
			Help: "Number of times events have been skipped by the forwarder.",
		}, []string{"name", "type", "reason"})
	if err := opts.MetricsRegisterer.Register(skips); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "elephant_event_forwarder_latency_seconds",
		Help:    "Observed time between event time and sink submission.",
		Buckets: prometheus.ExponentialBuckets(0.100, 2, 11),
	}, []string{"name"})
	if err := opts.MetricsRegisterer.Register(latency); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &EventForwarder{
		logger:    opts.Logger,
		restarts:  restarts,
		skips:     skips,
		latency:   latency,
		documents: opts.Documents,
		sink:      opts.Sink,
		state:     opts.StateStore,
	}, nil
}

func (r *EventForwarder) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	r.cancel = cancel
	r.stopped = make(chan struct{})

	go r.run(ctx)
}

func (r *EventForwarder) run(ctx context.Context) {
	const restartWaitSeconds = 10

	defer close(r.stopped)

	for {
		r.logger.Debug("starting event forwarder")

		err := r.loop(ctx)
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			r.restarts.WithLabelValues(r.sink.SinkName()).Inc()

			r.logger.ErrorCtx(
				ctx, "sink error, restarting",
				internal.LogKeyError, err,
				internal.LogKeyDelay, slog.DurationValue(restartWaitSeconds),
			)
		}

		select {
		case <-time.After(restartWaitSeconds * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (r *EventForwarder) loop(ctx context.Context) error {
	sinkName := r.sink.SinkName()

	pos, err := r.state.GetSinkPosition(ctx, sinkName)
	if err != nil {
		return fmt.Errorf("failed to get sink position: %w", err)
	}

	for {
		newPos, err := r.runNext(ctx, pos)

		if newPos > pos {
			err = r.state.SetSinkPosition(ctx, sinkName, newPos)
			if err != nil {
				return fmt.Errorf(
					"failed to update sink position: %w", err)
			}

			pos = newPos
		}

		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (r *EventForwarder) runNext(ctx context.Context, pos int64) (int64, error) {
	aCtx := repo.SetAuthInfo(ctx, &repo.AuthInfo{
		Claims: repo.JWTClaims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject: "internal://event-forwarder",
			},
			Scope: "superuser doc_read",
		},
	})

	log, err := r.documents.Eventlog(aCtx, &repository.GetEventlogRequest{
		After:       pos,
		BatchSize:   10,
		WaitMs:      60 * 1000,
		BatchWaitMs: 200,
	})
	if err != nil {
		return pos, fmt.Errorf("failed to read event log: %w", err)
	}

	events := make([]EventDetail, len(log.Items))

	for i, item := range log.Items {
		event, err := repo.RPCToEvent(item)
		if err != nil {
			r.logger.Error("invalid eventlog item",
				internal.LogKeyError, err,
				internal.LogKeyEventID, item.Id)

			events[i] = EventDetail{
				Event: repo.Event{
					ID:    item.Id,
					Event: repo.TypeEventIgnored,
				},
			}

			continue
		}

		detail := EventDetail{
			Event: event,
		}

		switch event.Event {
		case repo.TypeDocumentVersion, repo.TypeNewStatus:
			docRes, err := r.documents.Get(aCtx, &repository.GetDocumentRequest{
				Uuid:    item.Uuid,
				Version: item.Version,
			})
			if err != nil {
				return 0, fmt.Errorf(
					"failed to load document %s v%d for event: %w",
					item.Uuid, item.Version, err)
			}

			detail.Document = DetailFromDocument(
				repo.RPCToDocument(docRes.Document),
			)

			detail.Event.Version = docRes.Version
			detail.Event.Type = docRes.Document.Type
		}

		events[i] = detail
	}

	sent, err := r.sink.SendEvents(ctx, events, func(eventType, reason string) {
		r.skips.WithLabelValues(
			r.sink.SinkName(), eventType, reason,
		).Inc()
	})

	if sent > 0 {
		pos = events[sent-1].Event.ID

		latency := r.latency.WithLabelValues(r.sink.SinkName())

		for i := range events[0:sent] {
			if events[i].Event.Event == repo.TypeEventIgnored {
				continue
			}

			latency.Observe(
				time.Since(events[i].Event.Timestamp).Seconds(),
			)
		}
	}

	if err != nil {
		return pos, fmt.Errorf("error during send: %w", err)
	}

	return pos, nil
}
