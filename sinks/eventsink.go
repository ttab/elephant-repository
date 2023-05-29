package sinks

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
	repo "github.com/ttab/elephant/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/twitchtv/twirp"
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
	DB                *pgxpool.Pool
	Documents         repository.Documents
	MetricsRegisterer prometheus.Registerer
	Sink              EventSink
	StateStore        SinkStateStore
}

type EventForwarder struct {
	logger    *slog.Logger
	db        *pgxpool.Pool
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
		stopped:   make(chan struct{}),
		logger:    opts.Logger,
		db:        opts.DB,
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

	r.run(ctx)
}

func (r *EventForwarder) Stop() {
	r.cancel()
	<-r.stopped
}

func (r *EventForwarder) run(ctx context.Context) {
	const restartWaitSeconds = 10

	defer close(r.stopped)

	var wait time.Duration

	for {
		select {
		case <-time.After(wait):
			wait = restartWaitSeconds * time.Second
		case <-ctx.Done():
			return
		}

		jobLock, err := pg.NewJobLock(
			r.db, r.logger, "forwarder",
			10*time.Second, 1*time.Minute, 20*time.Second, 5*time.Second)
		if err != nil {
			r.logger.ErrorCtx(ctx, "failed to create job lock",
				elephantine.LogKeyError, err)

			continue
		}

		r.logger.Debug("starting event forwarder")

		err = jobLock.RunWithContext(ctx, r.loop)
		if err != nil {
			r.restarts.WithLabelValues(r.sink.SinkName()).Inc()

			r.logger.ErrorCtx(
				ctx, "sink error, restarting",
				elephantine.LogKeyError, err,
				elephantine.LogKeyDelay, slog.DurationValue(restartWaitSeconds),
			)
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
			return err //nolint:wrapcheck
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (r *EventForwarder) runNext(ctx context.Context, pos int64) (int64, error) {
	aCtx, cancel := context.WithCancel(repo.SetAuthInfo(ctx, &repo.AuthInfo{
		Claims: repo.JWTClaims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject: "internal://event-forwarder",
			},
			Scope: "superuser doc_read",
		},
	}))

	defer cancel()

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
				elephantine.LogKeyError, err,
				elephantine.LogKeyEventID, item.Id)

			events[i] = EventDetail{
				Event: repo.Event{
					ID:    item.Id,
					Event: repo.TypeEventIgnored,
				},
			}

			continue
		}

		detail, err := r.enrichEvent(aCtx, EventDetail{
			Event: event,
		})
		if err != nil {
			return pos, err
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

func (r *EventForwarder) enrichEvent(
	ctx context.Context, detail EventDetail,
) (EventDetail, error) {
	if detail.Event.Event != repo.TypeDocumentVersion &&
		detail.Event.Event != repo.TypeNewStatus {
		return detail, nil
	}

	docRes, err := r.documents.Get(ctx, &repository.GetDocumentRequest{
		Uuid:    detail.Event.UUID.String(),
		Version: detail.Event.Version,
	})

	switch {
	case elephantine.IsTwirpErrorCode(err, twirp.NotFound):
		// TODO: This raises the question if we need some other way to
		// get to the data for decoration purposes, as we will fail to
		// emit an update event for a document if it was deleted before
		// we got to it. This might also be fine, the end result is a
		// delete after all.
		r.logger.Error("document has been deleted",
			elephantine.LogKeyError, err,
			elephantine.LogKeyEventID, detail.Event.ID)

		r.skips.WithLabelValues(
			r.sink.SinkName(), string(detail.Event.Event), "deleted",
		).Inc()

		detail.Event.Event = repo.TypeEventIgnored

		return detail, nil

	case err != nil:
		return EventDetail{}, fmt.Errorf(
			"failed to load document %s v%d for event: %w",
			detail.Event.UUID, detail.Event.Version, err)
	}

	detail.Document = DetailFromDocument(
		repo.RPCToDocument(docRes.Document),
	)

	detail.Event.Version = docRes.Version
	detail.Event.Type = docRes.Document.Type

	return detail, nil
}
