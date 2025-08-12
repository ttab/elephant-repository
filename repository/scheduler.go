package repository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
)

// SchedulerMaxPollInterval is the maximum interval for looking up the next
// scheduled documents for publishing. This should be tweaked upwards when we
// have a good signal for when scheduling data has changed. That would be
// whenever a document enters the "withheld" workflow state or the publish time
// is set or changed on an assignment.
//
// This controls how quickly new scheduled articles are discovered, it does not
// reflect the resolution at which publishing can be scheduled, or how often the
// scheduler can act on set times.
const SchedulerMaxPollInterval = 1 * time.Minute

// SchedulerRetryWindow defines the duration after a planned publish time
// during which the scheduler will attempt to publish the document.
// After the retry window has passed the document will still be in the withheld state,
// unless otherwise changed, but no attempts will be made to publish it.
const SchedulerRetryWindow = 30 * time.Minute

// SchedulerDelayedTreshold is the treshold after which a scheduled publish will
// be counted as delayed. Delayed documents will be visible in the
// "elephant_scheduled_delayed_count" metric.
const SchedulerDelayedTreshold = 1 * time.Minute

type Scheduler struct {
	logger         *slog.Logger
	store          ScheduleStore
	docs           repository.Documents
	excludeSources []string

	scheduledPub *prometheus.CounterVec
	delayed      prometheus.Gauge
}

func NewScheduler(
	logger *slog.Logger,
	metricsRegisterer prometheus.Registerer,
	store ScheduleStore,
	docs repository.Documents,
	excludeSources []string,
) (*Scheduler, error) {
	scheduledPub := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "elephant_scheduled_publish_total",
		Help: "The number of attempts that have been made to publish documents",
	}, []string{"outcome"})

	err := metricsRegisterer.Register(scheduledPub)
	if err != nil {
		return nil, fmt.Errorf("register %q metric: %w",
			"elephant_scheduled_publish_total", err)
	}

	delayed := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "elephant_scheduled_delayed",
		Help: "The number of documents whose scheduled publish has been delayed.",
	})

	err = metricsRegisterer.Register(delayed)
	if err != nil {
		return nil, fmt.Errorf("register %q metric: %w",
			"elephant_scheduled_delayed", err)
	}

	return &Scheduler{
		logger:         logger,
		store:          store,
		docs:           docs,
		excludeSources: excludeSources,
		scheduledPub:   scheduledPub,
		delayed:        delayed,
	}, nil
}

func (s *Scheduler) RunInJobLock(
	ctx context.Context,
	recheckSignal <-chan struct{},
	lockFn func() (*pg.JobLock, error),
) error {
	for {
		lock, err := lockFn()
		if err != nil {
			return fmt.Errorf("create job lock: %w", err)
		}

		err = lock.RunWithContext(ctx, func(ctx context.Context) error {
			return s.Run(ctx, recheckSignal)
		})
		if err != nil {
			s.logger.ErrorContext(ctx, "run scheduler in lock",
				elephantine.LogKeyError, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (s *Scheduler) Run(ctx context.Context, recheckSignal <-chan struct{}) error {
	// Run iterations until we're cancelled. We wait between iterations
	// until the next known scheduled publish, we receive a recheck signal,
	// or the max interval has passed.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		next, err := s.iteration(ctx)
		if err != nil {
			return err
		}

		var wait time.Duration

		switch {
		case next.IsZero():
			wait = SchedulerMaxPollInterval
		case next.Before(time.Now()):
			continue
		default:
			wait = min(time.Until(next), SchedulerMaxPollInterval)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		case <-recheckSignal:
		}
	}
}

func (s *Scheduler) iteration(ctx context.Context) (time.Time, error) {
	after := time.Now().Add(-SchedulerRetryWindow)

	upcoming, err := s.store.GetScheduled(ctx, after, s.excludeSources)
	if err != nil {
		return time.Time{}, fmt.Errorf("get scheduled articles: %w", err)
	}

	if len(upcoming) == 0 {
		return time.Time{}, nil
	}

	var next time.Time

	now := time.Now()

	for _, sch := range upcoming {
		// Results are sorted by publish asc.
		if sch.Publish.After(now) {
			next = sch.Publish

			break
		}

		// Set the document version that the withheld status refers to
		// as usable, but only do that if the document still is in the
		// withheld workflow state, and if the withheld status hasn't
		// changed.
		update := repository.UpdateRequest{
			Uuid: sch.UUID.String(),
			Status: []*repository.StatusUpdate{
				{
					Name:    "usable",
					Version: sch.DocumentVersion,
					Meta: map[string]string{
						"scheduled-by": sch.ScheduledBy,
					},
				},
			},
			IfWorkflowState: "withheld",
			IfStatusHeads: map[string]int64{
				"withheld": sch.StatusID,
			},
		}

		authCtx := elephantine.SetAuthInfo(ctx, &elephantine.AuthInfo{
			Claims: elephantine.JWTClaims{
				Scope: "doc_admin",
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "internal://scheduler",
				},
			},
		})

		_, err := s.docs.Update(authCtx, &update)
		if err != nil {
			s.logger.ErrorContext(authCtx, "failed to publish scheduled article",
				elephantine.LogKeyDocumentUUID, sch.UUID,
				elephantine.LogKeyDocumentStatus, "withheld",
				elephantine.LogKeyDocumentStatusID, sch.StatusID,
				elephantine.LogKeyError, err)

			s.scheduledPub.WithLabelValues("failure").Inc()

			continue
		}

		s.scheduledPub.WithLabelValues("success").Inc()
	}

	delayLimit := now.Add(-SchedulerDelayedTreshold)

	delayed, err := s.store.GetDelayedScheduledCount(ctx,
		delayLimit, 24*time.Hour, s.excludeSources)

	switch {
	case err != nil:
		s.logger.ErrorContext(ctx, "get delayed scheduled count",
			elephantine.LogKeyError, err)
	default:
		s.delayed.Set(float64(delayed))
	}

	return next, nil
}
