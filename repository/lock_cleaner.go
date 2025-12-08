package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
)

func (s *PGDocStore) RunCleaner(ctx context.Context, period time.Duration) {
	for {
		select {
		case <-time.After(period):
		case <-ctx.Done():
			return
		}

		jobLock, err := pg.NewJobLock(s.pool, s.logger, "cleaner", pg.JobLockOptions{
			PingInterval:  10 * time.Second,
			StaleAfter:    1 * time.Minute,
			CheckInterval: 20 * time.Second,
			Timeout:       5 * time.Second,
		})
		if err != nil {
			s.logger.ErrorContext(ctx, "failed to create job lock",
				elephantine.LogKeyError, err)

			continue
		}

		err = jobLock.RunWithContext(ctx, s.cleanupTasks)
		if err != nil {
			s.logger.ErrorContext(
				ctx, "run cleanup tasks in lock",
				elephantine.LogKeyError, err,
			)
		}
	}
}

func (s *PGDocStore) cleanupTasks(ctx context.Context) error {
	err := s.removeExpiredLocks(ctx)
	if err != nil {
		s.logger.ErrorContext(
			ctx, "lock cleaner error",
			elephantine.LogKeyError, err,
		)
	}

	err = s.evictNonCurrent(ctx)
	if err != nil {
		s.logger.ErrorContext(
			ctx, "eviction error",
			elephantine.LogKeyError, err,
		)
	}

	return nil
}

const (
	taskNameEvictNoncurrent = "evict_noncurrent"
)

func (s *PGDocStore) evictNonCurrent(ctx context.Context) error {
	if !s.opts.EnableEviction {
		return nil
	}

	lastRun := s.lastRuns[taskNameEvictNoncurrent]
	if time.Since(lastRun) < 12*time.Hour {
		return nil
	}

	now := time.Now()
	if !s.opts.MaintenanceWindow.InWindow(now) {
		return nil
	}

	ages := s.opts.TypeConfigurations.GetEvictionAges()
	q := postgres.New(s.pool)

	var total int64

	for docType, age := range ages {
		cutoff := now.Add(-age)

		count, err := q.EvictNoncurrentVersions(ctx, postgres.EvictNoncurrentVersionsParams{
			DocType: docType,
			Cutoff:  pg.Time(cutoff),
		})
		if err != nil {
			s.logger.WarnContext(ctx,
				"noncurrent version eviction failed",
				elephantine.LogKeyError, err,
				elephantine.LogKeyDocumentType, docType,
			)

			continue
		}

		if count > 0 {
			s.logger.InfoContext(ctx, "evicted non-current versions",
				elephantine.LogKeyDocumentType, docType,
				elephantine.LogKeyCount, count,
			)
		}

		total += count
	}

	s.logger.InfoContext(ctx, "finished evicting non-current versions",
		elephantine.LogKeyCount, total,
	)

	s.lastRuns[taskNameEvictNoncurrent] = now

	return nil
}

func (s *PGDocStore) removeExpiredLocks(ctx context.Context) error {
	s.logger.Debug("removing expired document locks")

	cutoff := pg.Time(time.Now().Add(5 * time.Minute))

	err := pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		expired, err := q.GetExpiredDocumentLocks(ctx, cutoff)
		if err != nil {
			return fmt.Errorf("could not get expired locks: %w", err)
		}

		uuids := make([]uuid.UUID, len(expired))
		for i := range expired {
			uuids[i] = expired[i].UUID
		}

		err = q.DeleteExpiredDocumentLock(ctx, postgres.DeleteExpiredDocumentLockParams{
			Cutoff: cutoff,
			Uuids:  uuids,
		})
		if err != nil {
			return fmt.Errorf("could not remove expired locks: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not remove expired locks: %w", err)
	}

	return nil
}
