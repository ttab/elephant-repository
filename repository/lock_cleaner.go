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

		err = jobLock.RunWithContext(ctx, s.removeExpiredLocks)
		if err != nil {
			s.logger.ErrorContext(
				ctx, "lock cleaner error",
				elephantine.LogKeyError, err,
			)
		}
	}
}

func (s *PGDocStore) removeExpiredLocks(ctx context.Context) error {
	s.logger.Debug("removing expired document locks")

	cutoff := pg.Time(time.Now().Add(5 * time.Minute))

	err := s.withTX(ctx, "lock cleaner", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		expired, err := q.GetExpiredDocumentLocks(ctx, cutoff)
		if err != nil {
			return fmt.Errorf("could not get expired locks: %w", err)
		}

		uuids := make([]uuid.UUID, len(expired))
		for i := 0; i < len(expired); i++ {
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
