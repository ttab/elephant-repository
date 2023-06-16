package repository

import (
	"context"
	"fmt"
	"time"

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

		jobLock, err := pg.NewJobLock(s.pool, s.logger, "cleaner",
			10*time.Second, 1*time.Minute, 20*time.Second, 5*time.Second)
		if err != nil {
			s.logger.ErrorCtx(ctx, "failed to create job lock",
				elephantine.LogKeyError, err)

			continue
		}

		err = jobLock.RunWithContext(ctx, s.removeExpiredLocks)
		if err != nil {
			s.logger.ErrorCtx(
				ctx, "lock cleaner error",
				elephantine.LogKeyError, err,
			)
		}
	}
}

func (s *PGDocStore) removeExpiredLocks(ctx context.Context) error {
	s.logger.Debug("removing expired document locks")

	err := s.reader.DeleteExpiredDocumentLocks(ctx,
		pg.Time(time.Now().Add(5*time.Minute)),
	)
	if err != nil {
		return fmt.Errorf("could not remove expired locks: %w", err)
	}
	return nil
}
