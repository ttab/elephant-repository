package repository

import (
	"context"
	"time"

	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
)

func (p *PGDocStore) RunCleaner(ctx context.Context, period time.Duration) {
	for {
		select {
		case <-time.After(period):
		case <-ctx.Done():
			return
		}

		jobLock, err := pg.NewJobLock(p.pool, p.logger, "cleaner",
			10*time.Second, 1*time.Minute, 20*time.Second, 5*time.Second)
		if err != nil {
			p.logger.ErrorCtx(ctx, "failed to create job lock",
				elephantine.LogKeyError, err)

			continue
		}

		err = jobLock.RunWithContext(ctx, p.removeExpiredLocks)
		if err != nil {
			p.logger.ErrorCtx(
				ctx, "lock cleaner error",
				elephantine.LogKeyError, err,
			)
		}
	}
}

func (p *PGDocStore) removeExpiredLocks(ctx context.Context) error {
	p.logger.Debug("removing expired document locks")

	return p.reader.DeleteExpiredDocumentLocks(ctx,
		pg.Time(time.Now().Add(5*time.Minute)),
	)
}
