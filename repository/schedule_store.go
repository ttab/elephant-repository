package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine/pg"
)

type ScheduleStore interface {
	GetScheduled(
		ctx context.Context, after time.Time, notSource []string,
	) ([]ScheduledPublish, error)
	GetDelayedScheduled(
		ctx context.Context, before time.Time, cutoff time.Duration, notSource []string,
	) ([]ScheduledPublish, error)
	GetDelayedScheduledCount(
		ctx context.Context, before time.Time, cutoff time.Duration, notSource []string,
	) (int64, error)
}

type SchedulePGStore struct {
	db *pgxpool.Pool
}

func NewSchedulePGStore(db *pgxpool.Pool) *SchedulePGStore {
	s := SchedulePGStore{
		db: db,
	}

	return &s
}

type ScheduledPublish struct {
	// UUID is the ID of the scheduled document.
	UUID uuid.UUID
	// Type of the scheduled document.
	Type string
	// StatusID is the last withheld status ID.
	StatusID int64
	// DocumentVersion is the last version that was set as withheld.
	DocumentVersion int64
	// PlanningItem ID.
	PlanningItem uuid.UUID
	// Assignment ID.
	Assignment uuid.UUID
	// Publish timestamp set in the assignment.
	Publish time.Time
	// ScheduledBy is the sub of the user that set the withheld status.
	ScheduledBy string
}

func (s *SchedulePGStore) GetScheduled(
	ctx context.Context, after time.Time, notSource []string,
) ([]ScheduledPublish, error) {
	q := postgres.New(s.db)

	if len(notSource) == 0 {
		notSource = nil
	}

	rows, err := q.GetScheduled(ctx, postgres.GetScheduledParams{
		After:     pg.Time(after),
		NotSource: notSource,
	})
	if err != nil {
		return nil, fmt.Errorf("read from database: %w", err)
	}

	res := make([]ScheduledPublish, len(rows))

	for i, row := range rows {
		res[i] = ScheduledPublish{
			UUID:            row.UUID,
			Type:            row.Type,
			StatusID:        row.StatusID,
			DocumentVersion: row.DocumentVersion,
			PlanningItem:    row.PlanningItem,
			Assignment:      row.Assignment,
			Publish:         row.Publish.Time,
			ScheduledBy:     row.CreatorUri,
		}
	}

	return res, nil
}

func (s *SchedulePGStore) GetDelayedScheduled(
	ctx context.Context, before time.Time, cutoff time.Duration, notSource []string,
) ([]ScheduledPublish, error) {
	q := postgres.New(s.db)

	if len(notSource) == 0 {
		notSource = nil
	}

	rows, err := q.GetDelayedScheduled(ctx, postgres.GetDelayedScheduledParams{
		Before:    pg.Time(before),
		Cutoff:    pg.Time(before.Add(-cutoff)),
		NotSource: notSource,
	})
	if err != nil {
		return nil, fmt.Errorf("read from database: %w", err)
	}

	res := make([]ScheduledPublish, len(rows))

	for i, row := range rows {
		res[i] = ScheduledPublish{
			UUID:            row.UUID,
			Type:            row.Type,
			StatusID:        row.StatusID,
			DocumentVersion: row.DocumentVersion,
			PlanningItem:    row.PlanningItem,
			Assignment:      row.Assignment,
			Publish:         row.Publish.Time,
			ScheduledBy:     row.CreatorUri,
		}
	}

	return res, nil
}

func (s *SchedulePGStore) GetDelayedScheduledCount(
	ctx context.Context, before time.Time, cutoff time.Duration, notSource []string,
) (int64, error) {
	q := postgres.New(s.db)

	if len(notSource) == 0 {
		notSource = nil
	}

	count, err := q.GetDelayedScheduledCount(ctx,
		postgres.GetDelayedScheduledCountParams{
			Before:    pg.Time(before),
			Cutoff:    pg.Time(before.Add(-cutoff)),
			NotSource: notSource,
		})
	if err != nil {
		return 0, fmt.Errorf("read from database: %w", err)
	}

	return count, nil
}
