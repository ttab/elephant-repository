package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine/pg"
)

type EventType string

const (
	TypeEventIgnored    EventType = ""
	TypeDocumentVersion EventType = "document"
	TypeNewStatus       EventType = "status"
	TypeACLUpdate       EventType = "acl"
	TypeDeleteDocument  EventType = "delete_document"
	TypeRestoreFinished EventType = "restore_finished"
	TypeWorkflow        EventType = "workflow"
)

type Event struct {
	ID                 int64      `json:"id"`
	Event              EventType  `json:"event"`
	UUID               uuid.UUID  `json:"uuid"`
	Nonce              uuid.UUID  `json:"nonce"`
	Timestamp          time.Time  `json:"timestamp"`
	Updater            string     `json:"updater"`
	Type               string     `json:"type"`
	Language           string     `json:"language"`
	OldLanguage        string     `json:"old_language,omitempty"`
	MainDocument       *uuid.UUID `json:"main_document,omitempty"`
	Version            int64      `json:"version,omitempty"`
	StatusID           int64      `json:"status_id,omitempty"`
	Status             string     `json:"status,omitempty"`
	ACL                []ACLEntry `json:"acl,omitempty"`
	SystemState        string     `json:"system_state,omitempty"`
	WorkflowStep       string     `json:"workflow_step,omitempty"`
	WorkflowCheckpoint string     `json:"workflow_checkpoint,omitempty"`
	MainDocumentType   string     `json:"main_document_type,omitempty"`
	AttachedObjects    []string   `json:"attached_objects,omitempty"`
	DetachedObjects    []string   `json:"detached_objects,omitempty"`
	DeleteRecordID     int64      `json:"delete_record_id,omitempty"`
}

func NewEventlogBuilder(
	logger *slog.Logger,
	pool *pgxpool.Pool,
	metricsRegisterer prometheus.Registerer,
	outboxNotifications <-chan int64,
) (*EventlogBuilder, error) {
	if metricsRegisterer == nil {
		metricsRegisterer = prometheus.DefaultRegisterer
	}

	restarts := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "elephant_eventlog_restarts_total",
			Help: "Number of times the eventlog has restarted.",
		},
	)
	if err := metricsRegisterer.Register(restarts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	events := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_eventlog_events_total",
			Help: "Number of received eventlog events.",
		},
		[]string{"type", "doc_type"},
	)
	if err := metricsRegisterer.Register(events); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &EventlogBuilder{
		pool:                pool,
		logger:              logger,
		outboxNotifications: outboxNotifications,
		restarts:            restarts,
		events:              events,
	}, nil
}

type EventlogBuilder struct {
	pool   *pgxpool.Pool
	logger *slog.Logger

	outboxNotifications <-chan int64

	restarts prometheus.Counter
	events   *prometheus.CounterVec
}

const (
	outboxPollInterval = 1 * time.Minute
	outboxBatchSize    = 20
)

func (eb *EventlogBuilder) Run(ctx context.Context) error {
	eb.logger.Info("starting eventlog builder")

	q := postgres.New(eb.pool)

	timer := time.NewTimer(outboxPollInterval)

	lastID, err := q.GetLastEventID(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("get last event ID: %w", err)
	}

	for {
		outEvents, err := q.ReadEventOutbox(ctx, outboxBatchSize)
		if err != nil {
			return fmt.Errorf("read event outbox: %w", err)
		}

		for _, item := range outEvents {
			evt := item.Event

			var mainDocUUID pgtype.UUID

			if evt.MainDocument != nil {
				mainDocUUID.Bytes = *evt.MainDocument
				mainDocUUID.Valid = true
			}

			params := postgres.InsertIntoEventLogParams{
				ID:                 lastID + 1,
				Event:              evt.Event,
				UUID:               evt.UUID,
				Nonce:              evt.Nonce,
				Timestamp:          pg.Time(evt.Timestamp),
				Updater:            pg.TextOrNull(evt.Updater),
				Type:               pg.TextOrNull(evt.Type),
				Version:            pg.BigintOrNull(evt.Version),
				Status:             pg.TextOrNull(evt.Status),
				StatusID:           pg.BigintOrNull(evt.StatusID),
				MainDoc:            mainDocUUID,
				MainDocType:        pg.TextOrNull(evt.MainDocumentType),
				Language:           pg.TextOrNull(evt.Language),
				OldLanguage:        pg.TextOrNull(evt.OldLanguage),
				SystemState:        pg.TextOrNull(evt.SystemState),
				WorkflowState:      pg.TextOrNull(evt.WorkflowStep),
				WorkflowCheckpoint: pg.TextOrNull(evt.WorkflowCheckpoint),
				Extra: &postgres.EventlogExtra{
					AttachedObjects: evt.AttachedObjects,
					DetachedObjects: evt.DetachedObjects,
					DeleteRecordID:  evt.DeleteRecordID,
				},
			}

			if evt.ACL != nil {
				data, err := json.Marshal(evt.ACL)
				if err != nil {
					return fmt.Errorf("marshal ACL: %w", err)
				}

				params.Acl = data
			}

			err := eb.recordEvent(ctx, item.ID, params)
			if err != nil {
				return fmt.Errorf("record event: %w", err)
			}

			lastID = params.ID
		}

		if len(outEvents) == outboxBatchSize {
			// Immediately run again if we got a full batch.
			continue
		}

		timer.Reset(outboxPollInterval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-eb.outboxNotifications:
		case <-timer.C:
		}
	}
}

func (eb *EventlogBuilder) recordEvent(
	ctx context.Context,
	outboxID int64,
	params postgres.InsertIntoEventLogParams,
) (outErr error) {
	tx, err := eb.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("start transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	err = q.InsertIntoEventLog(ctx, params)
	if err != nil {
		return fmt.Errorf("insert eventlog entry: %w", err)
	}

	err = pg.Publish(ctx, tx, NotifyEventlog, params.ID)
	if err != nil {
		return fmt.Errorf("send event log notification: %w", err)
	}

	err = q.DeleteOutboxEvent(ctx, outboxID)
	if err != nil {
		return fmt.Errorf("delete outbox event: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
