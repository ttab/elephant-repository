package repository

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/postgres"
	"golang.org/x/exp/slog"
)

type NotifyChannel string

const (
	NotifySchemasUpdated   NotifyChannel = "schemas"
	NotifyArchived         NotifyChannel = "archived"
	NotifyWorkflowsUpdated NotifyChannel = "workflows"
	NotifyEventlog         NotifyChannel = "eventlog"
)

type ArchiveEventType int

const (
	ArchiveEventTypeStatus ArchiveEventType = iota
	ArchiveEventTypeVersion
)

type ArchivedEvent struct {
	Type ArchiveEventType `json:"type"`
	UUID uuid.UUID        `json:"uuid"`
	// Version is the version of a document or the ID of a status.
	Version int64  `json:"version"`
	Name    string `json:"name,omitempty"`
}

type SchemaEventType int

const (
	SchemaEventTypeActivation SchemaEventType = iota
	SchemaEventTypeDeactivation
)

type SchemaEvent struct {
	Type SchemaEventType `json:"type"`
	Name string          `json:"name"`
}

type WorkflowEventType int

const (
	WorkflowEventTypeStatusChange WorkflowEventType = iota
	WorkflowEventTypeStatusRuleChange
)

type WorkflowEvent struct {
	Type WorkflowEventType `json:"type"`
	Name string            `json:"name"`
}

func notifyArchived(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload ArchivedEvent,
) {
	pgNotify(ctx, logger, q, NotifyArchived, payload)
}

func notifySchemaUpdated(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload SchemaEvent,
) {
	pgNotify(ctx, logger, q, NotifySchemasUpdated, payload)
}

func notifyEventlog(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	id int64,
) {
	pgNotify(ctx, logger, q, NotifyEventlog, id)
}

func notifyWorkflowUpdated(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload WorkflowEvent,
) {
	pgNotify(ctx, logger, q, NotifyWorkflowsUpdated, payload)
}

func pgNotify[T any](
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	channel NotifyChannel, payload T,
) {
	message, err := json.Marshal(payload)
	if err != nil {
		logger.Error("failed to marshal payload for notification", err,
			internal.LogKeyChannel, channel)
	}

	err = q.Notify(ctx, postgres.NotifyParams{
		Channel: string(channel),
		Message: string(message),
	})
	if err != nil {
		logger.Error(
			"failed to marshal payload for notification", err,
			internal.LogKeyChannel, channel,
			internal.LogKeyMessage, json.RawMessage(message))
	}
}
