package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/ttab/elephant-repository/postgres"
)

type NotifyChannel string

const (
	NotifySchemasUpdated      NotifyChannel = "schemas"
	NotifyDeprecationsUpdated NotifyChannel = "deprecations"
	NotifyArchived            NotifyChannel = "archived"
	NotifyWorkflowsUpdated    NotifyChannel = "workflows"
	NotifyEventlog            NotifyChannel = "eventlog"
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

type DeprecationEvent struct {
	Label string `json:"label"`
}

type WorkflowEventType int

const (
	WorkflowEventTypeStatusChange WorkflowEventType = iota
	WorkflowEventTypeStatusRuleChange
)

type WorkflowEvent struct {
	Type    WorkflowEventType `json:"type"`
	DocType string            `json:"doc_type"`
	Name    string            `json:"name"`
}

func notifyArchived(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload ArchivedEvent,
) error {
	return pgNotify(ctx, logger, q, NotifyArchived, payload)
}

func notifySchemaUpdated(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload SchemaEvent,
) error {
	return pgNotify(ctx, logger, q, NotifySchemasUpdated, payload)
}

func notifyDeprecationUpdated(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload DeprecationEvent,
) error {
	return pgNotify(ctx, logger, q, NotifyDeprecationsUpdated, payload)
}

func notifyEventlog(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	id int64,
) error {
	return pgNotify(ctx, logger, q, NotifyEventlog, id)
}

func notifyWorkflowUpdated(
	ctx context.Context, logger *slog.Logger, q *postgres.Queries,
	payload WorkflowEvent,
) error {
	return pgNotify(ctx, logger, q, NotifyWorkflowsUpdated, payload)
}

func pgNotify[T any](
	ctx context.Context, _ *slog.Logger, q *postgres.Queries,
	channel NotifyChannel, payload T,
) error {
	message, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload for notification: %w", err)
	}

	err = q.Notify(ctx, postgres.NotifyParams{
		Channel: string(channel),
		Message: string(message),
	})
	if err != nil {
		return fmt.Errorf("failed to publish notification payload to channel: %w", err)
	}

	return nil
}
