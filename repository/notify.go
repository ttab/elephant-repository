package repository

import (
	"github.com/google/uuid"
)

const (
	NotifySchemasUpdated      = "schemas"
	NotifyDeprecationsUpdated = "deprecations"
	NotifyArchived            = "archived"
	NotifyWorkflowsUpdated    = "workflows"
	NotifyEventOutbox         = "event_outbox"
	NotifyEventlog            = "eventlog"
)

type ArchiveEventType int

const (
	ArchiveEventTypeStatus ArchiveEventType = iota
	ArchiveEventTypeVersion
	ArchiveEventTypeLogItem
)

type ArchivedEvent struct {
	Type    ArchiveEventType `json:"type"`
	EventID int64            `json:"event_id"`
	UUID    uuid.UUID        `json:"uuid"`
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
	WorkflowEventTypeWorkflowChange
)

type WorkflowEvent struct {
	Type    WorkflowEventType `json:"type"`
	DocType string            `json:"doc_type"`
	Name    string            `json:"name"`
}
