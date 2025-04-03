package postgres

import (
	"time"

	"github.com/google/uuid"
)

type OutboxEvent struct {
	Event              string     `json:"event"`
	UUID               uuid.UUID  `json:"uuid"`
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
	MetaDocVersion     int64      `json:"meta_doc_version,omitempty"`
	AttachedObjects    []string   `json:"attached_objects,omitempty"`
	DetachedObjects    []string   `json:"detached_objects,omitempty"`
	DeleteRecordID     int64      `json:"delete_record_id,omitempty"`
}

type ACLEntry struct {
	URI         string   `json:"uri"`
	Permissions []string `json:"permissions"`
}

type EventlogExtra struct {
	AttachedObjects []string `json:"attached_objects,omitempty"`
	DetachedObjects []string `json:"detached_objects,omitempty"`
	DeleteRecordID  int64    `json:"delete_record_id,omitempty"`
}
