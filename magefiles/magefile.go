//go:build mage
// +build mage

package main

import (
	"context"

	//mage:import s3
	_ "github.com/ttab/mage/s3"
	//mage:import sql
	sql "github.com/ttab/mage/sql"
)

var reportingTables = []string{
	"acl",
	"active_schemas",
	"attached_object",
	"attached_object_current",
	"delete_record",
	"deprecation",
	"document",
	"document_link",
	"document_lock",
	"document_schema",
	"document_status",
	"document_version",
	"event_outbox_item",
	"eventlog",
	"eventsink",
	"job_lock",
	"meta_type",
	"meta_type_use",
	"metric",
	"metric_kind",
	"planning_assignee",
	"planning_assignment",
	"planning_deliverable",
	"planning_item",
	"purge_request",
	"restore_request",
	"schema_version",
	"status",
	"status_heads",
	"status_rule",
	"upload",
	"workflow",
	"workflow_state",
}

func GrantReporting(ctx context.Context) error {
	return sql.GrantReporting(ctx, reportingTables)
}
