package repository

import (
	"context"
	"strconv"
	"strings"

	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
)

type Permission string

const (
	ReadPermission  Permission = "r"
	WritePermission Permission = "w"
)

func (p Permission) Name() string {
	switch p {
	case ReadPermission:
		return "read"
	case WritePermission:
		return "write"
	}

	return strconv.Quote(string(p))
}

const (
	// TODO: ScopeSuperuser is a bad scope, introducing doc_admin and
	// doc_read_all to replace usage of superuser.
	ScopeSuperuser       = "superuser"
	ScopeDocumentAdmin   = "doc_admin"
	ScopeDocumentReadAll = "doc_read_all"
	ScopeDocumentRead    = "doc_read"
	ScopeDocumentDelete  = "doc_delete"
	ScopeDocumentWrite   = "doc_write"
	ScopeEventlogRead    = "eventlog_read"
	ScopeMetricsAdmin    = "metrics_admin"
	ScopeMetricsWrite    = "metrics_write"
	ScopeReportAdmin     = "report_admin"
	ScopeReportRun       = "report_run"
	ScopeSchemaAdmin     = "schema_admin"
	ScopeSchemaRead      = "schema_read"
	ScopeWorkflowAdmin   = "workflow_admin"
)

func Subscope(scope string, resource ...string) string {
	if len(resource) == 0 {
		return scope
	}

	return scope + ":" + strings.Join(resource, ":")
}

func RequireAnyScope(ctx context.Context, scopes ...string) (*elephantine.AuthInfo, error) {
	auth, ok := elephantine.GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous access allowed")
	}

	if !auth.Claims.HasAnyScope(scopes...) {
		return nil, twirp.PermissionDenied.Errorf(
			"one of the the scopes %s is required",
			strings.Join(scopes, ", "))
	}

	return auth, nil
}
