package repository

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
)

type Permission string

const (
	ReadPermission      Permission = "r"
	WritePermission     Permission = "w"
	MetaWritePermission Permission = "m"
	SetStatusPermission Permission = "s"
)

var vailidPermissions = []Permission{
	ReadPermission,
	WritePermission,
	MetaWritePermission,
	SetStatusPermission,
}

func IsValidPermission(p Permission) bool {
	return slices.Contains(vailidPermissions, p)
}

func (p Permission) Name() string {
	switch p {
	case ReadPermission:
		return "read"
	case WritePermission:
		return "write"
	case MetaWritePermission:
		return "meta write"
	case SetStatusPermission:
		return "set status"
	}

	return strconv.Quote(string(p))
}

const (
	ScopeDocumentAdmin        = "doc_admin"
	ScopeDocumentReadAll      = "doc_read_all"
	ScopeDocumentRead         = "doc_read"
	ScopeDocumentDelete       = "doc_delete"
	ScopeDocumentRestore      = "doc_restore"
	ScopeDocumentPurge        = "doc_purge"
	ScopeDocumentWrite        = "doc_write"
	ScopeMetaDocumentWriteAll = "meta_doc_write_all"
	ScopeDocumentImport       = "doc_import"
	ScopeEventlogRead         = "eventlog_read"
	ScopeMetricsAdmin         = "metrics_admin"
	ScopeMetricsWrite         = "metrics_write"
	ScopeMetricsRead          = "metrics_read"
	ScopeReportAdmin          = "report_admin"
	ScopeReportRun            = "report_run"
	ScopeSchemaAdmin          = "schema_admin"
	ScopeSchemaRead           = "schema_read"
	ScopeWorkflowAdmin        = "workflow_admin"
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
