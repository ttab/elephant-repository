package repository

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/rpc"
)

type Permission string

const (
	ReadPermission      Permission = "r"
	WritePermission     Permission = "w"
	MetaWritePermission Permission = "m"
	SetStatusPermission Permission = "s"
)

var validPermissions = []Permission{
	ReadPermission,
	WritePermission,
	MetaWritePermission,
	SetStatusPermission,
}

func IsValidPermission(p Permission) bool {
	return slices.Contains(validPermissions, p)
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
	ScopeAssetUpload          = "asset_upload"
	ScopeEventlogRead         = "eventlog_read"
	ScopeMetricsAdmin         = "metrics_admin"
	ScopeMetricsWrite         = "metrics_write"
	ScopeMetricsRead          = "metrics_read"
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
		return nil, rpc.Unauthenticated(
			"no anonymous access allowed")
	}

	if !auth.Claims.HasAnyScope(scopes...) {
		err := rpc.PermissionDeniedf(
			"one of the the scopes %s is required",
			strings.Join(scopes, ", "))

		// The scopes that would have been accepted are carried as error
		// metadata under the fleet-wide key, so a client can tell the
		// caller what to ask for instead of parsing the message.
		return nil, rpc.WithMeta(err,
			rpc.MetaRequiredScopes, strings.Join(scopes, " "))
	}

	return auth, nil
}
