package repository

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
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
		return nil, missingScopeError(scopes)
	}

	return auth, nil
}

// documentRestrictableScopes are the scopes that can be restricted to a single
// document with a ":<uuid>" suffix, which is how elephant-delegate narrows a
// session token for a device.
var documentRestrictableScopes = []string{
	ScopeDocumentRead,
	ScopeDocumentWrite,
}

// RequireDocumentScope is RequireAnyScope for a method that acts on the single
// document named by document, the raw UUID from the request or from the
// document it supplies. A scope in scopes
// that can be restricted to a document also counts in its restricted form,
// "doc_read:<uuid>", when the UUID is that document's.
//
// A restricted scope only narrows the scope it restricts: the ACL check still
// has to pass. Methods that act on more than one document use RequireAnyScope,
// which matches scopes exactly and so refuses a restricted one.
func RequireDocumentScope(
	ctx context.Context, document string, scopes ...string,
) (*elephantine.AuthInfo, error) {
	auth, ok := elephantine.GetAuthInfo(ctx)
	if !ok {
		return nil, rpc.Unauthenticated(
			"no anonymous access allowed")
	}

	if auth.Claims.HasAnyScope(scopes...) {
		return auth, nil
	}

	// An unparseable request UUID is refused as a missing scope rather
	// than as an invalid argument, so that a caller without the scope gets
	// the same answer as before restricted scopes existed. The handler's
	// own validation reports the bad UUID to callers that do have it.
	docUUID, err := uuid.Parse(document)
	if err != nil {
		return nil, missingScopeError(scopes)
	}

	if !HasDocumentScope(auth.Claims, docUUID, scopes...) {
		return nil, missingScopeError(scopes)
	}

	return auth, nil
}

// HasDocumentScope reports whether the claims hold one of scopes restricted
// to docUUID. Only the scopes in documentRestrictableScopes can be restricted,
// and the suffix must be a UUID in its standard hyphenated form; anything else
// grants nothing. The UUIDs are compared parsed, so case doesn't matter.
func HasDocumentScope(
	claims elephantine.JWTClaims, docUUID uuid.UUID, scopes ...string,
) bool {
	if docUUID == uuid.Nil {
		return false
	}

	// Scopes are space separated, as elephantine splits them.
	for scope := range strings.SplitSeq(claims.Scope, " ") {
		name, suffix, ok := strings.Cut(scope, ":")
		if !ok {
			continue
		}

		if !slices.Contains(documentRestrictableScopes, name) ||
			!slices.Contains(scopes, name) {
			continue
		}

		// uuid.Parse also accepts the urn, braced and unhyphenated
		// forms; a suffix is only ever written in the standard one.
		if len(suffix) != 36 {
			continue
		}

		id, err := uuid.Parse(suffix)
		if err != nil {
			continue
		}

		if id == docUUID {
			return true
		}
	}

	return false
}

func missingScopeError(scopes []string) error {
	err := rpc.PermissionDeniedf(
		"one of the the scopes %s is required",
		strings.Join(scopes, ", "))

	// The scopes that would have been accepted are carried as error
	// metadata under the fleet-wide key, so a client can tell the caller
	// what to ask for instead of parsing the message.
	return rpc.WithMeta(err,
		rpc.MetaRequiredScopes, strings.Join(scopes, " "))
}
