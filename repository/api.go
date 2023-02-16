package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/revisor"
	"github.com/ttab/elephant/rpc/repository"
	"github.com/twitchtv/twirp"
	"golang.org/x/mod/semver"
)

type APIServer struct {
	store     DocStore
	validator *revisor.Validator
}

func NewAPIServer(store DocStore, validator *revisor.Validator) *APIServer {
	return &APIServer{
		store:     store,
		validator: validator,
	}
}

// Interface guard.
var _ repository.Documents = &APIServer{}

// Delete implements repository.Documents.
func (a *APIServer) Delete(
	ctx context.Context, req *repository.DeleteDocumentRequest,
) (*repository.DeleteDocumentResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("doc_delete") {
		return nil, twirp.PermissionDenied.Error(
			"no delete permission")
	}

	if req.IfMatch < -1 {
		return nil, twirp.InvalidArgumentError("if_match",
			"cannot be less than -1")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.writeAccessCheck(ctx, auth, docUUID)
	if err != nil {
		return nil, err
	}

	access, err := a.store.CheckPermission(ctx, CheckPermissionRequest{
		UUID: docUUID,
		GranteeURIs: append([]string{auth.Claims.Subject},
			auth.Claims.Units...),
		Permission: "w",
	})
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to check document permissions: %w", err)
	}

	switch access {
	case PermissionCheckNoSuchDocument:
		return &repository.DeleteDocumentResponse{}, nil
	case PermissionCheckAllowed:
	case PermissionCheckDenied:
		if !auth.Claims.HasScope("superuser") {
			return nil, twirp.PermissionDenied.Error(
				"no write permission for the document")
		}
	}

	err = a.store.Delete(ctx, DeleteRequest{
		UUID:    docUUID,
		Updated: time.Now(),
		Updater: auth.Claims.Subject,
		Meta:    req.Meta,
		IfMatch: req.IfMatch,
	})
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to delete document from data store: %w", err)
	}

	return &repository.DeleteDocumentResponse{}, nil
}

// Get implements repository.Documents.
func (a *APIServer) Get(
	ctx context.Context, req *repository.GetDocumentRequest,
) (*repository.GetDocumentResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("doc_read") {
		return nil, twirp.PermissionDenied.Error(
			"no read permission")
	}

	if req.Version < 0 {
		return nil, twirp.InvalidArgumentError("version",
			"cannot be a negative number")
	}

	if req.Lock {
		return nil, twirp.Unimplemented.Error(
			"locking is not implemented yet")
	}

	if req.Version > 0 && req.Status != "" {
		return nil, twirp.InvalidArgumentError("status",
			"status cannot be specified together with a version")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.readAccessCheck(ctx, auth, docUUID)
	if err != nil {
		return nil, err
	}

	// TODO: This is a bit wasteful to request for all document loads.
	meta, err := a.store.GetDocumentMeta(ctx, docUUID)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("the document doesn't exist")
	} else if err != nil {
		return nil, twirp.Internal.Errorf(
			"failed to load document metadata: %w", err)
	}

	if meta.Deleting {
		return nil, twirp.FailedPrecondition.Error(
			"document is being deleted")
	}

	var version int64

	switch {
	case req.Version > 0:
		version = req.Version
	case req.Status != "":
		status, ok := meta.Statuses[req.Status]
		if !ok {
			return nil, twirp.NotFoundError(
				"no such status set for the document")
		}

		version = status.Version
		if version == -1 {
			return nil, twirp.NotFoundError(
				"no such status set for the document")
		}
	default:
		version = meta.CurrentVersion
	}

	doc, err := a.store.GetDocument(ctx, docUUID, version)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("no such version")
	} else if err != nil {
		return nil, twirp.Internal.Errorf(
			"failed to load document version: %w", err)
	}

	return &repository.GetDocumentResponse{
		Document: DocumentToRPC(doc),
		Version:  version,
	}, nil
}

// GetHistory implements repository.Documents.
func (a *APIServer) GetHistory(
	ctx context.Context, req *repository.GetHistoryRequest,
) (*repository.GetHistoryResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("doc_read") {
		return nil, twirp.PermissionDenied.Error(
			"no read permission")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.readAccessCheck(ctx, auth, docUUID)
	if err != nil {
		return nil, err
	}

	if req.Before != 0 && req.Before < 2 {
		return nil, twirp.InvalidArgumentError("before",
			"cannot be non-zero and less that 2")
	}

	history, err := a.store.GetVersionHistory(
		ctx, docUUID, req.Before, 10,
	)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("no such version")
	}

	var res repository.GetHistoryResponse

	for _, up := range history {
		res.Versions = append(res.Versions, &repository.DocumentVersion{
			Version: up.Version,
			Created: up.Created.Format(time.RFC3339),
			Creator: up.Creator,
			Meta:    up.Meta,
		})
	}

	return &res, nil
}

func (a *APIServer) readAccessCheck(
	ctx context.Context,
	auth *AuthInfo, docUUID uuid.UUID,
) error {
	access, err := a.store.CheckPermission(ctx, CheckPermissionRequest{
		UUID: docUUID,
		GranteeURIs: append([]string{auth.Claims.Subject},
			auth.Claims.Units...),
		Permission: "r",
	})
	if err != nil {
		return twirp.InternalErrorf(
			"failed to check document permissions: %w", err)
	}

	switch access {
	case PermissionCheckNoSuchDocument:
		return twirp.NotFoundError("no such document")
	case PermissionCheckAllowed:
	case PermissionCheckDenied:
		if !auth.Claims.HasScope("superuser") {
			return twirp.PermissionDenied.Error(
				"no read permission for the document")
		}
	}

	return nil
}

func (a *APIServer) writeAccessCheck(
	ctx context.Context,
	auth *AuthInfo, docUUID uuid.UUID,
) error {
	access, err := a.store.CheckPermission(ctx, CheckPermissionRequest{
		UUID: docUUID,
		GranteeURIs: append([]string{auth.Claims.Subject},
			auth.Claims.Units...),
		Permission: "w",
	})
	if err != nil {
		return twirp.InternalErrorf(
			"failed to check document permissions: %w", err)
	}

	switch access {
	case PermissionCheckNoSuchDocument:
		return twirp.NotFoundError("no such document")
	case PermissionCheckAllowed:
	case PermissionCheckDenied:
		if !auth.Claims.HasScope("superuser") {
			return twirp.PermissionDenied.Error(
				"no write permission for the document")
		}
	}

	return nil
}

// GetMeta implements repository.Documents.
func (a *APIServer) GetMeta(
	ctx context.Context, req *repository.GetMetaRequest,
) (*repository.GetMetaResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("doc_read") {
		return nil, twirp.PermissionDenied.Error(
			"no read permission")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.readAccessCheck(ctx, auth, docUUID)
	if err != nil {
		return nil, err
	}

	meta, err := a.store.GetDocumentMeta(ctx, docUUID)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("the document doesn't exist")
	} else if err != nil {
		return nil, fmt.Errorf("failed to load basic metadata: %w", err)
	}

	resp := repository.DocumentMeta{
		Created:        meta.Created.Format(time.RFC3339),
		Modified:       meta.Modified.Format(time.RFC3339),
		CurrentVersion: meta.CurrentVersion,
	}

	for name, head := range meta.Statuses {
		if resp.Heads == nil {
			resp.Heads = make(map[string]*repository.Status)
		}

		s := repository.Status{
			Id:      head.ID,
			Version: head.Version,
			Creator: head.Creator,
			Created: head.Created.Format(time.RFC3339),
			Meta:    head.Meta,
		}

		resp.Heads[name] = &s
	}

	for _, acl := range meta.ACL {
		resp.Acl = append(resp.Acl, &repository.ACLEntry{
			Uri:         acl.URI,
			Permissions: acl.Permissions,
		})
	}

	return &repository.GetMetaResponse{
		Meta: &resp,
	}, nil
}

func validateRequiredUUIDParam(v string) (uuid.UUID, error) {
	if v == "" {
		return uuid.Nil, twirp.RequiredArgumentError("uuid")
	}

	u, err := uuid.Parse(v)
	if err != nil {
		return uuid.Nil, twirp.InvalidArgumentError("uuid", err.Error())
	}

	return u, nil
}

// Update implements repository.Documents.
func (a *APIServer) Update(
	ctx context.Context, req *repository.UpdateRequest,
) (*repository.UpdateResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("doc_write") {
		return nil, twirp.PermissionDenied.Error(
			"no write permission")
	}

	if req.ImportDirective != nil && !auth.Claims.HasScope("import_directive") {
		return nil, twirp.PermissionDenied.Error(
			"no import directive permission")
	}

	if req.Document == nil && len(req.Status) == 0 && len(req.Acl) == 0 {
		return nil, twirp.InvalidArgumentError(
			"document",
			"required when no status or ACL updates are included")
	}

	if req.IfMatch < -1 {
		return nil, twirp.InvalidArgumentError("if_match",
			"cannot be less than -1")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.writeAccessCheck(ctx, auth, docUUID)
	if err != nil {
		return nil, err
	}

	if req.Document != nil && req.Document.Uuid == "" {
		req.Document.Uuid = docUUID.String()
	} else if req.Document != nil && req.Document.Uuid != docUUID.String() {
		return nil, twirp.InvalidArgumentError("document.uuid",
			"the document must have the same UUID as the request uuid")
	}

	for i, s := range req.Status {
		if s == nil {
			return nil, twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d", i),
				"a status cannot be nil")
		}

		if s.Name == "" {
			return nil, twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.name", i),
				"a status cannot have an empty name")
		}

		if s.Version < 0 {
			return nil, twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.version", i),
				"cannot be negative")
		}

		if req.Document == nil && s.Version == 0 {
			return nil, twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.version", i),
				"required when no document is included")
		}
	}

	for i, e := range req.Acl {
		if e == nil {
			return nil, twirp.InvalidArgumentError(
				fmt.Sprintf("acl.%d", i),
				"an ACL entry cannot be nil")
		}

		if e.Uri == "" {
			return nil, twirp.InvalidArgumentError(
				fmt.Sprintf("acl.%d.uri", i),
				"an ACL grantee URI cannot be empty")
		}

		for _, p := range e.Permissions {
			// TODO: Should be validated against a list of
			// acceptable permissions, it's not like
			// []string{"z",".","k"} is valid.
			if len(p) != 1 {
				return nil, twirp.InvalidArgumentError(
					fmt.Sprintf("acl.%d.permissions", i),
					"a permission must be a single character")
			}
		}
	}

	access, err := a.store.CheckPermission(ctx, CheckPermissionRequest{
		UUID: docUUID,
		GranteeURIs: append([]string{auth.Claims.Subject},
			auth.Claims.Units...),
		Permission: "w",
	})
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to check document permissions: %w", err)
	}

	switch access {
	case PermissionCheckNoSuchDocument:
		// Allowed to create documents given a doc_write scope is set
	case PermissionCheckAllowed:
	case PermissionCheckDenied:
		if !auth.Claims.HasScope("superuser") {
			return nil, twirp.PermissionDenied.Error(
				"no write permission for the document")
		}
	}

	updater := auth.Claims.Subject
	updated := time.Now()

	if req.ImportDirective != nil {
		id := req.ImportDirective

		if id.OriginalCreator != "" {
			updater = req.ImportDirective.OriginalCreator
		}

		if id.OriginallyCreated != "" {
			t, err := time.Parse(time.RFC3339, id.OriginallyCreated)
			if err != nil {
				return nil, twirp.InvalidArgumentError(
					"import_directive.originally_created",
					fmt.Sprintf("invalid date: %v", err),
				)
			}

			updated = t
		}
	}

	doc := RPCToDocument(req.Document)

	if doc != nil {
		doc.UUID = docUUID.String()

		validationResult := a.validator.ValidateDocument(doc)

		if len(validationResult) > 0 {
			err := twirp.InvalidArgument.Errorf(
				"the document had %d validation errors, the first one is: %v",
				len(validationResult), validationResult[0].String())

			err = err.WithMeta("err_count",
				strconv.Itoa(len(validationResult)))

			for i := range validationResult {
				err = err.WithMeta(strconv.Itoa(i),
					validationResult[i].String())
			}

			return nil, err
		}
	}

	up := UpdateRequest{
		UUID:     docUUID,
		Updated:  updated,
		Updater:  updater,
		Meta:     req.Meta,
		Status:   RPCToStatusUpdate(req.Status),
		Document: doc,
		IfMatch:  req.IfMatch,
	}

	for _, e := range req.Acl {
		up.ACL = append(up.ACL, ACLEntry{
			URI:         e.Uri,
			Permissions: e.Permissions,
		})
	}

	up.DefaultACL = append(up.DefaultACL, ACLEntry{
		URI:         updater,
		Permissions: []string{"r", "w"},
	})

	if updater != auth.Claims.Subject {
		up.DefaultACL = append(up.DefaultACL, ACLEntry{
			URI:         auth.Claims.Subject,
			Permissions: []string{"r", "w"},
		})
	}

	res, err := a.store.Update(ctx, up)

	// TODO: generic docstore-to-twirp error translation is needed
	switch {
	case IsDocStoreErrorCode(err, ErrCodeOptimisticLock):
		return nil, twirp.FailedPrecondition.Error(err.Error())
	case IsDocStoreErrorCode(err, ErrCodeBadRequest):
		return nil, twirp.InvalidArgumentError("document", err.Error())
	case IsDocStoreErrorCode(err, ErrCodeDeleteLock):
		return nil, twirp.FailedPrecondition.Error(err.Error())
	case err != nil:
		return nil, twirp.InternalErrorf(
			"failed to update document: %w", err)
	}

	return &repository.UpdateResponse{
		Version: res.Version,
	}, nil
}

// Validate implements repository.Documents.
func (a *APIServer) Validate(
	ctx context.Context, req *repository.ValidateRequest,
) (*repository.ValidateResponse, error) {
	if req.Document == nil {
		return nil, twirp.RequiredArgumentError("document")
	}

	doc := RPCToDocument(req.Document)

	validationResult := a.validator.ValidateDocument(doc)

	var res repository.ValidateResponse

	for _, r := range validationResult {
		res.Errors = append(res.Errors, &repository.ValidationResult{
			Entity: EntityRefToRPC(r.Entity),
			Error:  r.Error,
		})
	}

	return &res, nil
}

// GetActiveSchemas implements repository.Documents.
func (a *APIServer) GetActiveSchemas(
	ctx context.Context, req *repository.GetActiveSchemasRequest,
) (*repository.GetActiveSchemasResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("schema_admin") {
		return nil, twirp.PermissionDenied.Error(
			"no administrative schema permission")
	}

	schemas, err := a.store.GetActiveSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to retrieve active schemas: %w", err)
	}

	var res repository.GetActiveSchemasResponse

	for i := range schemas {
		data, err := json.Marshal(schemas[i].Specification)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal %q@%s specification for response: %w",
				schemas[i].Name, schemas[i].Version, err)
		}

		res.Schemas = append(res.Schemas, &repository.Schema{
			Name:    schemas[i].Name,
			Version: schemas[i].Version,
			Spec:    data,
		})
	}

	return &res, nil
}

// GetSchema implements repository.Documents.
func (a *APIServer) GetSchema(
	ctx context.Context, req *repository.GetSchemaRequest,
) (*repository.GetSchemaResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("schema_admin") {
		return nil, twirp.PermissionDenied.Error(
			"no administrative schema permission")
	}

	schema, err := a.store.GetSchema(ctx, req.Name, req.Version)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to retrieve schema: %w", err)
	}

	data, err := json.Marshal(schema.Specification)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to marshal specification for response: %w",
			err)
	}

	return &repository.GetSchemaResponse{
		Version: schema.Version,
		Spec:    data,
	}, nil
}

// RegisterSchema implements repository.Documents.
func (a *APIServer) RegisterSchema(
	ctx context.Context, req *repository.RegisterSchemaRequest,
) (*repository.RegisterSchemaResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("schema_admin") {
		return nil, twirp.PermissionDenied.Error(
			"no administrative schema permission")
	}

	if req.Schema == nil {
		return nil, twirp.RequiredArgumentError("schema")
	}

	if req.Schema.Name == "" {
		return nil, twirp.RequiredArgumentError("schema.name")
	}

	if req.Schema.Version == "" {
		return nil, twirp.RequiredArgumentError("schema.version")
	}

	version := semver.Canonical(req.Schema.Version)
	if version == "" {
		return nil, twirp.InvalidArgumentError(
			"schema.version", "invalid semver version")
	}

	var spec revisor.ConstraintSet

	err := json.Unmarshal(req.Schema.Spec, &spec)
	if err != nil {
		return nil, twirp.InvalidArgument.Errorf(
			"invalid schema: %w", err)
	}

	err = a.store.RegisterSchema(ctx, RegisterSchemaRequest{
		Name:          req.Schema.Name,
		Version:       version,
		Specification: &spec,
	})
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil, twirp.FailedPrecondition.Error(
			"schema version already exists")
	} else if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}

	return &repository.RegisterSchemaResponse{}, nil
}

// SetActiveSchema implements repository.Documents.
func (a *APIServer) SetActiveSchema(
	ctx context.Context, req *repository.SetActiveSchemaRequest,
) (*repository.SetActiveSchemaResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("schema_admin") {
		return nil, twirp.PermissionDenied.Error(
			"no administrative schema permission")
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	if !req.Deactivate && req.Version == "" {
		return nil, twirp.RequiredArgumentError("version")
	}

	if req.Deactivate {
		err := a.store.DeactivateSchema(ctx, req.Name)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to deactivate schema: %w", err)
		}
	} else {
		err := a.store.RegisterSchema(ctx, RegisterSchemaRequest{
			Name:     req.Name,
			Version:  req.Version,
			Activate: true,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"failed to register activation: %w", err)
		}
	}

	return &repository.SetActiveSchemaResponse{}, nil
}

func EntityRefToRPC(ref []revisor.EntityRef) []*repository.EntityRef {
	var out []*repository.EntityRef

	for _, r := range ref {
		out = append(out, &repository.EntityRef{
			RefType: string(r.RefType),
			Kind:    string(r.BlockKind),
			Index:   int64(r.Index),
			Name:    r.Name,
			Type:    r.Type,
			Rel:     r.Rel,
		})
	}

	return out
}

func RPCToStatusUpdate(update []*repository.StatusUpdate) []StatusUpdate {
	var out []StatusUpdate

	for i := range update {
		if update[i] == nil {
			continue
		}

		out = append(out, StatusUpdate{
			Name:    update[i].Name,
			Version: update[i].Version,
			Meta:    update[i].Meta,
		})
	}

	return out
}

func DocumentToRPC(d *doc.Document) *repository.Document {
	if d == nil {
		return nil
	}

	return &repository.Document{
		Uuid:     d.UUID,
		Type:     d.Type,
		Uri:      d.URI,
		Url:      d.URL,
		Title:    d.Title,
		Content:  BlocksToRPC(d.Content),
		Meta:     BlocksToRPC(d.Meta),
		Links:    BlocksToRPC(d.Links),
		Language: d.Language,
	}
}

func BlocksToRPC(blocks []doc.Block) []*repository.Block {
	var res []*repository.Block

	// Not allocating up-front to avoid turning nil into [].
	if len(blocks) > 0 {
		res = make([]*repository.Block, len(blocks))
	}

	for i, b := range blocks {
		rb := repository.Block{
			Id:          b.ID,
			Uuid:        b.UUID,
			Uri:         b.URI,
			Url:         b.URL,
			Type:        b.Type,
			Title:       b.Title,
			Rel:         b.Rel,
			Role:        b.Role,
			Name:        b.Name,
			Value:       b.Value,
			ContentType: b.ContentType,
			Links:       BlocksToRPC(b.Links),
			Content:     BlocksToRPC(b.Content),
			Meta:        BlocksToRPC(b.Meta),
		}

		for k, v := range b.Data {
			if rb.Data == nil {
				rb.Data = make(map[string]string)
			}

			rb.Data[k] = v
		}

		res[i] = &rb
	}

	return res
}

func RPCToDocument(rpcDoc *repository.Document) *doc.Document {
	if rpcDoc == nil {
		return nil
	}

	return &doc.Document{
		UUID:     rpcDoc.Uuid,
		Type:     rpcDoc.Type,
		URI:      rpcDoc.Uri,
		URL:      rpcDoc.Url,
		Title:    rpcDoc.Title,
		Links:    RPCToBlocks(rpcDoc.Links),
		Content:  RPCToBlocks(rpcDoc.Content),
		Meta:     RPCToBlocks(rpcDoc.Meta),
		Language: rpcDoc.Language,
	}
}

func RPCToBlocks(blocks []*repository.Block) []doc.Block {
	var res []doc.Block

	// Not allocating up-front to avoid turning nil into [].
	if len(blocks) > 0 {
		res = make([]doc.Block, len(blocks))
	}

	for i, rb := range blocks {
		if rb == nil {
			continue
		}

		b := doc.Block{
			ID:          rb.Id,
			UUID:        rb.Uuid,
			URI:         rb.Uri,
			URL:         rb.Url,
			Type:        rb.Type,
			Title:       rb.Title,
			Rel:         rb.Rel,
			Role:        rb.Role,
			Name:        rb.Name,
			Value:       rb.Value,
			ContentType: rb.ContentType,
			Links:       RPCToBlocks(rb.Links),
			Content:     RPCToBlocks(rb.Content),
			Meta:        RPCToBlocks(rb.Meta),
		}

		for k, v := range rb.Data {
			if b.Data == nil {
				b.Data = make(doc.DataMap)
			}

			b.Data[k] = v
		}

		res[i] = b
	}

	return res
}
