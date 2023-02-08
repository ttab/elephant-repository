package docformat

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/navigacontentlab/revisor"
	"github.com/ttab/docformat/rpc/repository"
	"github.com/twitchtv/twirp"
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

// Interface guard
var _ repository.Documents = &APIServer{}

// Delete implements repository.Documents
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

	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
	if err != nil {
		return nil, err
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

// Get implements repository.Documents
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

	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
	if err != nil {
		return nil, err
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
		Version:  int64(version),
	}, nil
}

// GetHistory implements repository.Documents
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

	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
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
		return nil, twirp.NotFoundError("the document doesn't exist")
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

// GetMeta implements repository.Documents
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

	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
	if err != nil {
		return nil, err
	}

	meta, err := a.store.GetDocumentMeta(ctx, docUUID)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("the document doesn't exist")
	}

	resp := repository.DocumentMeta{
		Created:        meta.Created.Format(time.RFC3339),
		Modified:       meta.Modified.Format(time.RFC3339),
		CurrentVersion: int64(meta.CurrentVersion),
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
			Name:        acl.Name,
			Permissions: acl.Permissions,
		})
	}

	return &repository.GetMetaResponse{
		Meta: &resp,
	}, nil
}

func validateRequiredUUIDParam(v string, name string) (uuid.UUID, error) {
	if v == "" {
		return uuid.Nil, twirp.RequiredArgumentError(name)
	}

	u, err := uuid.Parse(v)
	if err != nil {
		return uuid.Nil, twirp.InvalidArgumentError(name, err.Error())
	}

	return u, nil
}

// Update implements repository.Documents
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

	if req.ImportDirective != nil && !auth.Claims.HasScope("import_direcive") {
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

	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
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

		validationResult, err := a.validateDocument(*doc)
		if err != nil {
			return nil, twirp.InternalErrorf(
				"failed to validate document: %w", err)
		}

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
			Name:        e.Name,
			Permissions: e.Permissions,
		})
	}

	res, err := a.store.Update(ctx, up)
	// TODO: generic docstore-to-twirp error translation is needed
	if IsDocStoreErrorCode(err, ErrCodeOptimisticLock) {
		return nil, twirp.FailedPrecondition.Error(err.Error())
	} else if IsDocStoreErrorCode(err, ErrCodeBadRequest) {
		return nil, twirp.InvalidArgumentError("document", err.Error())
	} else if IsDocStoreErrorCode(err, ErrCodeDeleteLock) {
		return nil, twirp.FailedPrecondition.Error(err.Error())
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to update document: %w", err)
	}

	return &repository.UpdateResponse{
		Version: int64(res.Version),
	}, nil
}

// Validate implements repository.Documents
func (a *APIServer) Validate(
	ctx context.Context, req *repository.ValidateRequest,
) (*repository.ValidateResponse, error) {
	if req.Document == nil {
		return nil, twirp.RequiredArgumentError("document")
	}

	doc := RPCToDocument(req.Document)

	validationResult, err := a.validateDocument(*doc)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to validate document: %w", err)
	}

	var res repository.ValidateResponse

	for _, r := range validationResult {
		res.Errors = append(res.Errors, &repository.ValidationResult{
			Entity: EntityRefToRPC(r.Entity),
			Error:  r.Error,
		})
	}

	return &res, nil
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

// UpdatePermissions implements repository.Documents
func (*APIServer) UpdatePermissions(
	ctx context.Context, req *repository.UpdatePermissionsRequest,
) (*repository.UpdatePermissionsResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasScope("doc_write") {
		return nil, twirp.PermissionDenied.Error(
			"no write permission")
	}

	return nil, twirp.Unimplemented.Error("not implemented yet")
}

func (a *APIServer) validateDocument(
	doc Document,
) ([]revisor.ValidationResult, error) {
	data, err := json.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to marshal document for validation: %w", err)
	}

	var nDoc navigadoc.Document

	err = json.Unmarshal(data, &nDoc)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal document for validation: %w", err)
	}

	errors := a.validator.ValidateDocument(&nDoc)

	return errors, nil
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

func DocumentToRPC(doc *Document) *repository.Document {
	if doc == nil {
		return nil
	}

	return &repository.Document{
		Uuid:     doc.UUID,
		Type:     doc.Type,
		Uri:      doc.URI,
		Url:      doc.URL,
		Title:    doc.Title,
		Content:  BlocksToRPC(doc.Content),
		Meta:     BlocksToRPC(doc.Meta),
		Links:    BlocksToRPC(doc.Links),
		Language: doc.Language,
	}
}

func BlocksToRPC(blocks []Block) []*repository.Block {
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

func RPCToDocument(rpcDoc *repository.Document) *Document {
	if rpcDoc == nil {
		return nil
	}

	return &Document{
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

func RPCToBlocks(blocks []*repository.Block) []Block {
	var res []Block

	// Not allocating up-front to avoid turning nil into [].
	if len(blocks) > 0 {
		res = make([]Block, len(blocks))
	}

	for i, rb := range blocks {
		if rb == nil {
			continue
		}

		b := Block{
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
				b.Data = make(DataMap)
			}

			b.Data[k] = v
		}

		res[i] = b
	}

	return res
}
