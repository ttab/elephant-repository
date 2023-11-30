package repository

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	rpcdoc "github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/langos"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
	"golang.org/x/exp/slices"
)

type DocumentValidator interface {
	ValidateDocument(document *newsdoc.Document) []revisor.ValidationResult
}

type WorkflowProvider interface {
	HasStatus(name string) bool
	EvaluateRules(input StatusRuleInput) []StatusRuleViolation
}

type DocumentsService struct {
	store     DocStore
	validator DocumentValidator
	workflows WorkflowProvider
}

func NewDocumentsService(
	store DocStore,
	validator DocumentValidator,
	workflows WorkflowProvider,
) *DocumentsService {
	return &DocumentsService{
		store:     store,
		validator: validator,
		workflows: workflows,
	}
}

// Interface guard.
var _ repository.Documents = &DocumentsService{}

// GetStatusHistory returns the history of a status for a document.
func (a *DocumentsService) GetStatusHistory(
	ctx context.Context, req *repository.GetStatusHistoryRequest,
) (*repository.GetStatusHistoryReponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentRead, ScopeDocumentReadAll, ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, ReadPermission)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	if req.Before != 0 && req.Before < 2 {
		return nil, twirp.InvalidArgumentError("before",
			"cannot be non-zero and less that 2")
	}

	history, err := a.store.GetStatusHistory(
		ctx, docUUID, req.Name, req.Before, 10,
	)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to get history from store: %w", err)
	}

	res := repository.GetStatusHistoryReponse{
		Statuses: make([]*repository.Status, len(history)),
	}

	for i := range history {
		res.Statuses[i] = &repository.Status{
			Id:      history[i].ID,
			Version: history[i].Version,
			Creator: history[i].Creator,
			Created: history[i].Created.Format(time.RFC3339),
			Meta:    history[i].Meta,
		}
	}

	return &res, nil
}

// GetPermissions returns the permissions you have for the document.
func (a *DocumentsService) GetPermissions(
	ctx context.Context, req *repository.GetPermissionsRequest,
) (*repository.GetPermissionsResponse, error) {
	auth, err := RequireAnyScope(ctx,
		ScopeDocumentRead, ScopeDocumentReadAll,
		ScopeDocumentWrite, ScopeDocumentDelete,
		ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	resp := repository.GetPermissionsResponse{
		Permissions: make(map[string]string),
	}

	allSet := func(permissions ...Permission) bool {
		for _, p := range permissions {
			_, ok := resp.Permissions[string(p)]
			if !ok {
				return false
			}
		}

		return true
	}

	elevated := map[Permission][]string{
		ReadPermission:  {ScopeDocumentReadAll, ScopeDocumentAdmin},
		WritePermission: {ScopeDocumentAdmin},
	}

	for permission, scopes := range elevated {
		for _, s := range scopes {
			if !auth.Claims.HasScope(s) {
				continue
			}

			resp.Permissions[string(permission)] = "scope://" + s

			break
		}
	}

	// Return early if elevated privileges already granted us the needed
	// permissions.
	if allSet(ReadPermission, WritePermission) {
		return &resp, nil
	}

	acl, err := a.store.GetDocumentACL(ctx, docUUID)
	if err != nil {
		return nil, twirp.InternalErrorf("failed to read document ACL: %w", err)
	}

	subs := []string{auth.Claims.Subject}
	subs = append(subs, auth.Claims.Units...)

	for _, sub := range subs {
		perms := aclPermissions(sub, acl)
		for _, perm := range perms {
			if allSet(perm) {
				continue
			}

			resp.Permissions[string(perm)] = sub
		}

		if allSet(ReadPermission, WritePermission) {
			break
		}
	}

	return &resp, nil
}

func aclPermissions(sub string, acl []ACLEntry) []Permission {
	var perms []Permission

	for _, e := range acl {
		if e.URI != sub {
			continue
		}

		for _, p := range e.Permissions {
			perm := Permission(p)

			if slices.Contains(perms, perm) {
				continue
			}

			perms = append(perms, perm)
		}
	}

	return perms
}

// CompactedEventlog implements repository.Documents.
func (a *DocumentsService) CompactedEventlog(
	ctx context.Context,
	req *repository.GetCompactedEventlogRequest,
) (*repository.GetCompactedEventlogResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeEventlogRead, ScopeDocumentAdmin)
	if err != nil {
		return nil, err
	}

	lastID, err := a.store.GetLastEventID(ctx)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return &repository.GetCompactedEventlogResponse{}, nil
	} else if err != nil {
		return nil, fmt.Errorf(
			"failed to get the latest event ID: %w", err)
	}

	cr := GetCompactedEventlogRequest{
		After:  req.After,
		Until:  req.Until,
		Offset: req.Offset,
		Type:   req.Type,
	}

	if req.Limit != 0 {
		cr.Limit = &req.Limit
	}

	if cr.Until == 0 {
		increment := max(req.Limit, req.Limit*5, 500)
		cr.Until = min(cr.After+int64(increment), lastID)
	}

	switch {
	case cr.Until <= cr.After:
		return nil, twirp.InvalidArgumentError("until",
			"until must be greater than 'after'")
	case cr.Until > lastID:
		return nil, twirp.InvalidArgumentError("until",
			"cannot be greater than the latest event ID")
	case cr.Until-cr.After > 10000:
		return nil, twirp.InvalidArgumentError("until",
			"`until` cannot be greater than `after`+10000")
	}

	evts, err := a.store.GetCompactedEventlog(ctx, cr)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read eventlog from database: %w", err)
	}

	var res repository.GetCompactedEventlogResponse

	for i := range evts {
		res.Items = append(res.Items, EventToRPC(evts[i]))
	}

	return &res, nil
}

// Eventlog returns document update events, optionally waiting for new events.
func (a *DocumentsService) Eventlog(
	ctx context.Context, req *repository.GetEventlogRequest,
) (*repository.GetEventlogResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeEventlogRead, ScopeDocumentAdmin)
	if err != nil {
		return nil, err
	}

	var (
		wait      time.Duration
		waitBatch time.Duration
	)

	if req.WaitMs == 0 {
		wait = 2 * time.Second
	} else {
		wait = time.Duration(req.WaitMs) * time.Millisecond
	}

	if req.BatchWaitMs == 0 {
		waitBatch = 200 * time.Millisecond
	} else {
		waitBatch = time.Duration(req.BatchWaitMs) * time.Millisecond
	}

	after := req.After
	if after < 0 {
		evt, err := a.store.GetLastEvent(ctx)

		switch {
		case IsDocStoreErrorCode(err, ErrCodeNotFound):
			after = 0
		case err != nil:
			return nil, twirp.InternalErrorf(
				"failed to get last event: %w", err)
		default:
			after = evt.ID + after
		}
	}

	limit := req.BatchSize
	if limit == 0 {
		limit = 10
	}

	evts, err := a.store.GetEventlog(ctx, after, limit)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to fetch events from store: %w", err)
	}

	var res repository.GetEventlogResponse

	maxID := req.After

	for i := range evts {
		if evts[i].ID > maxID {
			maxID = evts[i].ID
		}

		res.Items = append(res.Items, EventToRPC(evts[i]))
	}

	if wait < 0 || len(res.Items) == int(limit) {
		return &res, nil
	}

	return a.eventlogWaitLoop(ctx, maxID, limit, wait, waitBatch, &res)
}

func (a *DocumentsService) eventlogWaitLoop(
	ctx context.Context, after int64, limit int32,
	wait time.Duration, waitBatch time.Duration,
	res *repository.GetEventlogResponse,
) (*repository.GetEventlogResponse, error) {
	var batchTimeout <-chan time.Time

	timeout := time.After(wait)

	if len(res.Items) > 0 {
		batchTimeout = time.After(waitBatch)
	}

	newEvent := make(chan int64, 1)

	a.store.OnEventlog(ctx, newEvent)

	for {
		select {
		case <-batchTimeout:
			return res, nil
		case <-timeout:
			return res, nil
		case <-ctx.Done():
			return nil, twirp.Canceled.Error("context cancelled")
		case <-newEvent:
		}

		evts, err := a.store.GetEventlog(ctx, after, limit-int32(len(res.Items)))
		if err != nil {
			return nil, twirp.InternalErrorf(
				"failed to fetch events from store: %w", err)
		}

		for i := range evts {
			if evts[i].ID > after {
				after = evts[i].ID
			}

			res.Items = append(res.Items, EventToRPC(evts[i]))
		}

		if len(res.Items) == int(limit) {
			return res, nil
		}

		if batchTimeout == nil && len(res.Items) > 0 {
			batchTimeout = time.After(waitBatch)
		}
	}
}

func RPCToEvent(evt *repository.EventlogItem) (Event, error) {
	acl := make([]ACLEntry, len(evt.Acl))

	for i, a := range evt.Acl {
		if a == nil {
			continue
		}

		acl[i] = ACLEntry{
			URI:         a.Uri,
			Permissions: a.Permissions,
		}
	}

	docUUID, err := uuid.Parse(evt.Uuid)
	if err != nil {
		return Event{}, fmt.Errorf(
			"invalid document UUID for event: %w", err)
	}

	timestamp, err := time.Parse(time.RFC3339, evt.Timestamp)
	if err != nil {
		return Event{}, fmt.Errorf(
			"invalid timestamp for event: %w", err)
	}

	return Event{
		ID:        evt.Id,
		Event:     EventType(evt.Event),
		UUID:      docUUID,
		Type:      evt.Type,
		Timestamp: timestamp,
		Updater:   evt.UpdaterUri,
		Version:   evt.Version,
		Status:    evt.Status,
		StatusID:  evt.StatusId,
		ACL:       acl,
	}, nil
}

func EventToRPC(evt Event) *repository.EventlogItem {
	acl := make([]*repository.ACLEntry, len(evt.ACL))

	for i, a := range evt.ACL {
		acl[i] = &repository.ACLEntry{
			Uri:         a.URI,
			Permissions: a.Permissions,
		}
	}

	return &repository.EventlogItem{
		Id:         evt.ID,
		Event:      string(evt.Event),
		Uuid:       evt.UUID.String(),
		Type:       evt.Type,
		Timestamp:  evt.Timestamp.Format(time.RFC3339),
		UpdaterUri: evt.Updater,
		Version:    evt.Version,
		Status:     evt.Status,
		StatusId:   evt.StatusID,
		Acl:        acl,
	}
}

// Delete implements repository.Documents.
func (a *DocumentsService) Delete(
	ctx context.Context, req *repository.DeleteDocumentRequest,
) (*repository.DeleteDocumentResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentDelete, ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	if req.IfMatch < -1 {
		return nil, twirp.InvalidArgumentError("if_match",
			"cannot be less than -1")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, WritePermission)
	if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
		// Treat a delete of a document that doesn't exist as ok.
		return &repository.DeleteDocumentResponse{}, nil
	} else if err != nil {
		return nil, err
	}

	err = a.store.Delete(ctx, DeleteRequest{
		UUID:      docUUID,
		Updated:   time.Now(),
		Updater:   auth.Claims.Subject,
		Meta:      req.Meta,
		IfMatch:   req.IfMatch,
		LockToken: req.LockToken,
	})

	switch {
	case IsDocStoreErrorCode(err, ErrCodeFailedPrecondition):
		return nil, twirp.FailedPrecondition.Error(err.Error())
	case IsDocStoreErrorCode(err, ErrCodeDocumentLock):
		return nil, twirp.FailedPrecondition.Error("the document is locked by someone else")
	case IsDocStoreErrorCode(err, ErrCodeDeleteLock):
		// Treating a delete call as a success if the delete already is
		// in progress.
		return &repository.DeleteDocumentResponse{}, nil
	case err != nil:
		return nil, twirp.InternalErrorf(
			"failed to delete document from data store: %w", err)
	}

	return &repository.DeleteDocumentResponse{}, nil
}

// Get implements repository.Documents.
func (a *DocumentsService) Get(
	ctx context.Context, req *repository.GetDocumentRequest,
) (*repository.GetDocumentResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentRead, ScopeDocumentReadAll,
		ScopeDocumentAdmin,
	)
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

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, ReadPermission)
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
		Document: rpcdoc.DocumentToRPC(*doc),
		Version:  version,
	}, nil
}

// GetHistory implements repository.Documents.
func (a *DocumentsService) GetHistory(
	ctx context.Context, req *repository.GetHistoryRequest,
) (*repository.GetHistoryResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentRead, ScopeDocumentReadAll,
		ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, ReadPermission)
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

func (a *DocumentsService) accessCheck(
	ctx context.Context,
	auth *elephantine.AuthInfo, docUUID uuid.UUID,
	permission Permission,
) error {
	if auth.Claims.HasAnyScope(ScopeDocumentAdmin) {
		return nil
	}

	if permission == ReadPermission && auth.Claims.HasScope(ScopeDocumentReadAll) {
		return nil
	}

	access, err := a.store.CheckPermission(ctx, CheckPermissionRequest{
		UUID: docUUID,
		GranteeURIs: append([]string{auth.Claims.Subject},
			auth.Claims.Units...),
		Permission: permission,
	})
	if err != nil {
		return twirp.InternalErrorf(
			"failed to check document permissions: %w", err)
	}

	switch access {
	case PermissionCheckNoSuchDocument:
		return twirp.NotFoundError("no such document")
	case PermissionCheckDenied:
		return twirp.PermissionDenied.Errorf(
			"no %s permission for the document", permission.Name())
	case PermissionCheckAllowed:
	}

	return nil
}

// GetMeta implements repository.Documents.
func (a *DocumentsService) GetMeta(
	ctx context.Context, req *repository.GetMetaRequest,
) (*repository.GetMetaResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentRead, ScopeDocumentReadAll,
		ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, ReadPermission)
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

	if meta.Lock.Expires != (time.Time{}) {
		resp.Lock = &repository.Lock{
			Uri:     meta.Lock.URI,
			Created: meta.Lock.Created.Format(time.RFC3339),
			Expires: meta.Lock.Expires.Format(time.RFC3339),
			App:     meta.Lock.App,
			Comment: meta.Lock.Comment,
		}
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
func (a *DocumentsService) Update(
	ctx context.Context, req *repository.UpdateRequest,
) (*repository.UpdateResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := a.verifyUpdateRequests(ctx,
		[]*repository.UpdateRequest{req})
	if err != nil {
		return nil, err
	}

	up, err := a.buildUpdateRequest(auth, req)
	if err != nil {
		return nil, err
	}

	res, err := a.store.Update(ctx, a.workflows, []*UpdateRequest{up})
	if err != nil {
		return nil, twirpErrorFromDocumentUpdateError(err)
	}

	return &repository.UpdateResponse{
		Version: res[0].Version,
	}, nil
}

// BulkUpdate implements repository.Documents.
func (a *DocumentsService) BulkUpdate(
	ctx context.Context,
	req *repository.BulkUpdateRequest,
) (*repository.BulkUpdateResponse, error) {
	auth, err := a.verifyUpdateRequests(ctx, req.Updates)
	if err != nil {
		return nil, err
	}

	var updates []*UpdateRequest

	dedupe := make(map[string]bool)

	for _, update := range req.Updates {
		isDuplicate := dedupe[update.Uuid]
		if isDuplicate {
			return nil, twirp.InvalidArgumentError("updates",
				"a document can only be updated once in a batch")
		}

		dedupe[update.Uuid] = true

		up, err := a.buildUpdateRequest(auth, update)
		if err != nil {
			return nil, err
		}

		updates = append(updates, up)
	}

	res, err := a.store.Update(ctx, a.workflows, updates)
	if err != nil {
		return nil, twirpErrorFromDocumentUpdateError(err)
	}

	var resp repository.BulkUpdateResponse

	for i := range res {
		resp.Version = append(resp.Version, res[i].Version)
	}

	return &resp, nil
}

func twirpErrorFromDocumentUpdateError(err error) error {
	switch {
	case IsDocStoreErrorCode(err, ErrCodeOptimisticLock):
		return twirp.FailedPrecondition.Error(err.Error())
	case IsDocStoreErrorCode(err, ErrCodeBadRequest):
		return twirp.InvalidArgumentError("document", err.Error())
	case IsDocStoreErrorCode(err, ErrCodePermissionDenied):
		return twirp.PermissionDenied.Error(err.Error())
	case IsDocStoreErrorCode(err, ErrCodeDeleteLock):
		return twirp.FailedPrecondition.Error(err.Error())
	case err != nil:
		return twirp.InternalErrorf(
			"failed to update document: %w", err)
	}

	return nil
}

func (a *DocumentsService) buildUpdateRequest(
	auth *elephantine.AuthInfo,
	req *repository.UpdateRequest,
) (*UpdateRequest, error) {
	docUUID := uuid.MustParse(req.Uuid)
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

	up := UpdateRequest{
		UUID:      docUUID,
		Updated:   updated,
		Updater:   updater,
		Meta:      req.Meta,
		Status:    RPCToStatusUpdate(req.Status),
		IfMatch:   req.IfMatch,
		LockToken: req.LockToken,
	}

	if req.Document != nil {
		doc := rpcdoc.DocumentFromRPC(req.Document)

		doc.UUID = docUUID.String()

		validationResult := a.validator.ValidateDocument(&doc)

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

		up.Document = &doc
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

	return &up, nil
}

// verifyUpdateRequest verifies that a set of update request are correct, and
// that the user has the necessary permissions for making the updates.
func (a *DocumentsService) verifyUpdateRequests(
	ctx context.Context,
	updates []*repository.UpdateRequest,
) (*elephantine.AuthInfo, error) {
	auth, err := RequireAnyScope(ctx,
		ScopeDocumentWrite, ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	for _, req := range updates {
		err := a.verifyUpdateRequest(ctx, auth, req)
		if err != nil {
			return nil, err
		}
	}

	return auth, nil
}

func (a *DocumentsService) verifyUpdateRequest(
	ctx context.Context,
	auth *elephantine.AuthInfo,
	req *repository.UpdateRequest,
) error {
	if req.ImportDirective != nil && !auth.Claims.HasAnyScope(
		ScopeDocumentImport, ScopeDocumentAdmin) {
		return twirp.PermissionDenied.Error(
			"no import directive permission")
	}

	if req.Document == nil && len(req.Status) == 0 && len(req.Acl) == 0 {
		return twirp.InvalidArgumentError(
			"document",
			"required when no status or ACL updates are included")
	}

	if req.IfMatch < -1 {
		return twirp.InvalidArgumentError("if_match",
			"cannot be less than -1")
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return err
	}

	if req.Document != nil && req.Document.Uuid == "" {
		req.Document.Uuid = docUUID.String()
	} else if req.Document != nil && req.Document.Uuid != docUUID.String() {
		return twirp.InvalidArgumentError("document.uuid",
			"the document must have the same UUID as the request uuid")
	}

	if req.Document != nil {
		if req.Document.Uuid == "" {
			req.Document.Uuid = docUUID.String()
		} else if req.Document.Uuid != docUUID.String() {
			return twirp.InvalidArgumentError("document.uuid",
				"the document must have the same UUID as the request uuid")
		}

		if req.Document.Uri == "" {
			return twirp.RequiredArgumentError("document.uri")
		}

		if req.Document.Language == "" {
			return twirp.RequiredArgumentError("document.language")
		}

		_, err := langos.GetLanguage(req.Document.Language)
		if err != nil {
			return twirp.InvalidArgumentError("document.language",
				err.Error())
		}
	}

	for i, s := range req.Status {
		if s == nil {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d", i),
				"a status cannot be nil")
		}

		if s.Name == "" {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.name", i),
				"a status cannot have an empty name")
		}

		if s.Version < 0 {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.version", i),
				"cannot be negative")
		}

		if req.Document == nil && s.Version == 0 {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.version", i),
				"required when no document is included")
		}

		if !a.workflows.HasStatus(s.Name) {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("status.%d.name", i),
				fmt.Sprintf("unknown status %q", s.Name))
		}
	}

	for i, e := range req.Acl {
		if e == nil {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("acl.%d", i),
				"an ACL entry cannot be nil")
		}

		if e.Uri == "" {
			return twirp.InvalidArgumentError(
				fmt.Sprintf("acl.%d.uri", i),
				"an ACL grantee URI cannot be empty")
		}

		for _, p := range e.Permissions {
			// TODO: Should be validated against a list of
			// acceptable permissions, it's not like
			// []string{"z",".","k"} is valid.
			if len(p) != 1 {
				return twirp.InvalidArgumentError(
					fmt.Sprintf("acl.%d.permissions", i),
					"a permission must be a single character")
			}
		}
	}

	// Check for ACL write permission, but allow the write if no
	// document is found, as we want to allow the creation of new
	// documents.
	err = a.accessCheck(ctx, auth, docUUID, WritePermission)
	if err != nil && !elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
		return err
	}

	return nil
}

// Validate implements repository.Documents.
func (a *DocumentsService) Validate(
	_ context.Context, req *repository.ValidateRequest,
) (*repository.ValidateResponse, error) {
	if req.Document == nil {
		return nil, twirp.RequiredArgumentError("document")
	}

	doc := rpcdoc.DocumentFromRPC(req.Document)

	validationResult := a.validator.ValidateDocument(&doc)

	var res repository.ValidateResponse

	for _, r := range validationResult {
		res.Errors = append(res.Errors, &repository.ValidationResult{
			Entity: EntityRefToRPC(r.Entity),
			Error:  r.Error,
		})
	}

	return &res, nil
}

func (a *DocumentsService) Lock(
	ctx context.Context, req *repository.LockRequest,
) (*repository.LockResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentWrite, ScopeDocumentDelete, ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, WritePermission)
	if err != nil {
		return nil, err
	}

	if req.Ttl == 0 {
		return nil, twirp.RequiredArgumentError("ttl")
	}

	lock, err := a.store.Lock(ctx, LockRequest{
		UUID:    docUUID,
		TTL:     req.Ttl,
		URI:     auth.Claims.Subject,
		App:     req.App,
		Comment: req.Comment,
	})

	switch {
	case IsDocStoreErrorCode(err, ErrCodeDeleteLock), IsDocStoreErrorCode(err, ErrCodeNotFound):
		return nil, twirp.FailedPrecondition.Error("could not find the document")
	case IsDocStoreErrorCode(err, ErrCodeDocumentLock):
		return nil, twirp.FailedPrecondition.Error("the document is locked by someone else")
	case err != nil:
		return nil, fmt.Errorf("could not obtain lock: %w", err)
	}

	return &repository.LockResponse{
		Token: lock.Token,
	}, nil
}

// ExtendLock extends the expiration of an existing lock.
func (a *DocumentsService) ExtendLock(
	ctx context.Context, req *repository.ExtendLockRequest,
) (*repository.LockResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentWrite, ScopeDocumentDelete, ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, WritePermission)
	if err != nil {
		return nil, err
	}

	if req.Ttl == 0 {
		return nil, twirp.RequiredArgumentError("ttl")
	}

	if req.Token == "" {
		return nil, twirp.RequiredArgumentError("token")
	}

	lock, err := a.store.UpdateLock(ctx, UpdateLockRequest{
		UUID:  docUUID,
		TTL:   req.Ttl,
		Token: req.Token,
	})

	switch {
	case IsDocStoreErrorCode(err, ErrCodeDeleteLock), IsDocStoreErrorCode(err, ErrCodeNotFound):
		return nil, twirp.FailedPrecondition.Error("could not find the document")
	case IsDocStoreErrorCode(err, ErrCodeNoSuchLock):
		return nil, twirp.FailedPrecondition.Error("the document is not locked by anyone")
	case IsDocStoreErrorCode(err, ErrCodeDocumentLock):
		return nil, twirp.FailedPrecondition.Error("the doument is locked by someone else")
	case err != nil:
		return nil, fmt.Errorf("could not obtain lock: %w", err)
	}

	return &repository.LockResponse{
		Token: lock.Token,
	}, nil
}

func (a *DocumentsService) Unlock(
	ctx context.Context, req *repository.UnlockRequest,
) (*repository.UnlockResponse, error) {
	elephantine.SetLogMetadata(ctx,
		elephantine.LogKeyDocumentUUID, req.Uuid,
	)

	auth, err := RequireAnyScope(ctx,
		ScopeDocumentWrite, ScopeDocumentDelete, ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	err = a.accessCheck(ctx, auth, docUUID, WritePermission)
	if err != nil {
		return nil, err
	}

	if req.Token == "" {
		return nil, twirp.RequiredArgumentError("token")
	}

	err = a.store.Unlock(ctx, docUUID, req.Token)

	switch {
	case IsDocStoreErrorCode(err, ErrCodeDeleteLock):
		return &repository.UnlockResponse{}, nil
	case IsDocStoreErrorCode(err, ErrCodeDocumentLock):
		return nil, twirp.FailedPrecondition.Errorf("the document is locked by someone else")
	case err != nil:
		return nil, fmt.Errorf("could not unlock document: %w", err)
	}

	return &repository.UnlockResponse{}, nil
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
