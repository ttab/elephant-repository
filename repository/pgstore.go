package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

const (
	elephantCRC            = 3997770000
	LockSigningKeys        = elephantCRC + 1
	LockLogicalReplication = elephantCRC + 2
)

type PGDocStoreOptions struct {
	DeleteTimeout time.Duration
}

type PGDocStore struct {
	logger *slog.Logger
	pool   *pgxpool.Pool
	reader *postgres.Queries
	opts   PGDocStoreOptions

	archived  *FanOut[ArchivedEvent]
	schemas   *FanOut[SchemaEvent]
	workflows *FanOut[WorkflowEvent]
	eventlog  *FanOut[int64]
}

func NewPGDocStore(
	logger *slog.Logger, pool *pgxpool.Pool,
	options PGDocStoreOptions,
) (*PGDocStore, error) {
	if options.DeleteTimeout == 0 {
		options.DeleteTimeout = 5 * time.Second
	}

	return &PGDocStore{
		logger:    logger,
		pool:      pool,
		reader:    postgres.New(pool),
		opts:      options,
		archived:  NewFanOut[ArchivedEvent](),
		schemas:   NewFanOut[SchemaEvent](),
		workflows: NewFanOut[WorkflowEvent](),
		eventlog:  NewFanOut[int64](),
	}, nil
}

// OnSchemaUpdate notifies the channel ch of all archived status
// updates. Subscription is automatically cancelled once the context is
// cancelled.
//
// Note that we don't provide any delivery guarantees for these events.
// non-blocking send is used on ch, so if it's unbuffered events will be
// discarded if the receiver is busy.
func (s *PGDocStore) OnArchivedUpdate(
	ctx context.Context, ch chan ArchivedEvent,
) {
	go s.archived.Listen(ctx, ch, func(_ ArchivedEvent) bool {
		return true
	})
}

// OnSchemaUpdate notifies the channel ch of all schema updates. Subscription is
// automatically cancelled once the context is cancelled.
//
// Note that we don't provide any delivery guarantees for these events.
// non-blocking send is used on ch, so if it's unbuffered events will be
// discarded if the receiver is busy.
func (s *PGDocStore) OnSchemaUpdate(
	ctx context.Context, ch chan SchemaEvent,
) {
	go s.schemas.Listen(ctx, ch, func(_ SchemaEvent) bool {
		return true
	})
}

// OnWorkflowUpdate notifies the channel ch of all workflow updates.
// Subscription is automatically cancelled once the context is cancelled.
//
// Note that we don't provide any delivery guarantees for these events.
// non-blocking send is used on ch, so if it's unbuffered events will be
// discarded if the receiver is busy.
func (s *PGDocStore) OnWorkflowUpdate(
	ctx context.Context, ch chan WorkflowEvent,
) {
	go s.workflows.Listen(ctx, ch, func(_ WorkflowEvent) bool {
		return true
	})
}

// OnEventlog notifies the channel ch of all new eventlog IDs. Subscription is
// automatically cancelled once the context is cancelled.
//
// Note that we don't provide any delivery guarantees for these events.
// non-blocking send is used on ch, so if it's unbuffered events will be
// discarded if the receiver is busy.
func (s *PGDocStore) OnEventlog(
	ctx context.Context, ch chan int64,
) {
	go s.eventlog.Listen(ctx, ch, func(_ int64) bool {
		return true
	})
}

// RunListener opens a connection to the database and subscribes to all store
// notifications.
func (s *PGDocStore) RunListener(ctx context.Context) {
	for {
		err := s.runListener(ctx)
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			s.logger.ErrorCtx(
				ctx, "failed to run notification listener",
				elephantine.LogKeyError, err,
			)
		}

		time.Sleep(5 * time.Second)
	}
}

func (s *PGDocStore) runListener(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection from pool: %w", err)
	}

	defer conn.Release()

	notifications := []NotifyChannel{
		NotifyArchived,
		NotifySchemasUpdated,
		NotifyWorkflowsUpdated,
		NotifyEventlog,
	}

	for _, channel := range notifications {
		ident := pgx.Identifier{string(channel)}

		_, err := conn.Exec(ctx, "LISTEN "+ident.Sanitize())
		if err != nil {
			return fmt.Errorf("failed to start listening to %q: %w",
				channel, err)
		}
	}

	received := make(chan *pgconn.Notification)
	grp, gCtx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		for {
			notification, err := conn.Conn().WaitForNotification(gCtx)
			if err != nil {
				return fmt.Errorf(
					"error while waiting for notification: %w", err)
			}

			received <- notification
		}
	})

	grp.Go(func() error {
		for {
			var notification *pgconn.Notification

			select {
			case <-ctx.Done():
				return ctx.Err() //nolint:wrapcheck
			case notification = <-received:
			}

			switch NotifyChannel(notification.Channel) {
			case NotifySchemasUpdated:
				var e SchemaEvent

				err := json.Unmarshal(
					[]byte(notification.Payload), &e)
				if err != nil {
					break
				}

				s.schemas.Notify(e)
			case NotifyArchived:
				var e ArchivedEvent

				err := json.Unmarshal(
					[]byte(notification.Payload), &e)
				if err != nil {
					break
				}

				s.archived.Notify(e)
			case NotifyWorkflowsUpdated:
				var e WorkflowEvent

				err := json.Unmarshal(
					[]byte(notification.Payload), &e)
				if err != nil {
					break
				}

				s.workflows.Notify(e)
			case NotifyEventlog:
				var e int64

				err := json.Unmarshal(
					[]byte(notification.Payload), &e)
				if err != nil {
					break
				}

				s.eventlog.Notify(e)
			}
		}
	})

	err = grp.Wait()
	if err != nil {
		return err //nolint:wrapcheck
	}

	return nil
}

// Delete implements DocStore.
func (s *PGDocStore) Delete(ctx context.Context, req DeleteRequest) error {
	var metaJSON []byte

	if len(req.Meta) > 0 {
		mj, err := json.Marshal(req.Meta)
		if err != nil {
			return fmt.Errorf(
				"failed to marshal metadata for storage: %w", err)
		}

		metaJSON = mj
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.SafeRollback(ctx, s.logger, tx, "document delete")

	q := postgres.New(tx)

	info, err := s.updatePreflight(ctx, q, req.UUID, req.IfMatch)
	if err != nil {
		return err
	}

	if !info.Exists {
		return nil
	}

	timeout := time.After(s.opts.DeleteTimeout)

	archived := make(chan ArchivedEvent)

	go s.archived.Listen(ctx, archived, func(e ArchivedEvent) bool {
		return e.UUID == req.UUID
	})

	for {
		remaining, err := q.GetDocumentUnarchivedCount(ctx, req.UUID)
		if err != nil {
			return fmt.Errorf(
				"failed to check archiving status: %w", err)
		}

		if remaining == 0 {
			break
		}

		select {
		case <-timeout:
			return DocStoreErrorf(ErrCodeFailedPrecondition,
				"timed out while waiting for archiving to complete")
		case <-time.After(1 * time.Second):
		case <-archived:
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck
		}
	}

	recordID, err := q.InsertDeleteRecord(ctx,
		postgres.InsertDeleteRecordParams{
			UUID:       req.UUID,
			URI:        info.Info.URI,
			Type:       info.Info.Type,
			Version:    info.Info.CurrentVersion,
			Created:    pg.Time(req.Updated),
			CreatorUri: req.Updater,
			Meta:       metaJSON,
		})
	if err != nil {
		return fmt.Errorf("failed to create delete record: %w", err)
	}

	err = q.DeleteDocument(ctx, postgres.DeleteDocumentParams{
		UUID:     req.UUID,
		URI:      info.Info.URI,
		RecordID: recordID,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to delete document from database: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit delete: %w", err)
	}

	return nil
}

// GetDocument implements DocStore.
func (s *PGDocStore) GetDocument(
	ctx context.Context, uuid uuid.UUID, version int64,
) (*newsdoc.Document, error) {
	var (
		err  error
		data []byte
	)

	if version == 0 {
		data, err = s.reader.GetDocumentData(ctx, uuid)
	} else {
		data, err = s.reader.GetDocumentVersionData(ctx,
			postgres.GetDocumentVersionDataParams{
				UUID:    uuid,
				Version: version,
			})
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch document data: %w", err)
	}

	// TODO: check for nil data after pruning has been implemented.

	var d newsdoc.Document

	err = json.Unmarshal(data, &d)
	if err != nil {
		return nil, fmt.Errorf(
			"got an unreadable document from the database: %w", err)
	}

	return &d, nil
}

func (s *PGDocStore) GetEventlog(
	ctx context.Context, after int64, limit int32,
) ([]Event, error) {
	res, err := s.reader.GetEventlog(ctx, postgres.GetEventlogParams{
		After:    after,
		RowLimit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read from database: %w", err)
	}

	evts := make([]Event, len(res))

	for i := range res {
		e := Event{
			ID:        res[i].ID,
			Event:     EventType(res[i].Event),
			UUID:      res[i].UUID,
			Timestamp: res[i].Timestamp.Time,
			Updater:   res[i].Updater.String,
			Type:      res[i].Type.String,
			Version:   res[i].Version.Int64,
			Status:    res[i].Status.String,
			StatusID:  res[i].StatusID.Int64,
		}

		if res[i].Acl != nil {
			err := json.Unmarshal(res[i].Acl, &e.ACL)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to unmarshal event ACL: %w", err)
			}
		}

		evts[i] = e
	}

	return evts, nil
}

func (s *PGDocStore) GetSinkPosition(ctx context.Context, name string) (int64, error) {
	pos, err := s.reader.GetEventsinkPosition(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to read database record: %w", err)
	}

	return pos, nil
}

func (s *PGDocStore) SetSinkPosition(ctx context.Context, name string, pos int64) error {
	err := s.reader.UpdateEventsinkPosition(ctx, postgres.UpdateEventsinkPositionParams{
		Name:     name,
		Position: pos,
	})
	if err != nil {
		return fmt.Errorf("failed update database record: %w", err)
	}

	return nil
}

func (s *PGDocStore) GetVersion(
	ctx context.Context, uuid uuid.UUID, version int64,
) (DocumentUpdate, error) {
	v, err := s.reader.GetVersion(ctx, postgres.GetVersionParams{
		UUID:    uuid,
		Version: version,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DocumentUpdate{}, DocStoreErrorf(
			ErrCodeNotFound, "not found")
	} else if err != nil {
		return DocumentUpdate{}, fmt.Errorf(
			"failed to fetch version info: %w", err)
	}

	up := DocumentUpdate{
		Version: version,
		Creator: v.CreatorUri,
		Created: v.Created.Time,
	}

	if v.Meta != nil {
		err := json.Unmarshal(v.Meta, &up.Meta)
		if err != nil {
			return DocumentUpdate{}, fmt.Errorf(
				"failed to unmarshal metadata for version %d: %w",
				version, err)
		}
	}

	return up, nil
}

func (s *PGDocStore) GetVersionHistory(
	ctx context.Context, uuid uuid.UUID,
	before int64, count int,
) ([]DocumentUpdate, error) {
	history, err := s.reader.GetVersions(ctx, postgres.GetVersionsParams{
		UUID:   uuid,
		Before: before,
		Count:  int32(count),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch version history: %w", err)
	}

	var updates []DocumentUpdate

	for _, v := range history {
		up := DocumentUpdate{
			Version: v.Version,
			Creator: v.CreatorUri,
			Created: v.Created.Time,
		}

		if v.Meta != nil {
			err := json.Unmarshal(v.Meta, &up.Meta)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to unmarshal metadata for version %d: %w",
					v.Version, err)
			}
		}

		updates = append(updates, up)
	}

	return updates, nil
}

func (s *PGDocStore) GetStatusHistory(
	ctx context.Context, uuid uuid.UUID,
	name string, before int64, count int,
) ([]Status, error) {
	history, err := s.reader.GetStatusVersions(ctx, postgres.GetStatusVersionsParams{
		UUID:   uuid,
		Name:   name,
		Before: before,
		Count:  int32(count),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch status history: %w", err)
	}

	statuses := make([]Status, len(history))

	for i := range history {
		s := Status{
			ID:      history[i].ID,
			Version: history[i].Version,
			Creator: history[i].CreatorUri,
			Created: history[i].Created.Time,
		}

		if history[i].Meta != nil {
			err := json.Unmarshal(history[i].Meta, &s.Meta)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to unmarshal metadata for status %d: %w",
					s.ID, err)
			}
		}

		statuses[i] = s
	}

	return statuses, nil
}

// GetDocumentMeta implements DocStore.
func (s *PGDocStore) GetDocumentMeta(
	ctx context.Context, uuid uuid.UUID,
) (*DocumentMeta, error) {
	info, err := s.reader.GetDocumentInfo(ctx, uuid)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch document info: %w", err)
	}

	if info.Deleting {
		return &DocumentMeta{Deleting: true}, nil
	}

	meta := DocumentMeta{
		Created:        info.Created.Time,
		Modified:       info.Updated.Time,
		CurrentVersion: info.CurrentVersion,
		Statuses:       make(map[string]Status),
		Deleting:       info.Deleting,
		Lock: Lock{
			URI:     info.LockUri.String,
			Created: info.Created.Time,
			Expires: info.LockExpires.Time,
			App:     info.LockApp.String,
			Comment: info.LockComment.String,
		},
	}

	heads, err := s.getFullDocumentHeads(ctx, s.reader, uuid)
	if err != nil {
		return nil, err
	}

	meta.Statuses = heads

	acl, err := s.GetDocumentACL(ctx, uuid)
	if err != nil {
		return nil, err
	}

	meta.ACL = acl

	return &meta, nil
}

func (s *PGDocStore) GetDocumentACL(
	ctx context.Context, uuid uuid.UUID,
) ([]ACLEntry, error) {
	aclResult, err := s.reader.GetDocumentACL(ctx, uuid)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document ACL: %w", err)
	}

	var acl []ACLEntry

	for _, a := range aclResult {
		acl = append(acl, ACLEntry{
			URI:         a.URI,
			Permissions: a.Permissions,
		})
	}

	return acl, nil
}

func (s *PGDocStore) getFullDocumentHeads(
	ctx context.Context, q *postgres.Queries, docUUID uuid.UUID,
) (map[string]Status, error) {
	statuses := make(map[string]Status)

	heads, err := q.GetFullDocumentHeads(ctx, docUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document heads: %w", err)
	}

	for _, head := range heads {
		status := Status{
			ID:      head.ID,
			Version: head.Version,
			Creator: head.CreatorUri,
			Created: head.Created.Time,
		}

		if head.Meta != nil {
			err := json.Unmarshal(head.Meta, &status.Meta)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to decode %q metadata: %w",
					head.Name, err)
			}
		}

		statuses[head.Name] = status
	}

	return statuses, nil
}

// CheckPermission implements DocStore.
func (s *PGDocStore) CheckPermission(
	ctx context.Context, req CheckPermissionRequest,
) (CheckPermissionResult, error) {
	access, err := s.reader.CheckPermission(ctx,
		postgres.CheckPermissionParams{
			UUID:       req.UUID,
			URI:        req.GranteeURIs,
			Permission: string(req.Permission),
		})
	if errors.Is(err, pgx.ErrNoRows) {
		return PermissionCheckNoSuchDocument, nil
	} else if err != nil {
		return PermissionCheckDenied, fmt.Errorf(
			"failed check acl: %w", err)
	}

	if !access {
		return PermissionCheckDenied, nil
	}

	return PermissionCheckAllowed, nil
}

// Update implements DocStore.
func (s *PGDocStore) Update(
	ctx context.Context, workflows WorkflowProvider, update UpdateRequest,
) (*DocumentUpdate, error) {
	var docJSON, metaJSON []byte

	// Do serialisation work before we start a transaction. That way we
	// don't keep row locks or hog connections while doing that
	// busy-work. Likewise we don't even try to update the db in the
	// unlikely event that marshalling fails.

	if update.Document != nil {
		dj, err := json.Marshal(update.Document)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal document for storage: %w", err)
		}

		docJSON = dj
	}

	if len(update.Meta) > 0 {
		mj, err := json.Marshal(update.Meta)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal metadata for storage: %w", err)
		}

		metaJSON = mj
	}

	statusMeta := make([][]byte, len(update.Status))

	for i, stat := range update.Status {
		if len(stat.Meta) == 0 {
			continue
		}

		d, err := json.Marshal(stat.Meta)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal %q status metadata for storage: %w",
				stat.Name, err)
		}

		statusMeta[i] = d
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.SafeRollback(ctx, s.logger, tx, "document update")

	q := postgres.New(tx)

	info, err := s.updatePreflight(ctx, q, update.UUID, update.IfMatch)
	if err != nil {
		return nil, err
	}

	docType := info.Info.Type

	up := DocumentUpdate{
		Version: info.Info.CurrentVersion,
		Created: update.Updated,
		Creator: update.Updater,
		Meta:    update.Meta,
	}

	if update.Document != nil {
		up.Version++

		if info.Info.Type != "" && info.Info.Type != update.Document.Type {
			return nil, DocStoreErrorf(ErrCodeBadRequest,
				"cannot change the document type from %q",
				info.Info.Type)
		}

		docType = update.Document.Type

		err = q.CreateVersion(ctx, postgres.CreateVersionParams{
			UUID:         update.UUID,
			Version:      up.Version,
			Created:      pg.Time(up.Created),
			CreatorUri:   up.Creator,
			Meta:         metaJSON,
			DocumentData: docJSON,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create version in database: %w", err)
		}
	}

	statusHeads := make(map[string]Status)

	if len(update.Status) > 0 {
		heads, err := s.getFullDocumentHeads(ctx, s.reader, update.UUID)
		if err != nil {
			return nil, err
		}

		statusHeads = heads
	}

	for i, stat := range update.Status {
		statusID := statusHeads[stat.Name].ID + 1

		status := Status{
			ID:      statusID,
			Creator: up.Creator,
			Version: stat.Version,
			Meta:    stat.Meta,
			Created: up.Created,
		}

		if status.Version == 0 {
			status.Version = up.Version
		}

		input, err := s.buildStatusRuleInput(
			ctx, q, update.UUID, stat.Name, status, up,
			update.Document, update.Meta, statusHeads,
		)
		if err != nil {
			return nil, err
		}

		violations := workflows.EvaluateRules(input)

		for _, v := range violations {
			if v.AccessViolation {
				return nil, DocStoreErrorf(
					ErrCodePermissionDenied,
					"status rule violation %q: %s",
					v.Name,
					v.Description)
			}
		}

		if len(violations) > 0 {
			return nil, DocStoreErrorf(ErrCodeBadRequest,
				"status rule violation: %w", StatusRuleError{
					Violations: violations,
				})
		}

		err = q.CreateStatus(ctx, postgres.CreateStatusParams{
			UUID:       update.UUID,
			Name:       stat.Name,
			ID:         statusID,
			Version:    status.Version,
			Type:       docType,
			Created:    pg.Time(up.Created),
			CreatorUri: up.Creator,
			Meta:       statusMeta[i],
		})
		if err != nil {
			return nil, fmt.Errorf(
				"failed to update %q status: %w",
				stat.Name, err)
		}
	}

	updateACL := update.ACL

	if len(updateACL) == 0 && !info.Exists {
		updateACL = update.DefaultACL
	}

	err = s.updateACL(ctx, q, update.UUID, docType, updateACL)
	if err != nil {
		return nil, fmt.Errorf("failed to update ACL: %w", err)
	}

	// TODO: model links, or should we just skip that? Could a stored
	// procedure iterate over links instead of us doing the batch thing
	// here?

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	return &up, nil
}

func (s *PGDocStore) buildStatusRuleInput(
	ctx context.Context, q *postgres.Queries,
	uuid uuid.UUID, name string, status Status, up DocumentUpdate,
	d *newsdoc.Document, versionMeta newsdoc.DataMap, statusHeads map[string]Status,
) (StatusRuleInput, error) {
	input := StatusRuleInput{
		Name:   name,
		Status: status,
		Update: up,
		Heads:  statusHeads,
	}

	auth, ok := elephantine.GetAuthInfo(ctx)
	if ok {
		input.User = auth.Claims
	}

	if d != nil && status.Version == up.Version {
		input.Document = *d
		input.VersionMeta = versionMeta
	} else if d == nil {
		d, meta, err := s.loadDocument(
			ctx, q, uuid, status.Version)
		if err != nil {
			return StatusRuleInput{}, fmt.Errorf(
				"failed to retrieve document for rule evaluation: %w", err)
		}

		input.VersionMeta = meta
		input.Document = *d
	}

	return input, nil
}

func (s *PGDocStore) loadDocument(
	ctx context.Context, q *postgres.Queries,
	uuid uuid.UUID, version int64,
) (*newsdoc.Document, newsdoc.DataMap, error) {
	docV, err := q.GetFullVersion(ctx,
		postgres.GetFullVersionParams{
			UUID:    uuid,
			Version: version,
		})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load document data: %w", err)
	}

	if docV.DocumentData == nil {
		// TODO: take pruned document versions into account
		return nil, nil, errors.New(
			"no support for retrieving archived versions yet")
	}

	// TODO: should we restore pruned document data based on some condition
	// here? If a new status is created that refers to a previouly pruned
	// version we would probably like for it to be available later.

	var d newsdoc.Document

	err = json.Unmarshal(docV.DocumentData, &d)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to parse stored document: %w", err)
	}

	meta := make(newsdoc.DataMap)

	if docV.Meta != nil {
		err = json.Unmarshal(docV.Meta, &meta)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to parse stored document version meta: %w", err)
		}
	}

	return &d, meta, nil
}

func (s *PGDocStore) UpdateStatus(
	ctx context.Context, req UpdateStatusRequest,
) error {
	return s.withTX(ctx, "status update", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.UpdateStatus(ctx, postgres.UpdateStatusParams{
			Name:     req.Name,
			Disabled: req.Disabled,
		})
		if err != nil {
			return fmt.Errorf("failed to update status: %w", err)
		}

		notifyWorkflowUpdated(ctx, s.logger, q, WorkflowEvent{
			Type: WorkflowEventTypeStatusChange,
			Name: req.Name,
		})

		return nil
	})
}

func (s *PGDocStore) GetStatuses(ctx context.Context) ([]DocumentStatus, error) {
	res, err := s.reader.GetActiveStatuses(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active statuses: %w", err)
	}

	var list []DocumentStatus

	for i := range res {
		list = append(list, DocumentStatus{
			Name: res[i],
		})
	}

	return list, nil
}

func (s *PGDocStore) UpdateStatusRule(
	ctx context.Context, rule StatusRule,
) error {
	if len(rule.AppliesTo) == 0 {
		return DocStoreErrorf(ErrCodeBadRequest,
			"applies_to cannot be empty")
	}

	if len(rule.ForTypes) == 0 {
		return DocStoreErrorf(ErrCodeBadRequest,
			"for_types cannot be empty")
	}

	return s.withTX(ctx, "update status rule", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.UpdateStatusRule(ctx, postgres.UpdateStatusRuleParams{
			Name:        rule.Name,
			Description: rule.Description,
			AccessRule:  rule.AccessRule,
			AppliesTo:   rule.AppliesTo,
			ForTypes:    rule.ForTypes,
			Expression:  rule.Expression,
		})
		if err != nil {
			return fmt.Errorf("failed to update status rule: %w", err)
		}

		notifyWorkflowUpdated(ctx, s.logger, q, WorkflowEvent{
			Type: WorkflowEventTypeStatusRuleChange,
			Name: rule.Name,
		})

		return nil
	})
}

func (s *PGDocStore) DeleteStatusRule(
	ctx context.Context, name string,
) error {
	return s.withTX(ctx, "delete status rule", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.DeleteStatusRule(ctx, name)
		if err != nil {
			return fmt.Errorf("failed to delete status rule: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) GetStatusRules(ctx context.Context) ([]StatusRule, error) {
	res, err := s.reader.GetStatusRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch status rules: %w", err)
	}

	var list []StatusRule

	for _, row := range res {
		list = append(list, StatusRule{
			Name:        row.Name,
			Description: row.Description,
			AccessRule:  row.AccessRule,
			AppliesTo:   row.AppliesTo,
			ForTypes:    row.ForTypes,
			Expression:  row.Expression,
		})
	}

	return list, nil
}

type StatusRuleError struct {
	Violations []StatusRuleViolation
}

func (err StatusRuleError) Error() string {
	var rules []string

	for i := range err.Violations {
		rules = append(rules, err.Violations[i].Name)
	}

	return fmt.Sprintf("violates the following status rules: %s",
		strings.Join(rules, ", "))
}

func (s *PGDocStore) GetSchemaVersions(
	ctx context.Context,
) (map[string]string, error) {
	versions, err := s.reader.GetSchemaVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read from db: %w", err)
	}

	res := make(map[string]string)

	for _, r := range versions {
		res[r.Name] = r.Version
	}

	return res, nil
}

func (s *PGDocStore) Lock(ctx context.Context, uuid uuid.UUID, ttl int32, token string) error {
	now := time.Now()
	expires := now.Add(time.Millisecond * time.Duration(ttl))

	err := s.withTX(ctx, "document locking", func(tx pgx.Tx) error {
		info, err := s.reader.GetDocumentLock(ctx, uuid)

		if errors.Is(err, pgx.ErrNoRows) {
			// return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		} else if err != nil {
			return fmt.Errorf("could not read document: %w", err)
		}
		fmt.Println(info)

		err = s.reader.InsertDocumentLock(ctx, postgres.InsertDocumentLockParams{
			UUID:    uuid,
			Token:   token,
			Created: internal.PGTime(now),
			Expires: internal.PGTime(expires),
		})
		if internal.IsConstraintError(err, "lock_uuid_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		} else if err != nil {
			return fmt.Errorf("failed to insert document lock: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// RegisterSchema implements DocStore.
func (s *PGDocStore) RegisterSchema(
	ctx context.Context, req RegisterSchemaRequest,
) error {
	spec, err := json.Marshal(req.Specification)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal specification for storage: %w", err)
	}

	err = s.withTX(ctx, "schema registration", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err = q.RegisterSchema(ctx, postgres.RegisterSchemaParams{
			Name:    req.Name,
			Version: req.Version,
			Spec:    spec,
		})
		if pg.IsConstraintError(err, "document_schema_pkey") {
			return DocStoreErrorf(ErrCodeExists,
				"schema version already exists")
		} else if err != nil {
			return fmt.Errorf("failed to register schema version: %w", err)
		}

		if req.Activate {
			err = s.activateSchema(ctx, q, req.Name, req.Version)
			if err != nil {
				return fmt.Errorf(
					"failed to activate schema version: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// RegisterSchema implements DocStore.
func (s *PGDocStore) ActivateSchema(
	ctx context.Context, name, version string,
) error {
	return s.withTX(ctx, "schema registration", func(tx pgx.Tx) error {
		return s.activateSchema(ctx, postgres.New(tx), name, version)
	})
}

// RegisterSchema implements DocStore.
func (s *PGDocStore) activateSchema(
	ctx context.Context, q *postgres.Queries, name, version string,
) error {
	err := q.ActivateSchema(ctx, postgres.ActivateSchemaParams{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to activate schema version: %w", err)
	}

	notifySchemaUpdated(ctx, s.logger, q, SchemaEvent{
		Type: SchemaEventTypeActivation,
		Name: name,
	})

	return nil
}

func (s *PGDocStore) withTX(
	ctx context.Context, name string,
	fn func(tx pgx.Tx) error,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.SafeRollback(ctx, s.logger, tx, name)

	err = fn(tx)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// DeactivateSchema implements DocStore.
func (s *PGDocStore) DeactivateSchema(ctx context.Context, name string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.SafeRollback(ctx, s.logger, tx, "schema deactivation")

	q := postgres.New(tx)

	err = q.DeactivateSchema(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to remove active schema: %w", err)
	}

	notifySchemaUpdated(ctx, s.logger, q, SchemaEvent{
		Type: SchemaEventTypeDeactivation,
		Name: name,
	})

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// GetActiveSchemas implements DocStore.
func (s *PGDocStore) GetActiveSchemas(
	ctx context.Context,
) ([]*Schema, error) {
	rows, err := s.reader.GetActiveSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch active schemas: %w", err)
	}

	res := make([]*Schema, len(rows))

	for i := range rows {
		var spec revisor.ConstraintSet

		err := json.Unmarshal(rows[i].Spec, &spec)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid specification for %s@%s in database: %w",
				rows[i].Name, rows[i].Version, err)
		}

		res[i] = &Schema{
			Name:          rows[i].Name,
			Version:       rows[i].Version,
			Specification: spec,
		}
	}

	return res, nil
}

// GetSchema implements DocStore.
func (s *PGDocStore) GetSchema(
	ctx context.Context, name string, version string,
) (*Schema, error) {
	var (
		schema postgres.DocumentSchema
		err    error
	)

	if version == "" {
		schema, err = s.reader.GetActiveSchema(ctx, name)
	} else {
		schema, err = s.reader.GetSchema(ctx, postgres.GetSchemaParams{
			Name:    name,
			Version: version,
		})
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, fmt.Errorf(
			"failed to load schema: %w", err)
	}

	var spec revisor.ConstraintSet

	err = json.Unmarshal(schema.Spec, &spec)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid specification in database: %w", err)
	}

	return &Schema{
		Name:          schema.Name,
		Version:       schema.Version,
		Specification: spec,
	}, nil
}

type StoredReport struct {
	Report        Report
	Enabled       bool
	NextExecution time.Time
}

func (s *PGDocStore) GetReport(
	ctx context.Context, name string,
) (*StoredReport, error) {
	report, err := s.reader.GetReport(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(
			ErrCodeNotFound, "no report with that name")
	} else if err != nil {
		return nil, fmt.Errorf("failed to read from database: %w", err)
	}

	res := StoredReport{
		Enabled:       report.Enabled,
		NextExecution: report.NextExecution.Time,
	}

	err = json.Unmarshal(report.Spec, &res.Report)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal stored report: %w", err)
	}

	tz, err := time.LoadLocation(res.Report.CronTimezone)
	if err != nil {
		return nil, fmt.Errorf("failed to load location: %w", err)
	}

	res.NextExecution = res.NextExecution.In(tz)

	return &res, nil
}

func (s *PGDocStore) UpdateReport(
	ctx context.Context, report Report, enabled bool,
) (time.Time, error) {
	nextExec, err := report.NextTick()
	if err != nil {
		return time.Time{},
			fmt.Errorf("failed to calculate next execution: %w", err)
	}

	spec, err := json.Marshal(report)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"failed to marshal report spec for storage: %w", err)
	}

	err = s.withTX(ctx, "update report", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.UpdateReport(ctx, postgres.UpdateReportParams{
			Name:          report.Name,
			Enabled:       enabled,
			NextExecution: pg.Time(nextExec),
			Spec:          spec,
		})
		if err != nil {
			return fmt.Errorf("failed to save to database: %w", err)
		}

		return nil
	})
	if err != nil {
		return time.Time{}, err
	}

	return nextExec, nil
}

func (s *PGDocStore) RegisterMetricKind(
	ctx context.Context, name string, aggregation Aggregation,
) error {
	return s.withTX(ctx, "register metric kind", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.RegisterMetricKind(ctx, postgres.RegisterMetricKindParams{
			Name:        name,
			Aggregation: int16(aggregation),
		})
		if pg.IsConstraintError(err, "metric_kind_pkey") {
			return DocStoreErrorf(ErrCodeExists,
				"metric kind already exists")
		} else if err != nil {
			return fmt.Errorf("failed to save to databaase: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) DeleteMetricKind(
	ctx context.Context, name string,
) error {
	err := s.reader.DeleteMetricKind(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to delete metric kind: %w", err)
	}

	return nil
}

func (s *PGDocStore) GetMetricKind(
	ctx context.Context, name string,
) (*MetricKind, error) {
	kind, err := s.reader.GetMetricKind(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(
			ErrCodeNotFound, "metric kind not found")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch metric kind: %w", err)
	}

	return &MetricKind{
		Name:        kind.Name,
		Aggregation: Aggregation(kind.Aggregation),
	}, nil
}

func (s *PGDocStore) GetMetricKinds(
	ctx context.Context,
) ([]*MetricKind, error) {
	rows, err := s.reader.GetMetricKinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metric kinds: %w", err)
	}

	res := make([]*MetricKind, 0)

	for i := range rows {
		res = append(res, &MetricKind{
			Name:        rows[i].Name,
			Aggregation: Aggregation(rows[i].Aggregation),
		})
	}

	return res, nil
}

// RegisterMetric implements MetricStore.
func (s *PGDocStore) RegisterOrReplaceMetric(ctx context.Context, metric Metric) error {
	return s.withTX(ctx, "register metric", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.RegisterOrReplaceMetric(ctx, postgres.RegisterOrReplaceMetricParams{
			UUID:  metric.UUID,
			Kind:  metric.Kind,
			Label: metric.Label,
			Value: metric.Value,
		})
		if pg.IsConstraintError(err, "metric_kind_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "metric kind not found")
		}
		if pg.IsConstraintError(err, "metric_label_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "metric label not found")
		}
		if pg.IsConstraintError(err, "metric_label_kind_match") {
			return DocStoreErrorf(ErrCodeNotFound, "label does not apply to kind")
		}
		if pg.IsConstraintError(err, "metric_uuid_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		}
		if err != nil {
			return fmt.Errorf("failed to save to database: %w", err)
		}

		return nil
	})
}

// RegisterMetric implements MetricStore.
func (s *PGDocStore) RegisterOrIncrementMetric(ctx context.Context, metric Metric) error {
	return s.withTX(ctx, "register metric", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.RegisterOrIncrementMetric(ctx, postgres.RegisterOrIncrementMetricParams{
			UUID:  metric.UUID,
			Kind:  metric.Kind,
			Label: metric.Label,
			Value: metric.Value,
		})
		if pg.IsConstraintError(err, "metric_kind_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "metric kind not found")
		}
		if pg.IsConstraintError(err, "metric_label_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "metric label not found")
		}
		if pg.IsConstraintError(err, "metric_label_kind_match") {
			return DocStoreErrorf(ErrCodeNotFound, "label does not apply to kind")
		}
		if pg.IsConstraintError(err, "metric_uuid_fkey") {
			return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		}
		if err != nil {
			return fmt.Errorf("failed to save to database: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) updateACL(
	ctx context.Context, q *postgres.Queries,
	docUUID uuid.UUID, docType string, updateACL []ACLEntry,
) error {
	if len(updateACL) == 0 {
		return nil
	}

	auth, ok := elephantine.GetAuthInfo(ctx)
	if !ok {
		return errors.New("unauthenticated context")
	}

	// Batch ACL updates, ACLs with empty permissions are dropped
	// immediately.
	var acls []postgres.ACLUpdateParams

	for _, acl := range updateACL {
		if len(acl.Permissions) == 0 {
			err := q.DropACL(ctx, postgres.DropACLParams{
				UUID: docUUID,
				URI:  acl.URI,
			})
			if err != nil {
				return fmt.Errorf(
					"failed to drop entry for %q: %w",
					acl.URI, err)
			}

			continue
		}

		acls = append(acls, postgres.ACLUpdateParams{
			UUID:        docUUID,
			URI:         acl.URI,
			Permissions: acl.Permissions,
		})
	}

	if len(acls) > 0 {
		var errs []error

		q.ACLUpdate(ctx, acls).Exec(func(_ int, err error) {
			if err != nil {
				errs = append(errs, err)
			}
		})

		if len(errs) > 0 {
			return fmt.Errorf("failed to update entries: %w",
				errors.Join(errs...))
		}
	}

	err := q.InsertACLAuditEntry(ctx, postgres.InsertACLAuditEntryParams{
		UUID:       docUUID,
		Type:       pg.TextOrNull(docType),
		Updated:    pg.Time(time.Now()),
		UpdaterUri: auth.Claims.Subject,
	})
	if err != nil {
		return fmt.Errorf("failed to record audit trail: %w", err)
	}

	return nil
}

type updatePrefligthInfo struct {
	Info   postgres.GetDocumentForUpdateRow
	Exists bool
}

func (s *PGDocStore) updatePreflight(
	ctx context.Context, q *postgres.Queries,
	uuid uuid.UUID, ifMatch int64,
) (*updatePrefligthInfo, error) {
	info, err := q.GetDocumentForUpdate(ctx, uuid)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf(
			"failed to get document information: %w", err)
	}

	exists := !errors.Is(err, pgx.ErrNoRows)
	currentVersion := info.CurrentVersion

	if info.Deleting {
		return nil, DocStoreErrorf(ErrCodeDeleteLock,
			"the document is being deleted")
	}

	switch ifMatch {
	case 0:
	case -1:
		if exists {
			return nil, DocStoreErrorf(ErrCodeOptimisticLock,
				"document already exists")
		}
	default:
		if currentVersion != ifMatch {
			return nil, DocStoreErrorf(ErrCodeOptimisticLock,
				"document version is %d, not %d as expected",
				info.CurrentVersion, ifMatch,
			)
		}
	}

	return &updatePrefligthInfo{
		Info:   info,
		Exists: exists,
	}, nil
}

// Interface guard.
var _ DocStore = &PGDocStore{}
