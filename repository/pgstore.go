package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-repository/internal"
	"github.com/ttab/elephant-repository/planning"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
)

const (
	elephantCRC     = 3997770000
	LockSigningKeys = elephantCRC + 1
)

type PGDocStoreOptions struct {
	MetricsCalculators []MetricCalculator
	DeleteTimeout      time.Duration
}

// Interface guard.
var (
	_ DocStore    = &PGDocStore{}
	_ MetricStore = &PGDocStore{}
)

type PGDocStore struct {
	logger *slog.Logger
	pool   *pgxpool.Pool
	reader *postgres.Queries
	assets *AssetBucket
	opts   PGDocStoreOptions
	metr   *IntrinsicMetrics

	archived     *pg.FanOut[ArchivedEvent]
	schemas      *pg.FanOut[SchemaEvent]
	deprecations *pg.FanOut[DeprecationEvent]
	workflows    *pg.FanOut[WorkflowEvent]
	eventOutbox  *pg.FanOut[int64]
	eventlog     *pg.FanOut[int64]
}

func NewPGDocStore(
	ctx context.Context,
	logger *slog.Logger, pool *pgxpool.Pool,
	assets *AssetBucket,
	options PGDocStoreOptions,
) (*PGDocStore, error) {
	if options.DeleteTimeout == 0 {
		options.DeleteTimeout = 5 * time.Second
	}

	s := &PGDocStore{
		logger:       logger,
		pool:         pool,
		reader:       postgres.New(pool),
		assets:       assets,
		opts:         options,
		metr:         NewIntrinsicMetrics(logger, options.MetricsCalculators),
		archived:     pg.NewFanOut[ArchivedEvent](NotifyArchived),
		schemas:      pg.NewFanOut[SchemaEvent](NotifySchemasUpdated),
		deprecations: pg.NewFanOut[DeprecationEvent](NotifyDeprecationsUpdated),
		workflows:    pg.NewFanOut[WorkflowEvent](NotifyWorkflowsUpdated),
		eventOutbox:  pg.NewFanOut[int64](NotifyEventOutbox),
		eventlog:     pg.NewFanOut[int64](NotifyEventlog),
	}

	err := s.metr.Setup(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("set up intrinsic metrics: %w", err)
	}

	return s, nil
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

// OnSchemaUpdate notifies the channel ch of all schema updates. Subscription is
// automatically cancelled once the context is cancelled.
//
// Note that we don't provide any delivery guarantees for these events.
// non-blocking send is used on ch, so if it's unbuffered events will be
// discarded if the receiver is busy.
func (s *PGDocStore) OnDeprecationUpdate(
	ctx context.Context, ch chan DeprecationEvent,
) {
	go s.deprecations.Listen(ctx, ch, func(_ DeprecationEvent) bool {
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

// OnEventOutbox notifies the channel ch of all new outbox events. Subscription
// is automatically cancelled once the context is cancelled.
//
// Note that we don't provide any delivery guarantees for these events.
// non-blocking send is used on ch, so if it's unbuffered events will be
// discarded if the receiver is busy.
func (s *PGDocStore) OnEventOutbox(
	ctx context.Context, ch chan int64,
) {
	go s.eventOutbox.Listen(ctx, ch, func(_ int64) bool {
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
	fanOuts := []pg.ChannelSubscription{
		s.archived,
		s.schemas,
		s.deprecations,
		s.workflows,
		s.eventOutbox,
		s.eventlog,
	}

	pg.Subscribe(ctx, s.logger, s.pool, fanOuts...)
}

// Delete implements DocStore.
func (s *PGDocStore) Delete(
	ctx context.Context, req DeleteRequest,
) (outErr error) {
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
	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	mainInfo, err := s.UpdatePreflight(ctx, q, req.UUID, req.IfMatch, nil)
	if err != nil {
		return err
	}

	if !mainInfo.Exists {
		return nil
	}

	lock := checkLock(mainInfo.Lock, req.LockToken)
	if lock == lockCheckDenied {
		return DocStoreErrorf(ErrCodeDocumentLock, "document locked")
	}

	var (
		metaUUID uuid.UUID
		metaInfo *UpdatePrefligthInfo
	)

	deleteDocs := []uuid.UUID{req.UUID}

	if mainInfo.MainDoc == nil {
		metaUUID, _ = metaIdentity(req.UUID)

		// Make a preflight request for the meta document.
		mInfo, err := s.UpdatePreflight(ctx, q, metaUUID, 0, mainInfo.MainDoc)
		if err != nil {
			return fmt.Errorf("meta document: %w", err)
		}

		if mInfo.Exists {
			metaInfo = mInfo

			deleteDocs = append(deleteDocs, metaUUID)
		}
	}

	timeout := time.After(s.opts.DeleteTimeout)

	archived := make(chan ArchivedEvent)

	go s.archived.Listen(ctx, archived, func(e ArchivedEvent) bool {
		return slices.Contains(deleteDocs, e.UUID)
	})

	for {
		var remaining int64

		for _, id := range deleteDocs {
			n, err := q.GetDocumentUnarchivedCount(ctx, id)
			if err != nil {
				return fmt.Errorf(
					"failed to check archiving status for %s: %w",
					id, err)
			}

			remaining += n
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

	var metaDocRecord int64

	if metaInfo != nil {
		mdr, err := s.insertDeleteRecord(
			ctx, tx, q, req.Updated, req.Updater,
			metaUUID, metaInfo,
			metaJSON, 0, nil, nil,
		)
		if err != nil {
			return fmt.Errorf(
				"create meta doc delete record: %w", err)
		}

		metaDocRecord = mdr
	}

	acl, err := s.GetDocumentACL(ctx, req.UUID)
	if err != nil {
		return fmt.Errorf("get ACLs for archiving: %w", err)
	}

	attachments, err := q.GetDocumentAttachmentDetails(ctx, req.UUID)
	if err != nil {
		return fmt.Errorf("get attachment info for archiving: %w", err)
	}

	_, err = s.insertDeleteRecord(
		ctx, tx, q, req.Updated, req.Updater,
		req.UUID, mainInfo,
		metaJSON, metaDocRecord, acl, attachments,
	)
	if err != nil {
		return fmt.Errorf(
			"create delete record: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit delete: %w", err)
	}

	return nil
}

func (s *PGDocStore) insertDeleteRecord(
	ctx context.Context, tx pgx.Tx, q *postgres.Queries,
	updated time.Time, updater string,
	id uuid.UUID,
	pf *UpdatePrefligthInfo,
	metaJSON []byte,
	metaDocRecord int64,
	acls []ACLEntry,
	attachments []postgres.AttachedObject,
) (int64, error) {
	heads, err := q.GetDocumentHeads(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("get document heads: %w", err)
	}

	statusHeads := make(map[string]int64, len(heads))

	for _, h := range heads {
		statusHeads[h.Name] = h.CurrentID
	}

	aclEntries := make([]postgres.ACLEntry, len(acls))

	for i := range acls {
		aclEntries[i] = postgres.ACLEntry{
			URI:         acls[i].URI,
			Permissions: acls[i].Permissions,
		}
	}

	recordID, err := q.InsertDeleteRecord(ctx,
		postgres.InsertDeleteRecordParams{
			UUID:          id,
			URI:           pf.Info.URI,
			Type:          pf.Info.Type,
			Version:       pf.Info.CurrentVersion,
			Created:       pg.Time(updated),
			CreatorUri:    updater,
			Meta:          metaJSON,
			MainDoc:       pg.PUUID(pf.MainDoc),
			MainDocType:   pg.TextOrNull(pf.MainDocType),
			MetaDocRecord: pg.BigintOrNull(metaDocRecord),
			Language:      pg.Text(pf.Language),
			Heads:         statusHeads,
			Acl:           aclEntries,
			Attachments:   attachments,
		})
	if err != nil {
		return 0, fmt.Errorf("create delete record: %w", err)
	}

	err = addEventToOutbox(ctx, tx, postgres.OutboxEvent{
		Event:            string(TypeDeleteDocument),
		UUID:             id,
		Version:          pf.Info.CurrentVersion,
		Timestamp:        updated,
		Updater:          updater,
		Type:             pf.Info.Type,
		MainDocument:     pf.MainDoc,
		MainDocumentType: pf.MainDocType,
		Language:         pf.Language,
		DeleteRecordID:   recordID,
	})
	if err != nil {
		return 0, fmt.Errorf("add delete event to outbox: %w", err)
	}

	err = q.DeleteDocumentEntry(ctx, id)
	if err != nil {
		return 0, fmt.Errorf(
			"delete current document entry: %w", err)
	}

	err = q.InsertDeletionPlaceholder(ctx,
		postgres.InsertDeletionPlaceholderParams{
			UUID:     id,
			URI:      pf.Info.URI,
			RecordID: recordID,
		})
	if err != nil {
		return 0, fmt.Errorf(
			"insert deletion placeholder: %w", err)
	}

	return recordID, nil
}

func (s *PGDocStore) RestoreDocument(
	ctx context.Context, docUUID uuid.UUID, deleteRecordID int64,
	creator string, acl []ACLEntry,
) (outErr error) {
	specData, err := json.Marshal(RestoreSpec{
		ACL: acl,
	})
	if err != nil {
		return fmt.Errorf("marshal restore spec: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	record, err := q.GetDeleteRecordForUpdate(ctx,
		postgres.GetDeleteRecordForUpdateParams{
			ID:   deleteRecordID,
			UUID: docUUID,
		})
	if errors.Is(err, pgx.ErrNoRows) {
		return DocStoreErrorf(ErrCodeNotFound,
			"delete record doesn't exist")
	} else if err != nil {
		return fmt.Errorf("read delete record: %w", err)
	}

	if record.Purged.Valid {
		return DocStoreErrorf(ErrCodeBadRequest,
			"delete record has been purged")
	}

	pendingPurge, err := q.CheckForPendingPurge(ctx, deleteRecordID)
	if err != nil {
		return fmt.Errorf("check for pending purges: %w", err)
	}

	if pendingPurge {
		return DocStoreErrorf(ErrCodeBadRequest,
			"delete record has been queued for purging")
	}

	state, err := q.ReadForRestore(ctx, docUUID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check document status: %w", err)
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		if !state.Valid {
			return DocStoreErrorf(ErrCodeExists,
				"document already exists")
		}

		return DocStoreErrorf(ErrCodeFailedPrecondition,
			"document is currently locked for %q", state.String)
	}

	// Insert a placeholder document row vith the state restoring and
	// version 0.
	err = q.InsertDocument(ctx, postgres.InsertDocumentParams{
		UUID:        docUUID,
		URI:         record.URI,
		Type:        record.Type,
		Created:     pg.Time(time.Now()),
		CreatorUri:  creator,
		Version:     0,
		Language:    record.Language,
		SystemState: pg.Text(SystemStateRestoring),
	})
	if pg.IsConstraintError(err, "document_pkey") {
		return DocStoreErrorf(ErrCodeFailedPrecondition,
			"document already exists")
	} else if err != nil {
		return fmt.Errorf("insert restore placeholder: %w", err)
	}

	err = q.InsertRestoreRequest(ctx,
		postgres.InsertRestoreRequestParams{
			UUID:           docUUID,
			DeleteRecordID: deleteRecordID,
			Created:        pg.Time(time.Now()),
			Creator:        creator,
			Spec:           specData,
		})
	if err != nil {
		return fmt.Errorf("insert restore request: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit restore: %w", err)
	}

	return nil
}

func (s *PGDocStore) PurgeDocument(
	ctx context.Context, docUUID uuid.UUID, deleteRecordID int64,
	creator string,
) (outErr error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	record, err := q.GetDeleteRecordForUpdate(ctx,
		postgres.GetDeleteRecordForUpdateParams{
			ID:   deleteRecordID,
			UUID: docUUID,
		})
	if errors.Is(err, pgx.ErrNoRows) {
		return DocStoreErrorf(ErrCodeNotFound,
			"delete record doesn't exist")
	} else if err != nil {
		return fmt.Errorf("read delete record: %w", err)
	}

	if record.Purged.Valid {
		// Treat this as ok, the end goal has been achieved after all.
		return nil
	}

	pendingPurge, err := q.CheckForPendingPurge(ctx, deleteRecordID)
	if err != nil {
		return fmt.Errorf("check for pending purges: %w", err)
	}

	if pendingPurge {
		// Likewise ok.
		return nil
	}

	err = q.InsertPurgeRequest(ctx, postgres.InsertPurgeRequestParams{
		UUID:           docUUID,
		DeleteRecordID: deleteRecordID,
		Created:        pg.Time(time.Now()),
		Creator:        creator,
	})
	if err != nil {
		return fmt.Errorf("insert purge request: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit restore: %w", err)
	}

	return nil
}

func (s *PGDocStore) ListDeleteRecords(
	ctx context.Context, docUUID *uuid.UUID,
	beforeID int64, beforeTime *time.Time,
) ([]DeleteRecord, error) {
	// Before time will be ignored when beforeID pagination is used.
	if beforeID != 0 {
		beforeTime = nil
	}

	rows, err := s.reader.ListDeleteRecords(ctx,
		postgres.ListDeleteRecordsParams{
			UUID:       pg.PUUID(docUUID),
			BeforeID:   beforeID,
			BeforeTime: pg.PTime(beforeTime),
		})
	if err != nil {
		return nil, fmt.Errorf("read rows from database: %w", err)
	}

	res := make([]DeleteRecord, len(rows))

	for i, row := range rows {
		meta := make(newsdoc.DataMap)

		if row.Meta != nil {
			err := json.Unmarshal(row.Meta, &meta)
			if err != nil {
				return nil, fmt.Errorf(
					"unmarshal meta of delete record %d: %w",
					row.ID, err)
			}
		}

		var (
			mainDoc   *uuid.UUID
			finalised *time.Time
			purged    *time.Time
		)

		if row.MainDoc.Valid {
			var id uuid.UUID = row.MainDoc.Bytes
			mainDoc = &id
		}

		if row.Finalised.Valid {
			finalised = &row.Finalised.Time
		}

		if row.Purged.Valid {
			purged = &row.Purged.Time
		}

		res[i] = DeleteRecord{
			ID:           row.ID,
			UUID:         row.UUID,
			URI:          row.URI,
			Type:         row.Type,
			Language:     row.Language.String,
			Version:      row.Version,
			Created:      row.Created.Time,
			Creator:      row.CreatorUri,
			Meta:         meta,
			MainDocument: mainDoc,
			Finalised:    finalised,
			Purged:       purged,
		}
	}

	return res, nil
}

// GetDocument implements DocStore.
func (s *PGDocStore) GetDocument(
	ctx context.Context, uuid uuid.UUID, version int64,
) (*newsdoc.Document, int64, error) {
	var (
		err  error
		data []byte
	)

	if version == 0 {
		res, e := s.reader.GetDocumentData(ctx, uuid)

		if e == nil {
			data = res.DocumentData
			version = res.Version
		}

		err = e
	} else {
		data, err = s.reader.GetDocumentVersionData(ctx,
			postgres.GetDocumentVersionDataParams{
				UUID:    uuid,
				Version: version,
			})
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch document data: %w", err)
	}

	// TODO: check for nil data after pruning has been implemented.

	var d newsdoc.Document

	err = json.Unmarshal(data, &d)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"got an unreadable document from the database: %w", err)
	}

	return &d, version, nil
}

type BulkGetReference struct {
	UUID    uuid.UUID
	Version int64
}

type BulkGetItem struct {
	Document newsdoc.Document
	Version  int64
}

// BulkGetDocuments implements DocStore.
func (s *PGDocStore) BulkGetDocuments(
	ctx context.Context, documents []BulkGetReference,
) ([]BulkGetItem, error) {
	ids := make([]uuid.UUID, len(documents))
	versions := make([]int64, len(documents))

	for i, ref := range documents {
		ids[i] = ref.UUID
		versions[i] = ref.Version
	}

	docs, err := s.reader.BulkGetDocumentData(ctx,
		postgres.BulkGetDocumentDataParams{
			Uuids:    ids,
			Versions: versions,
		})
	if err != nil {
		return nil, fmt.Errorf("load documents from database: %w", err)
	}

	result := make([]BulkGetItem, 0, len(docs))

	for _, row := range docs {
		var d newsdoc.Document

		// TODO: check for nil data after pruning has been implemented.

		err = json.Unmarshal(row.DocumentData, &d)
		if err != nil {
			return nil, fmt.Errorf(
				"got an unreadable document from the database for %q v%d: %w",
				row.UUID, row.Version, err)
		}

		result = append(result, BulkGetItem{
			Document: d,
			Version:  row.Version,
		})
	}

	return result, nil
}

func (s *PGDocStore) GetLastEvent(
	ctx context.Context,
) (*Event, error) {
	res, err := s.reader.GetLastEvent(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}

	return &Event{
		ID:               res.ID,
		Event:            EventType(res.Event),
		UUID:             res.UUID,
		Timestamp:        res.Timestamp.Time,
		Updater:          res.Updater.String,
		Type:             res.Type.String,
		Version:          res.Version.Int64,
		Status:           res.Status.String,
		StatusID:         res.StatusID.Int64,
		MainDocument:     pg.ToUUIDPointer(res.MainDoc),
		MainDocumentType: res.MainDocType.String,
		Language:         res.Language.String,
		OldLanguage:      res.OldLanguage.String,
	}, nil
}

// GetLastEventID implements DocStore.
func (s *PGDocStore) GetLastEventID(ctx context.Context) (int64, error) {
	id, err := s.reader.GetLastEventID(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return 0, fmt.Errorf("database query failed: %w", err)
	}

	return id, nil
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

	//nolint: dupl
	for i := range res {
		e := Event{
			ID:                 res[i].ID,
			Event:              EventType(res[i].Event),
			UUID:               res[i].UUID,
			Timestamp:          res[i].Timestamp.Time,
			Updater:            res[i].Updater.String,
			Type:               res[i].Type.String,
			Version:            res[i].Version.Int64,
			Status:             res[i].Status.String,
			StatusID:           res[i].StatusID.Int64,
			MainDocument:       pg.ToUUIDPointer(res[i].MainDoc),
			MainDocumentType:   res[i].MainDocType.String,
			Language:           res[i].Language.String,
			OldLanguage:        res[i].OldLanguage.String,
			SystemState:        res[i].SystemState.String,
			WorkflowStep:       res[i].WorkflowState.String,
			WorkflowCheckpoint: res[i].WorkflowCheckpoint.String,
		}

		extra := res[i].Extra
		if extra != nil {
			e.AttachedObjects = extra.AttachedObjects
			e.DetachedObjects = extra.DetachedObjects
			e.DeleteRecordID = extra.DeleteRecordID
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

func (s *PGDocStore) GetCompactedEventlog(
	ctx context.Context, req GetCompactedEventlogRequest,
) ([]Event, error) {
	params := postgres.GetCompactedEventlogParams{
		After:     req.After,
		Until:     req.Until,
		Type:      pg.TextOrNull(req.Type),
		RowOffset: req.Offset,
	}

	if req.Limit != nil {
		params.RowLimit = pgtype.Int4{
			Int32: *req.Limit,
			Valid: true,
		}
	}

	res, err := s.reader.GetCompactedEventlog(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to read from database: %w", err)
	}

	evts := make([]Event, len(res))

	//nolint: dupl
	for i := range res {
		e := Event{
			ID:                 res[i].ID,
			Event:              EventType(res[i].Event),
			UUID:               res[i].UUID,
			Timestamp:          res[i].Timestamp.Time,
			Updater:            res[i].Updater.String,
			Type:               res[i].Type.String,
			Version:            res[i].Version.Int64,
			Status:             res[i].Status.String,
			StatusID:           res[i].StatusID.Int64,
			MainDocument:       pg.ToUUIDPointer(res[i].MainDoc),
			MainDocumentType:   res[i].MainDocType.String,
			Language:           res[i].Language.String,
			OldLanguage:        res[i].OldLanguage.String,
			SystemState:        res[i].SystemState.String,
			WorkflowStep:       res[i].WorkflowState.String,
			WorkflowCheckpoint: res[i].WorkflowCheckpoint.String,
		}

		extra := res[i].Extra
		if extra != nil {
			e.AttachedObjects = extra.AttachedObjects
			e.DetachedObjects = extra.DetachedObjects
			e.DeleteRecordID = extra.DeleteRecordID
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
	before int64, count int64, loadStatuses bool,
) ([]DocumentHistoryItem, error) {
	if count > VersionHistoryMaxCount {
		return nil, fmt.Errorf("count cannot be greater than %d",
			VersionHistoryMaxCount)
	}

	history, err := s.reader.GetVersions(ctx, postgres.GetVersionsParams{
		UUID:   uuid,
		Before: before,
		Count:  count,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch version history: %w", err)
	}

	versions := make([]int64, len(history))
	updates := make([]DocumentHistoryItem, len(history))

	for i, v := range history {
		up := DocumentHistoryItem{
			Version:  v.Version,
			Creator:  v.CreatorUri,
			Created:  v.Created.Time,
			Statuses: make(map[string][]Status),
		}

		if v.Meta != nil {
			err := json.Unmarshal(v.Meta, &up.Meta)
			if err != nil {
				return nil, fmt.Errorf(
					"unmarshal metadata for version %d: %w",
					v.Version, err)
			}
		}

		updates[i] = up
		versions[i] = v.Version
	}

	// Bail early if we aren't loading statuses or no versions were found.
	if !loadStatuses || len(versions) == 0 {
		return updates, nil
	}

	statuses, err := s.reader.GetStatusesForVersions(ctx,
		postgres.GetStatusesForVersionsParams{
			UUID:     uuid,
			Versions: versions,
		})
	if err != nil {
		return nil, fmt.Errorf("load statuses: %w", err)
	}

	var versionIdx int

	for _, status := range statuses {
		// Find the version that the status applies to.
		for versions[versionIdx] != status.Version {
			versionIdx++

			if versionIdx > len(versions) {
				versionIdx = -1

				break
			}
		}

		if versionIdx == -1 {
			break
		}

		updates[versionIdx].Statuses[status.Name] = append(
			updates[versionIdx].Statuses[status.Name],
			Status{
				ID:             status.ID,
				Version:        status.Version,
				Creator:        status.CreatorUri,
				Created:        status.Created.Time,
				Meta:           status.Meta,
				MetaDocVersion: status.MetaDocVersion.Int64,
			},
		)
	}

	return updates, nil
}

func (s *PGDocStore) GetNilStatuses(
	ctx context.Context, uuid uuid.UUID,
	names []string,
) (map[string][]Status, error) {
	if len(names) == 0 {
		names = nil
	}

	res, err := s.reader.GetNilStatuses(ctx, postgres.GetNilStatusesParams{
		UUID:  uuid,
		Names: names,
	})
	if err != nil {
		return nil, fmt.Errorf("read from database: %w", err)
	}

	m := make(map[string][]Status, len(res))

	for _, row := range res {
		m[row.Name] = append(m[row.Name], Status{
			ID:             row.ID,
			Version:        row.Version,
			Creator:        row.CreatorUri,
			Created:        row.Created.Time,
			Meta:           row.Meta,
			MetaDocVersion: row.MetaDocVersion.Int64,
		})
	}

	return m, nil
}

func (s *PGDocStore) GetStatus(
	ctx context.Context, uuid uuid.UUID,
	name string, id int64,
) (Status, error) {
	row, err := s.reader.GetStatus(ctx, postgres.GetStatusParams{
		UUID: uuid,
		Name: name,
		ID:   id,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Status{}, DocStoreErrorf(ErrCodeNotFound, "no status found")
	} else if err != nil {
		return Status{}, fmt.Errorf("database error: %w", err)
	}

	return Status{
		ID:             row.ID,
		Version:        row.Version,
		Creator:        row.CreatorUri,
		Created:        row.Created.Time,
		Meta:           row.Meta,
		MetaDocVersion: row.MetaDocVersion.Int64,
	}, nil
}

func (s *PGDocStore) GetStatusHistory(
	ctx context.Context, uuid uuid.UUID,
	name string, before int64, count int,
) ([]Status, error) {
	if count > StatusHistoryMaxCount {
		return nil, fmt.Errorf("count cannot be greater than %d",
			StatusHistoryMaxCount)
	}

	history, err := s.reader.GetStatusVersions(ctx, postgres.GetStatusVersionsParams{
		UUID:   uuid,
		Name:   name,
		Before: before,
		Count:  internal.MustInt32(count),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch status history: %w", err)
	}

	statuses := make([]Status, len(history))

	for i := range history {
		s := Status{
			ID:             history[i].ID,
			Version:        history[i].Version,
			Creator:        history[i].CreatorUri,
			Created:        history[i].Created.Time,
			MetaDocVersion: history[i].MetaDocVersion.Int64,
			Meta:           history[i].Meta,
		}

		statuses[i] = s
	}

	return statuses, nil
}

// BulkCheckPermissions implements DocStore.
func (s *PGDocStore) BulkCheckPermissions(
	ctx context.Context, req BulkCheckPermissionRequest,
) ([]uuid.UUID, error) {
	perms := make([]string, len(req.Permissions))

	for i := range req.Permissions {
		perms[i] = string(req.Permissions[i])
	}

	uuids, err := s.reader.BulkCheckPermissions(ctx, postgres.BulkCheckPermissionsParams{
		URI:         req.GranteeURIs,
		Permissions: perms,
		Uuids:       req.UUIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("check acls: %w", err)
	}

	return uuids, nil
}

// GetStatusOverview implements DocStore.
func (s *PGDocStore) GetStatusOverview(
	ctx context.Context, uuids []uuid.UUID, statuses []string,
	getMeta bool,
) ([]StatusOverviewItem, error) {
	versions, err := s.reader.GetCurrentDocumentVersions(ctx, uuids)
	if err != nil {
		return nil, fmt.Errorf("get current versions: %w", err)
	}

	collected := make(map[uuid.UUID]*StatusOverviewItem, len(versions))

	for _, v := range versions {
		collected[v.UUID] = &StatusOverviewItem{
			UUID:               v.UUID,
			CurrentVersion:     v.CurrentVersion,
			Updated:            v.Updated.Time,
			WorkflowStep:       v.WorkflowStep.String,
			WorkflowCheckpoint: v.WorkflowCheckpoint.String,
		}
	}

	if len(statuses) > 0 {
		heads, err := s.reader.GetMultipleStatusHeads(ctx,
			postgres.GetMultipleStatusHeadsParams{
				Uuids:    uuids,
				Statuses: statuses,
				GetMeta:  getMeta,
			})
		if err != nil {
			return nil, fmt.Errorf("get document heads: %w", err)
		}

		for _, h := range heads {
			doc := collected[h.UUID]
			if doc.Heads == nil {
				doc.Heads = make(map[string]Status)
			}

			var meta newsdoc.DataMap

			if len(h.Meta) != 0 {
				err := json.Unmarshal(h.Meta, &meta)
				if err != nil {
					return nil, fmt.Errorf(
						"unmarshal metadata for %s status %q: %w",
						h.UUID, h.Name, err)
				}
			}

			doc.Heads[h.Name] = Status{
				ID:             h.CurrentID,
				Version:        h.Version,
				Creator:        h.UpdaterUri,
				Created:        h.Updated.Time,
				Meta:           meta,
				MetaDocVersion: h.MetaDocVersion.Int64,
			}
		}
	}

	var res []StatusOverviewItem

	for _, id := range uuids {
		item, ok := collected[id]
		if !ok {
			continue
		}

		res = append(res, *item)
	}

	return res, nil
}

// GetDocumentMeta implements DocStore.
func (s *PGDocStore) GetDocumentMeta(
	ctx context.Context, docID uuid.UUID,
) (*DocumentMeta, error) {
	info, err := s.reader.GetDocumentInfo(ctx, postgres.GetDocumentInfoParams{
		UUID: docID,
		Now:  pg.Time(time.Now()),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch document info: %w", err)
	}

	if info.SystemState.Valid {
		state := SystemState(info.SystemState.String)

		switch state {
		case SystemStateDeleting, SystemStateRestoring:
			return &DocumentMeta{
				SystemLock: state,
			}, nil
		default:
			return nil, fmt.Errorf(
				"unknown system state for document: %q",
				state,
			)
		}
	}

	var mainDoc string

	if info.MainDoc.Valid {
		mainDoc = uuid.UUID(info.MainDoc.Bytes).String()
	}

	meta := DocumentMeta{
		Created:            info.Created.Time,
		CreatorURI:         info.CreatorUri,
		Modified:           info.Updated.Time,
		UpdaterURI:         info.UpdaterUri,
		CurrentVersion:     info.CurrentVersion,
		Statuses:           make(map[string]StatusHead),
		MainDocument:       mainDoc,
		WorkflowState:      info.WorkflowState.String,
		WorkflowCheckpoint: info.WorkflowCheckpoint.String,
		Lock: Lock{
			Token:   info.LockToken.String,
			URI:     info.LockUri.String,
			Created: info.LockCreated.Time,
			Expires: info.LockExpires.Time,
			App:     info.LockApp.String,
			Comment: info.LockComment.String,
		},
	}

	heads, err := s.getFullDocumentHeads(ctx, s.reader, docID)
	if err != nil {
		return nil, err
	}

	meta.Statuses = heads

	attached, err := s.reader.GetAttachments(ctx, docID)
	if err != nil {
		return nil, fmt.Errorf("get attachment information: %w", err)
	}

	meta.Attachments = make([]AttachmentRef, len(attached))

	for i := range attached {
		meta.Attachments[i] = AttachmentRef{
			Name:    attached[i].Name,
			Version: attached[i].Version,
		}
	}

	acl, err := s.GetDocumentACL(ctx, docID)
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
) (map[string]StatusHead, error) {
	statuses := make(map[string]StatusHead)

	heads, err := q.GetFullDocumentHeads(ctx, docUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document heads: %w", err)
	}

	for _, head := range heads {
		status := StatusHead{
			ID:             head.ID,
			Version:        head.Version,
			Creator:        head.CreatorUri,
			Created:        head.Created.Time,
			MetaDocVersion: head.MetaDocVersion.Int64,
			Meta:           head.Meta,
			Language:       head.Language.String,
		}

		statuses[head.Name] = status
	}

	return statuses, nil
}

// CheckPermission implements DocStore.
func (s *PGDocStore) CheckPermissions(
	ctx context.Context, req CheckPermissionRequest,
) (CheckPermissionResult, error) {
	ps := make([]string, len(req.Permissions))

	for i := range req.Permissions {
		ps[i] = string(req.Permissions[i])
	}

	access, err := s.reader.CheckPermissions(ctx,
		postgres.CheckPermissionsParams{
			UUID:        req.UUID,
			URI:         req.GranteeURIs,
			Permissions: ps,
		})
	if errors.Is(err, pgx.ErrNoRows) {
		return PermissionCheckNoSuchDocument, nil
	} else if err != nil {
		return PermissionCheckDenied, fmt.Errorf(
			"failed check acl: %w", err)
	}

	switch {
	case access.SystemState.String != "":
		return PermissionCheckSystemLock, nil
	case !access.HasAccess:
		return PermissionCheckDenied, nil
	}

	return PermissionCheckAllowed, nil
}

func checkLock(
	lock Lock,
	token string,
) checkLockResult {
	// We only need to validate the token; the expiration has already been
	// handled in the SQL layer.
	if token == lock.Token {
		return lockCheckAllowed
	}

	return lockCheckDenied
}

type checkLockResult int

const (
	lockCheckAllowed = iota
	lockCheckDenied
)

// Update implements DocStore.
func (s *PGDocStore) Update(
	ctx context.Context, workflows WorkflowProvider,
	requests []*UpdateRequest,
) (_ []DocumentUpdate, outErr error) {
	var updates []*docUpdateState

	// Check if referenced uploads exist before we start any actual work.
	for _, req := range requests {
		for name, upload := range req.AttachObjects {
			uploadInfo, err := s.reader.GetUpload(ctx, upload.ID)
			if err != nil {
				return nil, fmt.Errorf(
					"unknown upload %s: %w",
					upload.ID, err,
				)
			}

			// Transfer the metadata from the upload row.
			upload.Meta = AssetMetadata{
				Filename: uploadInfo.Meta.Filename,
				Mimetype: uploadInfo.Meta.Mimetype,
				Props:    uploadInfo.Meta.Props,
			}

			req.AttachObjects[name] = upload

			exists, err := s.assets.UploadExists(ctx, upload.ID)
			if err != nil {
				return nil, fmt.Errorf(
					"check if %q (%s) has been uploaded: %w",
					name, upload, err,
				)
			}

			if !exists {
				return nil, elephantine.InvalidArgumentf(
					"attach_objects",
					"no object uploaded for %q (%s)",
					name, upload,
				)
			}
		}
	}

	// Do serialisation work before we start a transaction. That way we
	// don't keep row locks or hog connections while doing that
	// busy-work. Likewise we don't even try to statedate the db in the
	// unlikely event that marshalling fails.
	for _, req := range requests {
		state, err := newUpdateState(req)
		if err != nil {
			return nil, err
		}

		updates = append(updates, state)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	for _, state := range updates {
		info, err := s.UpdatePreflight(ctx, q,
			state.Request.UUID, state.Request.IfMatch, state.MainDocID)
		if err != nil {
			return nil, err
		}

		state.Version = info.Info.CurrentVersion
		state.Exists = info.Exists
		state.Language = info.Language
		state.IsMetaDoc = info.MainDoc != nil
		state.MainDocType = info.MainDocType
		state.SystemState = info.Info.SystemState.String

		if state.Exists {
			state.Type = info.Info.Type
		}

		if state.Request.IfWorkflowState != "" {
			wf, err := q.GetWorkflowState(ctx, state.UUID)
			if err != nil {
				return nil, fmt.Errorf("get workflow state: %w", err)
			}

			if wf.Step != state.Request.IfWorkflowState {
				return nil, DocStoreErrorf(ErrCodeFailedPrecondition,
					"not in the correct workflow state")
			}
		}

		if state.IsMetaDoc && len(state.Request.AttachObjects) > 0 {
			return nil, DocStoreErrorf(
				ErrCodeBadRequest,
				"objects cannot be attached to meta documents")
		}

		//nolint:nestif
		if state.Exists && state.Doc != nil {
			if isMetaURI(state.Doc.URI) && info.MainDoc == nil {
				return nil, DocStoreErrorf(ErrCodeBadRequest,
					"cannot change a normal document into a meta document")
			}

			if info.MainDoc != nil {
				expectUUID, expectURI := metaIdentity(*info.MainDoc)

				if state.UUID != expectUUID {
					return nil, DocStoreErrorf(ErrCodeBadRequest,
						"expected meta document to have the UUID %s based on the main document UUID %s",
						expectUUID, *info.MainDoc,
					)
				}

				if state.Doc.URI != expectURI {
					return nil, DocStoreErrorf(ErrCodeBadRequest,
						"expected meta document to have the URI %s based on the main document UUID %s",
						expectURI, *info.MainDoc,
					)
				}
			}

			if state.Doc.Type != state.Type {
				return nil, DocStoreErrorf(ErrCodeBadRequest,
					"cannot change the document type from %q",
					state.Type)
			}
		}

		lock := checkLock(info.Lock, state.Request.LockToken)
		if lock == lockCheckDenied {
			return nil, DocStoreErrorf(ErrCodeDocumentLock, "document locked")
		}
	}

	var evts []postgres.OutboxEvent

	for _, state := range updates {
		var (
			initialCreate bool
			startWState   WorkflowState
			wState        WorkflowState
		)

		workflow, hasWorkflow := workflows.GetDocumentWorkflow(state.Type)

		if hasWorkflow {
			s, err := loadWorkflowState(ctx, q, workflow, state.UUID)
			if err != nil {
				return nil, fmt.Errorf("load workflow state: %w", err)
			}

			wState = s
			startWState = s
		}

		if state.Doc != nil {
			if state.Exists && state.Language != state.Doc.Language {
				state.OldLanguage = state.Language
			}

			state.Language = state.Doc.Language

			version := state.Version + 1

			initialCreate = version == 1

			props := documentVersionProps{
				UUID:             state.UUID,
				Version:          version,
				Type:             state.Type,
				URI:              state.Doc.URI,
				Language:         state.Language,
				Created:          state.Created,
				Creator:          state.Creator,
				MainDocument:     state.Request.MainDocument,
				MainDocumentType: state.MainDocType,
				MetaJSON:         state.MetaJSON,
				DocJSON:          state.DocJSON,
				Document:         state.Doc,
			}

			err := createNewDocumentVersion(ctx, tx, q, props)
			if err != nil {
				return nil, err
			}

			state.Version = version

			evt := postgres.OutboxEvent{
				Event:            string(TypeDocumentVersion),
				UUID:             state.UUID,
				Timestamp:        state.Created,
				Updater:          state.Creator,
				Type:             state.Type,
				Language:         state.Language,
				OldLanguage:      state.OldLanguage,
				MainDocument:     state.Request.MainDocument,
				MainDocumentType: state.MainDocType,
				Version:          state.Version,
				SystemState:      state.SystemState,
			}

			// Add attaches and detaches to the event, we'll act on
			// these later, but they're recorded as part of the
			// document update.
			for name := range state.Request.AttachObjects {
				evt.AttachedObjects = append(evt.AttachedObjects, name)
			}

			evt.DetachedObjects = append(evt.DetachedObjects,
				state.Request.DetachObjects...)

			// Queue up the document version event for the eventlog.
			evts = append(evts, evt)

			// Progress workflow state.
			wState = workflow.Step(wState, WorkflowStep{
				Version: version,
			})
		}

		var metaDocVersion int64

		statusHeads := make(map[string]StatusHead)

		if len(state.Request.Status) > 0 || len(state.Request.IfStatusHeads) > 0 {
			heads, err := s.getFullDocumentHeads(ctx, s.reader,
				state.Request.UUID)
			if err != nil {
				return nil, err
			}

			for name, id := range state.Request.IfStatusHeads {
				current, ok := heads[name]

				switch {
				case id == -1 && !ok:
					continue
				case id == -1 && ok:
					return nil, DocStoreErrorf(
						ErrCodeFailedPrecondition,
						"status %q exists", name,
					)
				case current.ID != id:
					return nil, DocStoreErrorf(
						ErrCodeFailedPrecondition,
						"status %q didn't have the expected ID %d",
						name, id,
					)
				}
			}

			mv, err := q.GetMetaDocVersion(ctx, pg.UUID(state.UUID))
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf(
					"read meta document version: %w", err)
			}

			metaDocVersion = mv

			statusHeads = heads
		}

		var (
			lastStatusName *string
			lastStatusID   *int64
		)

		for i, stat := range state.Request.Status {
			statusID := statusHeads[stat.Name].ID + 1

			if stat.Version == 0 {
				stat.Version = state.Version
			}

			status := Status{
				ID:             statusID,
				Creator:        state.Creator,
				Version:        stat.Version,
				Meta:           stat.Meta,
				Created:        state.Created,
				MetaDocVersion: metaDocVersion,
			}

			input, err := s.buildStatusRuleInput(
				ctx, q, state.Request.UUID, stat.Name, status,
				state.DocumentUpdate, state.Doc, state.Request.Meta, statusHeads,
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

			lang := state.Language

			// If the status is referencing a specific version we'll
			// have to fetch the language of it so that the status
			// language reflects the language of the document.
			if status.Version != -1 && status.Version != state.Version {
				l, err := q.GetVersionLanguage(ctx,
					postgres.GetVersionLanguageParams{
						UUID:    state.Request.UUID,
						Version: status.Version,
					})
				if err != nil {
					return nil, fmt.Errorf(
						"read language of status %q document version: %w",
						stat.Name, err)
				}

				if l.Valid {
					lang = l.String
				}
			}

			err = q.CreateStatusHead(ctx, postgres.CreateStatusHeadParams{
				UUID:       state.Request.UUID,
				Name:       stat.Name,
				Type:       state.Type,
				Version:    status.Version,
				ID:         status.ID,
				Created:    pg.Time(state.Created),
				CreatorUri: state.Creator,
				Language:   lang,
			})
			if err != nil {
				return nil, fmt.Errorf("create status head: %w", err)
			}

			err = q.InsertDocumentStatus(ctx, postgres.InsertDocumentStatusParams{
				UUID:           state.Request.UUID,
				Name:           stat.Name,
				ID:             status.ID,
				Version:        status.Version,
				Created:        pg.Time(state.Created),
				CreatorUri:     state.Creator,
				Meta:           state.StatusMeta[i],
				MetaDocVersion: metaDocVersion,
			})
			if err != nil {
				return nil, fmt.Errorf("insert document status: %w", err)
			}

			var oldLanguage string

			if lang != statusHeads[stat.Name].Language {
				oldLanguage = statusHeads[stat.Name].Language
			}

			// Queue up the status event for the eventlog.
			evts = append(evts, postgres.OutboxEvent{
				Event:            string(TypeNewStatus),
				UUID:             state.Request.UUID,
				Status:           stat.Name,
				StatusID:         status.ID,
				Timestamp:        state.Created,
				Updater:          state.Creator,
				Type:             state.Type,
				Language:         state.Language,
				OldLanguage:      oldLanguage,
				MainDocument:     state.Request.MainDocument,
				MainDocumentType: state.MainDocType,
				Version:          status.Version,
				MetaDocVersion:   metaDocVersion,
				SystemState:      state.SystemState,
			})

			// Progress workflow state
			oldState := wState
			wState = workflow.Step(wState, WorkflowStep{
				Status: &stat,
			})

			// We want a reference to the last status that affected
			// the workflow state.
			if !wState.Equal(oldState) {
				lastStatusName = pointer(stat.Name)
				lastStatusID = pointer(status.ID)
			}
		}

		// TODO: don't update the ACL where it would be a noop.
		aclUpdate := state.Request.ACL

		if len(aclUpdate) == 0 && !state.Exists {
			aclUpdate = state.Request.DefaultACL
		}

		if len(aclUpdate) > 0 {
			err := updateACL(ctx, q, state.Request.Updater,
				state.Request.UUID, state.Type, state.Language, aclUpdate)
			if err != nil {
				return nil, fmt.Errorf("update ACL: %w", err)
			}

			entries := make([]postgres.ACLEntry, len(aclUpdate))

			for i := range aclUpdate {
				entries[i] = postgres.ACLEntry{
					URI:         aclUpdate[i].URI,
					Permissions: aclUpdate[i].Permissions,
				}
			}

			// Queue up the event for the ACL update.
			evts = append(evts, postgres.OutboxEvent{
				Event:            string(TypeACLUpdate),
				UUID:             state.Request.UUID,
				Timestamp:        state.Created,
				Updater:          state.Creator,
				Type:             state.Type,
				ACL:              entries,
				Language:         state.Language,
				MainDocument:     state.Request.MainDocument,
				MainDocumentType: state.MainDocType,
				SystemState:      state.SystemState,
			})
		}

		if !state.IsMetaDoc && hasWorkflow && (initialCreate || !wState.Equal(startWState)) {
			err := q.ChangeWorkflowState(ctx,
				postgres.ChangeWorkflowStateParams{
					UUID:            state.UUID,
					Type:            state.Type,
					Language:        state.Language,
					Updated:         pg.Time(state.Created),
					UpdaterUri:      state.Creator,
					Step:            wState.Step,
					Checkpoint:      wState.LastCheckpoint,
					StatusName:      pg.PText(lastStatusName),
					StatusID:        pg.PInt64(lastStatusID),
					DocumentVersion: state.Version,
				})
			if err != nil {
				return nil, fmt.Errorf("update workflow state: %w", err)
			}

			// Queue up the event for the workflow update.
			evts = append(evts, postgres.OutboxEvent{
				Event:              string(TypeWorkflow),
				UUID:               state.Request.UUID,
				Version:            state.Version,
				Timestamp:          state.Created,
				Updater:            state.Creator,
				Type:               state.Type,
				Language:           state.Language,
				MainDocument:       state.Request.MainDocument,
				MainDocumentType:   state.MainDocType,
				WorkflowStep:       wState.Step,
				WorkflowCheckpoint: wState.LastCheckpoint,
				// TODO: previous step?
				SystemState: state.SystemState,
			})
		}
	}

	for i := range evts {
		err := addEventToOutbox(ctx, tx, evts[i])
		if err != nil {
			return nil, fmt.Errorf("send events: %w", err)
		}
	}

	// Process attachments last, as we want to reasonably certain that the
	// transaction will be a success before we start manipulating the object
	// store.
	revertAttachments, err := s.processAttachments(ctx, q, updates)
	if err != nil {
		return nil, fmt.Errorf("process attachments: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		// Revert object store changes, any failure to revert an object
		// will be logged. The database entries will still refer to the
		// correct object versions after such a failure, but if f.ex. an
		// object was updated the most recent version of the object will
		// not be the one referenced by the DB, we then run the risk of
		// having the referenced version being deleted by bucket
		// lifecycle policies.
		//
		// TODO: We need dedicated methods that can be used for
		// recovering from such an inconsistent state. As the database
		// is the source of truth it should only require the UUID of a
		// potentially inconsistent document to be able to fix the issue
		// by manipulating the object store and attached_object table.
		revertAttachments()

		return nil, fmt.Errorf("commit: %w", err)
	}

	var res []DocumentUpdate

	for _, up := range updates {
		// Collect intrinsic metrics for document. This is outside of
		// the transaction, and errors are only logged, because it's
		// secondary to actually successfully storing documents.
		if up.Doc != nil && !up.IsMetaDoc {
			s.metr.MeasureDocument(ctx, s, up.UUID, *up.Doc)
		}

		res = append(res, up.DocumentUpdate)
	}

	return res, nil
}

// processAttachments for the updates, on success it returns a function that can
// be called to roll back the changes to the object store.
func (s *PGDocStore) processAttachments(
	ctx context.Context,
	q *postgres.Queries,
	updates []*docUpdateState,
) (_ func(), outErr error) {
	// If we fail we need to roll back the object store updates that have
	// been made. This is tricky stuff as we never can guarantee any real
	// consistency with the object store, but this will have to be on an
	// best effort basis.
	var attachRollback []attached

	rollback := func() {
		for _, r := range attachRollback {
			err := s.doAttachmentObjectRevert(ctx, r)
			if err != nil {
				// Log the failure, flag for human review using an alert code.
				s.logger.ErrorContext(ctx,
					"failed to revert attached object",
					elephantine.LogKeyError, err,
					elephantine.LogKeyAlertCode, "attachment_object_rollback",
					elephantine.LogKeyDocumentUUID, r.Document.String(),
					"object_name", r.Name,
				)
			}
		}
	}

	defer func() {
		if outErr == nil || len(attachRollback) == 0 {
			return
		}

		rollback()
	}()

	for _, state := range updates {
		for name, spec := range state.Request.AttachObjects {
			current, err := q.GetAttachedObject(ctx,
				postgres.GetAttachedObjectParams{
					Document: state.UUID,
					Name:     name,
				})
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf(
					"get current %q attachments for %s: %w",
					name, state.UUID, err)
			}

			version := current.Version + 1

			objectVersion, err := s.assets.AttachUpload(ctx, spec.ID, state.UUID, name)
			if err != nil {
				return nil, fmt.Errorf(
					"attach upload %q to document %s as %q: %w",
					spec.ID, state.UUID, name, err,
				)
			}

			attachRollback = append(attachRollback, attached{
				Document:       state.UUID,
				Name:           name,
				CreatedVersion: objectVersion,
			})

			err = q.AddAttachedObject(ctx,
				postgres.AddAttachedObjectParams{
					Document:      state.UUID,
					Name:          name,
					Version:       version,
					ObjectVersion: objectVersion,
					AttachedAt:    state.Version,
					CreatedAt:     pg.Time(state.Created),
					CreatedBy:     state.Creator,
					Meta: postgres.AssetMetadata{
						Filename: spec.Meta.Filename,
						Mimetype: spec.Meta.Mimetype,
						Props:    spec.Meta.Props,
					},
				})
			if err != nil {
				return nil, fmt.Errorf(
					"add attached %q object for document %s: %w",
					name, state.UUID, err)
			}

			err = q.SetCurrentAttachedObject(ctx,
				postgres.SetCurrentAttachedObjectParams{
					Document: state.UUID,
					Name:     name,
					Version:  version,
					Deleted:  false,
				})
			if err != nil {
				return nil, fmt.Errorf(
					"add current attached %q object for document %s: %w",
					name, state.UUID, err)
			}
		}

		for _, name := range state.Request.DetachObjects {
			current, err := q.GetAttachedObject(ctx,
				postgres.GetAttachedObjectParams{
					Document: state.UUID,
					Name:     name,
				})
			if errors.Is(err, pgx.ErrNoRows) || current.Deleted {
				continue
			} else if err != nil {
				return nil, fmt.Errorf(
					"get current %q attachments for %s: %w",
					name, state.UUID, err)
			}

			err = s.assets.DeleteObject(ctx, state.UUID, name)
			if err != nil {
				return nil, fmt.Errorf("delete attached object: %w", err)
			}

			attachRollback = append(attachRollback, attached{
				Document: state.UUID,
				Name:     name,
			})

			err = q.SetCurrentAttachedObject(ctx,
				postgres.SetCurrentAttachedObjectParams{
					Document: state.UUID,
					Name:     name,
					Version:  current.Version,
					Deleted:  true,
				})
			if err != nil {
				return nil, fmt.Errorf(
					"set current attached %q object for document %s as deleted: %w",
					name, state.UUID, err)
			}
		}
	}

	return rollback, nil
}

// Attached objects that should be rolled back to previous version, or deleted.
type attached struct {
	Document       uuid.UUID
	DocVersion     int64
	Name           string
	CreatedVersion string
}

func (s *PGDocStore) doAttachmentObjectRevert(
	ctx context.Context, a attached,
) (outErr error) {
	// Detach the context cancellation from the parent context, if we get to
	// this point we don't want to allow cancellation.
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		10*time.Second,
	)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("start transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	// We run a pre-flight request for an optimistic lock check (and to get
	// a for-update-lock on the document row). If the document has been
	// updated between our failure and the revert we will have to give up
	// and flag the problem for human review.
	_, err = s.UpdatePreflight(ctx, q,
		a.Document, a.DocVersion, nil)
	if err != nil {
		return fmt.Errorf("acquire optimistic lock: %w", err)
	}

	var doesntExist bool

	current, err := q.GetAttachedObject(ctx,
		postgres.GetAttachedObjectParams{
			Document: a.Document,
			Name:     a.Name,
		})
	if errors.Is(err, pgx.ErrNoRows) {
		doesntExist = true
	} else if err != nil {
		return fmt.Errorf("read information about current attachment: %w", err)
	}

	switch {
	// Reverting first time creation.
	case doesntExist && a.CreatedVersion != "":
		err := s.assets.DeleteObject(ctx, a.Document, a.Name)
		if err != nil {
			return fmt.Errorf("delete object: %w", err)
		}
	// Reverting detach (delete) of object that didn't exist. This shouldn't
	// happen as we treat a detach of something that doesn't exist as a noop
	// and no rollback should be queued, but keeping this for case
	// completeness - belt and suspenders.
	case doesntExist && a.CreatedVersion == "":
		return nil
	// Reverting a new version that replaced previous version - or a delete
	// of the attached object. Either way we need to restore the last
	// version.
	default:
		newVersion, err := s.assets.RevertObject(
			ctx, a.Document, a.Name, current.ObjectVersion)
		if err != nil {
			return fmt.Errorf(
				"revert back to object version %q: %w",
				current.ObjectVersion, err)
		}

		// Update the object version of the current version so that it
		// refers to the version created by the revert.
		err = q.ChangeAttachedObjectVersion(ctx,
			postgres.ChangeAttachedObjectVersionParams{
				Document:      a.Document,
				Name:          a.Name,
				Version:       current.Version,
				ObjectVersion: newVersion,
			})
		if err != nil {
			return fmt.Errorf("set object version to reverted object version: %w", err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit object revert: %w", err)
	}

	return nil
}

// CreateUpload implements DocStore.
func (s *PGDocStore) CreateUpload(
	ctx context.Context,
	upload Upload,
) error {
	err := s.reader.CreateUpload(ctx, postgres.CreateUploadParams{
		ID:        upload.ID,
		CreatedAt: pg.Time(upload.CreatedAt),
		CreatedBy: upload.CreatedBy,
		Meta: postgres.AssetMetadata{
			Filename: upload.Meta.Filename,
			Mimetype: upload.Meta.Mimetype,
			Props:    upload.Meta.Props,
		},
	})
	if err != nil {
		return fmt.Errorf("insert upload row: %w", err)
	}

	return nil
}

// GetAttachments implements DocStore.
func (s *PGDocStore) GetAttachments(
	ctx context.Context,
	documents []uuid.UUID,
	attachment string,
	getDownloadLink bool,
) ([]AttachmentDetails, error) {
	rows, err := s.reader.GetAttachmentsForDocuments(ctx,
		postgres.GetAttachmentsForDocumentsParams{
			Documents: documents,
			Name:      attachment,
		})
	if err != nil {
		return nil, fmt.Errorf("read from database: %w", err)
	}

	res := make([]AttachmentDetails, len(rows))

	for i := range rows {
		var dlLink string

		if getDownloadLink {
			l, err := s.assets.CreateDownloadURL(ctx,
				rows[i].Document,
				rows[i].Name)
			if err != nil {
				return nil, fmt.Errorf("create download link: %w", err)
			}

			dlLink = l
		}

		res[i] = AttachmentDetails{
			Document:     rows[i].Document,
			Name:         rows[i].Name,
			Version:      rows[i].Version,
			DownloadLink: dlLink,
			Filename:     rows[i].Meta.Filename,
			ContentType:  rows[i].Meta.Mimetype,
		}
	}

	return res, nil
}

// GetDeliverableInfo implements DocStore.
func (s *PGDocStore) GetDeliverableInfo(ctx context.Context, id uuid.UUID) (DeliverableInfo, error) {
	info, err := s.reader.GetDeliverableInfo(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeliverableInfo{
			HasPlanningInfo: false,
		}, nil
	} else if err != nil {
		return DeliverableInfo{}, fmt.Errorf("read from database: %w", err)
	}

	return DeliverableInfo{
		HasPlanningInfo: true,
		PlanningUUID:    &info.PlanningUuid,
		AssignmentUUID:  &info.AssignmentUuid,
		EventUUID:       pg.ToUUIDPointer(info.EventUuid),
	}, nil
}

func pointer[T any](v T) *T {
	return &v
}

func loadWorkflowState(
	ctx context.Context, q *postgres.Queries,
	workflow DocumentWorkflow, id uuid.UUID,
) (WorkflowState, error) {
	row, err := q.GetWorkflowState(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.Start(), nil
	} else if err != nil {
		return WorkflowState{}, fmt.Errorf("read from database: %w", err)
	}

	return WorkflowState{
		Step:           row.Step,
		LastCheckpoint: row.Checkpoint,
	}, nil
}

type documentVersionProps struct {
	UUID             uuid.UUID
	Version          int64
	Type             string
	URI              string
	Language         string
	Created          time.Time
	Creator          string
	MainDocument     *uuid.UUID
	MainDocumentType string
	MetaJSON         []byte
	DocJSON          []byte
	Document         *newsdoc.Document
}

func createNewDocumentVersion(
	ctx context.Context,
	tx pgx.Tx,
	q *postgres.Queries,
	props documentVersionProps,
) error {
	err := q.UpsertDocument(ctx, postgres.UpsertDocumentParams{
		UUID:        props.UUID,
		URI:         props.URI,
		Type:        props.Type,
		Version:     props.Version,
		Created:     pg.Time(props.Created),
		CreatorUri:  props.Creator,
		Language:    pg.TextOrNull(props.Language),
		MainDoc:     pg.PUUID(props.MainDocument),
		MainDocType: pg.TextOrNull(props.MainDocumentType),
	})
	if pg.IsConstraintError(err, "document_uri_key") {
		return DocStoreErrorf(ErrCodeDuplicateURI,
			"duplicate URI: %s", props.URI)
	} else if err != nil {
		return fmt.Errorf(
			"failed to create document in database: %w", err)
	}

	err = q.CreateDocumentVersion(ctx, postgres.CreateDocumentVersionParams{
		UUID:         props.UUID,
		Version:      props.Version,
		Created:      pg.Time(props.Created),
		CreatorUri:   props.Creator,
		Meta:         props.MetaJSON,
		Language:     pg.Text(props.Language),
		DocumentData: props.DocJSON,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to create version in database: %w", err)
	}

	// TODO: I'm a bit unsure about this now, was it a good idea to have a
	// document type that gets special treatment?
	if props.Type == "core/planning-item" {
		doc := props.Document
		if doc == nil {
			var d newsdoc.Document

			err := json.Unmarshal(props.DocJSON, &d)
			if err != nil {
				return fmt.Errorf("unmarshal full document: %w", err)
			}

			doc = &d
		}

		err = planning.UpdateDatabase(ctx, tx,
			*doc, props.Version)
		if err != nil {
			return fmt.Errorf(
				"failed to update planning data: %w", err)
		}
	}

	return nil
}

type docUpdateState struct {
	DocumentUpdate

	Request    *UpdateRequest
	Doc        *newsdoc.Document
	DocJSON    []byte
	MetaJSON   []byte
	StatusMeta []map[string]string

	Type        string
	Exists      bool
	Language    string
	OldLanguage string
	IsMetaDoc   bool
	MainDocID   *uuid.UUID
	MainDocType string
	SystemState string
}

func newUpdateState(req *UpdateRequest) (*docUpdateState, error) {
	state := docUpdateState{
		DocumentUpdate: DocumentUpdate{
			UUID:    req.UUID,
			Created: req.Updated,
			Creator: req.Updater,
			Meta:    req.Meta,
		},
		Request:    req,
		Doc:        req.Document,
		StatusMeta: make([]map[string]string, len(req.Status)),
		MainDocID:  req.MainDocument,
	}

	if state.Doc != nil {
		state.Type = state.Doc.Type

		dj, err := json.Marshal(state.Doc)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal document for storage: %w", err)
		}

		state.DocJSON = dj
	}

	if len(state.Request.Meta) > 0 {
		mj, err := json.Marshal(state.Request.Meta)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal metadata for storage: %w", err)
		}

		state.MetaJSON = mj
	}

	for i, stat := range state.Request.Status {
		state.StatusMeta[i] = stat.Meta
	}

	return &state, nil
}

func (s *PGDocStore) buildStatusRuleInput(
	ctx context.Context, q *postgres.Queries,
	uuid uuid.UUID, name string, status Status, up DocumentUpdate,
	d *newsdoc.Document, versionMeta newsdoc.DataMap, statusHeads map[string]StatusHead,
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
	} else if d == nil && input.Status.Version != -1 {
		d, meta, err := s.loadDocument(
			ctx, q, uuid, status.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return StatusRuleInput{}, DocStoreErrorf(
				ErrCodeNotFound, "cannot set a status for a version that doesn't exist")
		} else if err != nil {
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
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.UpdateStatus(ctx, postgres.UpdateStatusParams{
			Type:     req.Type,
			Name:     req.Name,
			Disabled: req.Disabled,
		})
		if err != nil {
			return fmt.Errorf("failed to update status: %w", err)
		}

		err = s.workflows.Publish(ctx, tx, WorkflowEvent{
			Type: WorkflowEventTypeStatusChange,
			Name: req.Name,
		})
		if err != nil {
			return fmt.Errorf("failed to send workflow update notification: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) GetStatuses(ctx context.Context, docType string) ([]DocumentStatus, error) {
	res, err := s.reader.GetActiveStatuses(ctx, pg.TextOrNull(docType))
	if err != nil {
		return nil, fmt.Errorf("failed to get active statuses: %w", err)
	}

	var list []DocumentStatus

	for i := range res {
		list = append(list, DocumentStatus{
			Type: res[i].Type,
			Name: res[i].Name,
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

	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.UpdateStatusRule(ctx, postgres.UpdateStatusRuleParams{
			Type:        rule.Type,
			Name:        rule.Name,
			Description: rule.Description,
			AccessRule:  rule.AccessRule,
			AppliesTo:   rule.AppliesTo,
			Expression:  rule.Expression,
		})
		if err != nil {
			return fmt.Errorf("failed to update status rule: %w", err)
		}

		err = s.workflows.Publish(ctx, tx, WorkflowEvent{
			Type:    WorkflowEventTypeStatusRuleChange,
			DocType: rule.Type,
			Name:    rule.Name,
		})
		if err != nil {
			return fmt.Errorf("failed to send workflow update notification: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) DeleteStatusRule(
	ctx context.Context, docType string, name string,
) error {
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.DeleteStatusRule(ctx, postgres.DeleteStatusRuleParams{
			Type: docType,
			Name: name,
		})
		if err != nil {
			return fmt.Errorf("failed to delete status rule: %w", err)
		}

		err = s.workflows.Publish(ctx, tx, WorkflowEvent{
			Type:    WorkflowEventTypeStatusRuleChange,
			DocType: docType,
			Name:    name,
		})
		if err != nil {
			return fmt.Errorf("failed to send workflow update notification: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) GetDocumentWorkflows(
	ctx context.Context,
) ([]DocumentWorkflow, error) {
	res, err := s.reader.GetDocumentWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workflows: %w", err)
	}

	var list []DocumentWorkflow

	for _, row := range res {
		wf := DocumentWorkflow{
			Type:       row.Type,
			Updated:    row.Updated.Time,
			UpdaterURI: row.UpdaterUri,
		}

		err := json.Unmarshal(row.Configuration, &wf.Configuration)
		if err != nil {
			s.logger.Error("invalid document workflow for %q: %w", row.Type, err)

			continue
		}

		list = append(list, wf)
	}

	return list, nil
}

func (s *PGDocStore) GetDocumentWorkflow(
	ctx context.Context, docType string,
) (DocumentWorkflow, error) {
	var _z DocumentWorkflow

	row, err := s.reader.GetDocumentWorkflow(ctx, docType)
	if err != nil {
		return _z, fmt.Errorf("failed to fetch workflows: %w", err)
	}

	wf := DocumentWorkflow{
		Type:       row.Type,
		Updated:    row.Updated.Time,
		UpdaterURI: row.UpdaterUri,
	}

	err = json.Unmarshal(row.Configuration, &wf.Configuration)
	if err != nil {
		return _z, fmt.Errorf("invalid document workflow for %q: %w", row.Type, err)
	}

	return wf, nil
}

func (s *PGDocStore) SetDocumentWorkflow(
	ctx context.Context, workflow DocumentWorkflow,
) error {
	config, err := json.Marshal(workflow.Configuration)
	if err != nil {
		return fmt.Errorf("marshal configuration for storage: %w", err)
	}

	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.SetDocumentWorkflow(ctx,
			postgres.SetDocumentWorkflowParams{
				Type:          workflow.Type,
				Updated:       pg.Time(workflow.Updated),
				UpdaterUri:    workflow.UpdaterURI,
				Configuration: config,
			})
		if err != nil {
			return fmt.Errorf("failed to update workflow: %w", err)
		}

		err = s.workflows.Publish(ctx, tx, WorkflowEvent{
			Type:    WorkflowEventTypeWorkflowChange,
			DocType: workflow.Type,
		})
		if err != nil {
			return fmt.Errorf("failed to send workflow update notification: %w", err)
		}

		return nil
	})
}

func (s *PGDocStore) DeleteDocumentWorkflow(
	ctx context.Context, docType string,
) error {
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		affected, err := q.DeleteDocumentWorkflow(ctx, docType)
		if err != nil {
			return fmt.Errorf("failed to delete workflow: %w", err)
		}

		// Don't notify if nothing was deleted.
		if affected == 0 {
			return nil
		}

		err = s.workflows.Publish(ctx, tx, WorkflowEvent{
			Type:    WorkflowEventTypeWorkflowChange,
			DocType: docType,
		})
		if err != nil {
			return fmt.Errorf("failed to send workflow update notification: %w", err)
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
			Type:        row.Type,
			Name:        row.Name,
			Description: row.Description,
			AccessRule:  row.AccessRule,
			AppliesTo:   row.AppliesTo,
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

// GetTypeOfDocument implements DocStore.
func (s *PGDocStore) GetTypeOfDocument(ctx context.Context, uuid uuid.UUID) (string, error) {
	t, err := s.reader.GetTypeOfDocument(ctx, uuid)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", DocStoreErrorf(ErrCodeNotFound, "no such document")
	} else if err != nil {
		return "", fmt.Errorf("query failed: %w", err)
	}

	return t, nil
}

type DocumentMetaType struct {
	MetaType       string
	IsMetaDocument bool
	Exists         bool
}

// GetMetaTypeForDocument implements DocStore.
func (s *PGDocStore) GetMetaTypeForDocument(
	ctx context.Context, uuid uuid.UUID,
) (DocumentMetaType, error) {
	t, err := s.reader.CheckMetaDocumentType(ctx, uuid)
	if errors.Is(err, pgx.ErrNoRows) {
		return DocumentMetaType{}, nil
	} else if err != nil {
		return DocumentMetaType{}, fmt.Errorf("query failed: %w", err)
	}

	return DocumentMetaType{
		MetaType:       t.MetaType,
		IsMetaDocument: t.IsMetaDoc.Bool,
		Exists:         true,
	}, nil
}

// RegisterMetaType implements DocStore.
func (s *PGDocStore) RegisterMetaType(
	ctx context.Context, metaType string, exclusive bool,
) error {
	q := postgres.New(s.pool)

	err := q.RegisterMetaType(ctx, postgres.RegisterMetaTypeParams{
		MetaType:         metaType,
		ExclusiveForMeta: exclusive,
	})
	if err != nil {
		return fmt.Errorf("write to db: %w", err)
	}

	return nil
}

// RegisterMetaTypeUse implements DocStore.
func (s *PGDocStore) RegisterMetaTypeUse(ctx context.Context, mainType string, metaType string) error {
	q := postgres.New(s.pool)

	err := q.RegisterMetaTypeUse(ctx, postgres.RegisterMetaTypeUseParams{
		MainType: mainType,
		MetaType: metaType,
	})

	switch {
	case pg.IsConstraintError(err, "meta_type_use_meta_type_fkey"):
		return DocStoreErrorf(ErrCodeFailedPrecondition,
			"the meta type hasn't been registered")
	case pg.IsConstraintError(err, "meta_type_use_pkey"):
		return DocStoreErrorf(ErrCodeExists,
			"the meta document use has already been registered")
	case err != nil:
		return fmt.Errorf("write to db: %w", err)
	}

	return nil
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

func (s *PGDocStore) Lock(ctx context.Context, req LockRequest) (LockResult, error) {
	now := time.Now()
	expires := now.Add(time.Millisecond * time.Duration(req.TTL))

	res := LockResult{
		Created: now,
		Expires: expires,
		Token:   uuid.NewString(),
	}

	err := pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		info, err := s.UpdatePreflight(ctx, q, req.UUID, 0, nil)
		if err != nil {
			return err
		}

		if info.MainDoc != nil {
			return DocStoreErrorf(ErrCodeBadRequest, "meta documents cannot be locked")
		}

		if !info.Exists {
			return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		}

		if info.Lock.Token != "" {
			return DocStoreErrorf(ErrCodeDocumentLock, "document locked")
		}

		err = q.DeleteExpiredDocumentLock(ctx, postgres.DeleteExpiredDocumentLockParams{
			Cutoff: pg.Time(now),
			Uuids:  []uuid.UUID{req.UUID},
		})
		if err != nil {
			return fmt.Errorf("could not delete expired locks: %w", err)
		}

		err = q.InsertDocumentLock(ctx, postgres.InsertDocumentLockParams{
			UUID:    req.UUID,
			Token:   res.Token,
			Created: pg.Time(now),
			Expires: pg.Time(expires),
			URI:     pg.TextOrNull(req.URI),
			App:     pg.TextOrNull(req.App),
			Comment: pg.TextOrNull(req.Comment),
		})
		if err != nil {
			return fmt.Errorf("failed to insert document lock: %w", err)
		}

		return nil
	})
	if err != nil {
		return LockResult{}, err
	}

	return res, nil
}

func (s *PGDocStore) UpdateLock(ctx context.Context, req UpdateLockRequest) (LockResult, error) {
	now := time.Now()
	expires := now.Add(time.Millisecond * time.Duration(req.TTL))

	res := LockResult{
		Created: now,
		Expires: expires,
		Token:   req.Token,
	}

	err := pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		info, err := s.UpdatePreflight(ctx, q, req.UUID, 0, nil)
		if err != nil {
			return err
		}

		if !info.Exists {
			return DocStoreErrorf(ErrCodeNotFound, "not found")
		}

		if info.Lock.Token == "" {
			return DocStoreErrorf(ErrCodeNoSuchLock, "not locked")
		}

		lock := checkLock(info.Lock, req.Token)
		if lock == lockCheckDenied {
			return DocStoreErrorf(ErrCodeDocumentLock, "invalid lock token")
		}

		err = q.UpdateDocumentLock(ctx, postgres.UpdateDocumentLockParams{
			UUID:    req.UUID,
			Expires: pg.Time(expires),
		})
		if err != nil {
			return fmt.Errorf("failed to extend lock: %w", err)
		}

		return nil
	})
	if err != nil {
		return LockResult{}, err
	}

	return res, nil
}

func (s *PGDocStore) Unlock(ctx context.Context, uuid uuid.UUID, token string) error {
	err := pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		info, err := s.UpdatePreflight(ctx, q, uuid, 0, nil)
		if err != nil {
			return err
		}

		if !info.Exists || info.Lock.Token == "" {
			return nil
		}

		if info.Lock.Token != token {
			return DocStoreErrorf(ErrCodeDocumentLock, "document locked")
		}

		deleted, err := s.reader.DeleteDocumentLock(ctx, postgres.DeleteDocumentLockParams{
			UUID:  uuid,
			Token: token,
		})
		if err != nil {
			return fmt.Errorf("could not delete lock: %w", err)
		}

		if deleted == 0 {
			return errors.New("data constistency error, failed to delete lock")
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

	err = pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
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
			err = s.activateSchema(ctx, tx, req.Name, req.Version)
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
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		return s.activateSchema(ctx, tx, name, version)
	})
}

// RegisterSchema implements DocStore.
func (s *PGDocStore) activateSchema(
	ctx context.Context, tx pgx.Tx, name, version string,
) error {
	q := postgres.New(tx)

	err := q.ActivateSchema(ctx, postgres.ActivateSchemaParams{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to activate schema version: %w", err)
	}

	err = s.schemas.Publish(ctx, tx, SchemaEvent{
		Type: SchemaEventTypeActivation,
		Name: name,
	})
	if err != nil {
		return fmt.Errorf("failed to send schema update notification: %w", err)
	}

	return nil
}

// DeactivateSchema implements DocStore.
func (s *PGDocStore) DeactivateSchema(
	ctx context.Context, name string,
) (outErr error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// We defer a rollback, rollback after commit won't be treated as an
	// error.
	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	err = q.DeactivateSchema(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to remove active schema: %w", err)
	}

	err = s.schemas.Publish(ctx, tx, SchemaEvent{
		Type: SchemaEventTypeDeactivation,
		Name: name,
	})
	if err != nil {
		return fmt.Errorf("failed to send schema update notification: %w", err)
	}

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

type EnforcedDeprecations map[string]bool

// GetEnforcedDeprecations implements SchemaLoader.
func (s *PGDocStore) GetEnforcedDeprecations(
	ctx context.Context,
) (EnforcedDeprecations, error) {
	rows, err := s.reader.GetEnforcedDeprecations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch enforced deprecations: %w", err)
	}

	res := make(EnforcedDeprecations, len(rows))

	for _, l := range rows {
		res[l] = true
	}

	return res, nil
}

// GetDeprecations implements SchemaLoader.
func (s *PGDocStore) GetDeprecations(
	ctx context.Context,
) ([]*Deprecation, error) {
	rows, err := s.reader.GetDeprecations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch deprecations: %w", err)
	}

	var res []*Deprecation

	for i := range rows {
		deprecation := &Deprecation{
			Label:    rows[i].Label,
			Enforced: rows[i].Enforced,
		}

		res = append(res, deprecation)
	}

	return res, nil
}

// UpdateDeprecation implements SchemaLoader.
func (s *PGDocStore) UpdateDeprecation(
	ctx context.Context, deprecation Deprecation,
) error {
	err := pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.UpdateDeprecation(ctx, postgres.UpdateDeprecationParams{
			Label:    deprecation.Label,
			Enforced: deprecation.Enforced,
		})
		if err != nil {
			return fmt.Errorf("failed to save deprecation to database: %w", err)
		}

		err = s.deprecations.Publish(ctx, tx, DeprecationEvent{
			Label: deprecation.Label,
		})
		if err != nil {
			return fmt.Errorf("failed to send deprecation update notification: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *PGDocStore) RegisterMetricKind(
	ctx context.Context, name string, aggregation Aggregation,
) error {
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
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

// GetMetrics implements MetricStore.
func (s *PGDocStore) GetMetrics(
	ctx context.Context, uuids []uuid.UUID, kinds []string,
) ([]Metric, error) {
	if len(kinds) == 0 {
		kinds = nil
	}

	rows, err := s.reader.GetMetrics(ctx, postgres.GetMetricsParams{
		UUID: uuids,
		Kind: kinds,
	})
	if err != nil {
		return nil, fmt.Errorf("read from db: %w", err)
	}

	var res []Metric

	for _, row := range rows {
		res = append(res, Metric{
			UUID:  row.UUID,
			Kind:  row.Kind,
			Label: row.Label,
			Value: row.Value,
		})
	}

	return res, nil
}

// RegisterMetric implements MetricStore.
func (s *PGDocStore) RegisterOrReplaceMetric(ctx context.Context, metric Metric) error {
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.RegisterOrReplaceMetric(ctx, postgres.RegisterOrReplaceMetricParams{
			UUID:  metric.UUID,
			Kind:  metric.Kind,
			Label: metric.Label,
			Value: metric.Value,
		})

		switch {
		case pg.IsConstraintError(err, "metric_kind_fkey"):
			return DocStoreErrorf(ErrCodeNotFound, "metric kind not found")
		case pg.IsConstraintError(err, "metric_label_fkey"):
			return DocStoreErrorf(ErrCodeNotFound, "metric label not found")
		case pg.IsConstraintError(err, "metric_label_kind_match"):
			return DocStoreErrorf(ErrCodeNotFound, "label does not apply to kind")
		case pg.IsConstraintError(err, "metric_uuid_fkey"):
			return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		case err != nil:
			return fmt.Errorf("failed to save to database: %w", err)
		}

		return nil
	})
}

// RegisterMetric implements MetricStore.
func (s *PGDocStore) RegisterOrIncrementMetric(ctx context.Context, metric Metric) error {
	return pg.WithTX(ctx, s.pool, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.RegisterOrIncrementMetric(ctx, postgres.RegisterOrIncrementMetricParams{
			UUID:  metric.UUID,
			Kind:  metric.Kind,
			Label: metric.Label,
			Value: metric.Value,
		})

		switch {
		case pg.IsConstraintError(err, "metric_kind_fkey"):
			return DocStoreErrorf(ErrCodeNotFound, "metric kind not found")
		case pg.IsConstraintError(err, "metric_label_fkey"):
			return DocStoreErrorf(ErrCodeNotFound, "metric label not found")
		case pg.IsConstraintError(err, "metric_label_kind_match"):
			return DocStoreErrorf(ErrCodeNotFound, "label does not apply to kind")
		case pg.IsConstraintError(err, "metric_uuid_fkey"):
			return DocStoreErrorf(ErrCodeNotFound, "document uuid not found")
		case err != nil:
			return fmt.Errorf("failed to save to database: %w", err)
		}

		return nil
	})
}

func updateACL(
	ctx context.Context, q *postgres.Queries, updater string,
	docUUID uuid.UUID, docType string, language string, updateACL []ACLEntry,
) error {
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
		UpdaterUri: updater,
		Language:   language,
	})
	if err != nil {
		return fmt.Errorf("failed to record audit trail: %w", err)
	}

	return nil
}

type UpdatePrefligthInfo struct {
	Info        postgres.GetDocumentForUpdateRow
	Exists      bool
	Lock        Lock
	MainDoc     *uuid.UUID
	MainDocType string
	Language    string
}

// UpdatePreflight must always be called before any information related to a
// document is updated. mainDocUUID must be supplied if the update might be the
// creation of the first version of a meta document.
func (s *PGDocStore) UpdatePreflight(
	ctx context.Context, q *postgres.Queries,
	docUUID uuid.UUID, ifMatch int64, mainDocUUID *uuid.UUID,
) (*UpdatePrefligthInfo, error) {
	info, err := q.GetDocumentForUpdate(ctx, postgres.GetDocumentForUpdateParams{
		UUID: docUUID,
		Now:  pg.Time(time.Now()),
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf(
			"failed to get document information: %w", err)
	}

	exists := !errors.Is(err, pgx.ErrNoRows)
	currentVersion := info.CurrentVersion

	if info.SystemState.Valid {
		return nil, DocStoreErrorf(ErrCodeSystemLock,
			"the document is in a %q state and cannot be changed",
			info.SystemState.String)
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

	var mainDocType string

	if info.MainDoc.Valid {
		mainDocUUID = pg.ToUUIDPointer(info.MainDoc)
	}

	if mainDocUUID != nil {
		mainInfo, err := q.GetDocumentRow(ctx, *mainDocUUID)
		if err != nil {
			return nil, fmt.Errorf("get main document information: %w", err)
		}

		mainDocType = mainInfo.Type
	}

	return &UpdatePrefligthInfo{
		Info:        info,
		Exists:      exists,
		MainDoc:     pg.ToUUIDPointer(info.MainDoc),
		MainDocType: mainDocType,
		Language:    info.Language.String,
		Lock: Lock{
			URI:     info.LockUri.String,
			Token:   info.LockToken.String,
			Created: info.LockCreated.Time,
			Expires: info.LockExpires.Time,
			App:     info.LockApp.String,
			Comment: info.LockComment.String,
		},
	}, nil
}

func addEventToOutbox(
	ctx context.Context,
	tx pgx.Tx,
	evt postgres.OutboxEvent,
) error {
	q := postgres.New(tx)

	id, err := q.AddEventToOutbox(ctx, evt)
	if err != nil {
		return fmt.Errorf("store event in outbox: %w", err)
	}

	err = pg.Publish(ctx, tx, NotifyEventOutbox, id)
	if err != nil {
		return fmt.Errorf("send outbox notification: %w", err)
	}

	return nil
}
