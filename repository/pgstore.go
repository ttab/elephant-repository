package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/postgres"
)

const (
	elephantCRC            = 3997770000
	LockSigningKeys        = elephantCRC + 1
	LockLogicalReplication = elephantCRC + 2
)

type PGDocStore struct {
	pool     *pgxpool.Pool
	reader   *postgres.Queries
	archived *FanOut[ArchivedEvent]
}

type ArchiveEventType int

const (
	ArchiveEventTypeStatus ArchiveEventType = iota
	ArchiveEventTypeVersion
)

type ArchivedEvent struct {
	Type    ArchiveEventType
	UUID    uuid.UUID
	Version int64
	Name    string
}

func NewPGDocStore(pool *pgxpool.Pool) (*PGDocStore, error) {
	return &PGDocStore{
		pool:     pool,
		reader:   postgres.New(pool),
		archived: NewFanOut[ArchivedEvent](),
	}, nil
}

// Delete implements DocStore
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
	defer s.safeRollback(ctx, tx, "document delete")

	q := postgres.New(tx)

	info, err := s.updatePreflight(ctx, q, req.UUID, req.IfMatch)
	if err != nil {
		return err
	}

	if !info.Exists {
		return nil
	}

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
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	recordID, err := q.InsertDeleteRecord(ctx,
		postgres.InsertDeleteRecordParams{
			Uuid:       req.UUID,
			Uri:        info.Info.Uri,
			Version:    info.Info.CurrentVersion,
			Created:    pgTime(req.Updated),
			CreatorUri: req.Updater,
			Meta:       metaJSON,
		})
	if err != nil {
		return fmt.Errorf("failed to create delete record: %w", err)
	}

	err = q.DeleteDocument(ctx, postgres.DeleteDocumentParams{
		Uuid:     req.UUID,
		Uri:      info.Info.Uri,
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

// GetDocument implements DocStore
func (s *PGDocStore) GetDocument(
	ctx context.Context, uuid uuid.UUID, version int64,
) (*doc.Document, error) {
	var (
		err  error
		data []byte
	)

	if version == 0 {
		data, err = s.reader.GetDocumentData(ctx, uuid)
	} else {
		data, err = s.reader.GetDocumentVersionData(ctx,
			postgres.GetDocumentVersionDataParams{
				Uuid:    uuid,
				Version: version,
			})
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, DocStoreErrorf(ErrCodeNotFound, "not found")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch document data: %w", err)
	}

	// TODO: check for nil data after archiving has been implemented.

	var d doc.Document

	err = json.Unmarshal(data, &d)
	if err != nil {
		return nil, fmt.Errorf(
			"got an unreadable document from the database: %w", err)
	}

	return &d, nil
}

func (s *PGDocStore) GetVersion(
	ctx context.Context, uuid uuid.UUID, version int64,
) (DocumentUpdate, error) {
	v, err := s.reader.GetVersion(ctx, postgres.GetVersionParams{
		Uuid:    uuid,
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
				"failed to unmarshal metadata for version %d: %err",
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
		Uuid:   uuid,
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
					"failed to unmarshal metadata for version %d: %err",
					v.Version, err)
			}
		}

		updates = append(updates, up)
	}

	return updates, nil
}

// GetDocumentMeta implements DocStore
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
	}

	heads, err := s.reader.GetFullDocumentHeads(ctx, uuid)
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

		meta.Statuses[head.Name] = status
	}

	return &meta, nil
}

// CheckPermission implements DocStore
func (s *PGDocStore) CheckPermission(
	ctx context.Context, req CheckPermissionRequest,
) (CheckPermissionResult, error) {
	access, err := s.reader.CheckPermission(ctx,
		postgres.CheckPermissionParams{
			Uuid:       req.UUID,
			Uri:        pgStringArray(req.GranteeURIs),
			Permission: req.Permission,
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

func pgStringArray(v []string) pgtype.Array[string] {
	return pgtype.Array[string]{
		Elements: v,
		Valid:    v != nil,
		Dims: []pgtype.ArrayDimension{
			{
				Length:     int32(len(v)),
				LowerBound: 1,
			},
		},
	}
}

// Update implements DocStore
func (s *PGDocStore) Update(ctx context.Context, update UpdateRequest) (*DocumentUpdate, error) {
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
	defer s.safeRollback(ctx, tx, "document update")

	q := postgres.New(tx)

	info, err := s.updatePreflight(ctx, q, update.UUID, update.IfMatch)
	if err != nil {
		return nil, err
	}

	up := DocumentUpdate{
		Version: info.Info.CurrentVersion,
		Created: update.Updated,
		Creator: update.Updater,
		Meta:    update.Meta,
	}

	if update.Document != nil {
		up.Version++

		err = q.CreateVersion(ctx, postgres.CreateVersionParams{
			Uuid:         update.UUID,
			Version:      up.Version,
			Created:      pgTime(up.Created),
			CreatorUri:   up.Creator,
			Meta:         metaJSON,
			DocumentData: docJSON,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create version in database: %w", err)
		}
	}

	statusHeads := make(map[string]int64)

	if len(update.Status) > 0 {
		heads, err := q.GetDocumentHeads(ctx, update.UUID)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to get current document status heads: %w", err)
		}

		for _, head := range heads {
			statusHeads[head.Name] = head.ID
		}
	}

	for i, stat := range update.Status {
		statusID := statusHeads[stat.Name] + 1

		status := Status{
			Creator: up.Creator,
			Version: stat.Version,
			Meta:    stat.Meta,
			Created: up.Created,
		}

		if status.Version == 0 {
			status.Version = up.Version
		}

		err = q.CreateStatus(ctx, postgres.CreateStatusParams{
			Uuid:       update.UUID,
			Name:       stat.Name,
			ID:         statusID,
			Version:    status.Version,
			Created:    pgTime(up.Created),
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

	err = s.updateACL(ctx, q, update.UUID, updateACL)
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

func (s *PGDocStore) updateACL(
	ctx context.Context, q *postgres.Queries,
	docUUID uuid.UUID, updateACL []ACLEntry,
) error {
	if len(updateACL) == 0 {
		return nil
	}

	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return errors.New("unauthenticated context")
	}

	// Batch ACL updates, ACLs with empty permissions are dropped
	// immediately.
	var acls []postgres.ACLUpdateParams

	for _, acl := range updateACL {
		if len(acl.Permissions) == 0 {
			err := q.DropACL(ctx, postgres.DropACLParams{
				Uuid: docUUID,
				Uri:  acl.URI,
			})
			if err != nil {
				return fmt.Errorf(
					"failed to drop entry for %q: %w",
					acl.URI, err)
			}

			continue
		}

		acls = append(acls, postgres.ACLUpdateParams{
			Uuid:        docUUID,
			Uri:         acl.URI,
			Permissions: pgStringArray(acl.Permissions),
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
		Uuid:       docUUID,
		Updated:    pgTime(time.Now()),
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

func pgTime(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{
		Time:  t,
		Valid: true,
	}
}

func (s *PGDocStore) safeRollback(ctx context.Context, tx pgx.Tx, txName string) {
	err := tx.Rollback(context.Background())
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		// TODO: better logging
		log.Println("failed to roll back", txName, err.Error())
	}
}

// Interface guard
var _ DocStore = &PGDocStore{}
