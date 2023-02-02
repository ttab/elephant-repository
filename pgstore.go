package docformat

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
	"github.com/ttab/docformat/postgres"
)

const (
	elephantCRC            = 3997770000
	LockArchive            = elephantCRC + 1
	LockLogicalReplication = elephantCRC + 2
)

type PGDocStore struct {
	pool *pgxpool.Pool
}

func NewPGDocStore(pool *pgxpool.Pool) (*PGDocStore, error) {
	return &PGDocStore{
		pool: pool,
	}, nil
}

// Delete implements DocStore
func (*PGDocStore) Delete(ctx context.Context, uuid string) error {
	panic("unimplemented")
}

// GetDocument implements DocStore
func (*PGDocStore) GetDocument(ctx context.Context, uuid string, version int64) (*Document, error) {
	panic("unimplemented")
}

// GetDocumentMeta implements DocStore
func (*PGDocStore) GetDocumentMeta(ctx context.Context, uuid string) (*DocumentMeta, error) {
	panic("unimplemented")
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
				"failed to marshal document for storage: %w", err)
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

	docUUID := uuid.MustParse(update.UUID)

	q := postgres.New(tx)

	info, err := q.GetDocumentForUpdate(ctx, docUUID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("failed to get document information: %w", err)
	}

	exists := !errors.Is(err, pgx.ErrNoRows)
	currentVersion := info.CurrentVersion

	switch update.IfMatch {
	case 0:
	case -1:
		if exists {
			return nil, DocStoreErrorf(ErrCodeOptimisticLock,
				"document already exists")
		}
	default:
		if currentVersion != update.IfMatch {
			return nil, DocStoreErrorf(ErrCodeOptimisticLock,
				"document version is %d, not %d as expected",
				info.CurrentVersion, update.IfMatch,
			)
		}
	}

	up := DocumentUpdate{
		Version: currentVersion,
		Created: update.Created,
		Updater: update.Updater,
		Meta:    update.Meta,
	}

	if update.Document != nil {
		up.Version++

		err = q.CreateVersion(ctx, postgres.CreateVersionParams{
			Uuid:         docUUID,
			Version:      up.Version,
			Created:      pgTime(up.Created),
			CreatorUri:   up.Updater.URI,
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
		heads, err := q.GetDocumentHeads(ctx, docUUID)
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
			Updater: up.Updater,
			Version: stat.Version,
			Meta:    stat.Meta,
			Created: up.Created,
		}

		if status.Version == 0 {
			status.Version = up.Version
		}

		err = q.CreateStatus(ctx, postgres.CreateStatusParams{
			Uuid:       docUUID,
			Name:       stat.Name,
			ID:         statusID,
			Version:    status.Version,
			Created:    pgTime(up.Created),
			CreatorUri: up.Updater.URI,
			Meta:       statusMeta[i],
		})
		if err != nil {
			return nil, fmt.Errorf(
				"failed to update %q status: %w",
				stat.Name, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	return nil, errors.New("not implemented")
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
