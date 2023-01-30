package docformat

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
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
func (*PGDocStore) GetDocument(ctx context.Context, uuid string, version int) (*Document, error) {
	panic("unimplemented")
}

// GetDocumentMeta implements DocStore
func (*PGDocStore) GetDocumentMeta(ctx context.Context, uuid string) (*DocumentMeta, error) {
	panic("unimplemented")
}

// Update implements DocStore
func (*PGDocStore) Update(ctx context.Context, update UpdateRequest) (*DocumentUpdate, error) {
	panic("unimplemented")
}

// Interface guard
var _ DocStore = &PGDocStore{}
