package internal

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
)

func Migrate(ctx context.Context, pool *pgxpool.Pool, dbFs fs.FS) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migration: %w", err)
	}

	defer conn.Release()

	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}

	err = m.LoadMigrations(dbFs)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	err = m.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}
