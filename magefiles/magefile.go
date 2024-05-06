//go:build mage
// +build mage

package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	//mage:import sql
	"github.com/ttab/mage/sql"
	//mage:import s3
	_ "github.com/ttab/mage/s3"
)

func ConnString() error {
	_, _ = fmt.Fprintln(os.Stdout, sql.MustGetConnString())

	return nil
}

// ReportingUser creates a reporting user for the local database.
func ReportingUser() error {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://admin:pass@localhost/elephant-repository")
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	_, err = conn.Exec(ctx, fmt.Sprintf(`
CREATE ROLE "repository-reportuser"
WITH LOGIN PASSWORD 'pass'`,
	))
	if err != nil && !isErrCode(err, pgerrcode.DuplicateObject) {
		return fmt.Errorf("create role: %w", err)
	}

	_, err = conn.Exec(ctx, `
GRANT SELECT
ON TABLE
   document, delete_record, document_version, document_status,
   status_heads, status, status_rule, acl, acl_audit, metric
TO "repository-reportuser"`)
	if err != nil {
		return fmt.Errorf("grant select: %w", err)
	}

	return nil
}

// ReplicationPermissions sets up replication permissions for the repository
// user.
func ReplicationPermissions() error {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://admin:pass@localhost/elephant-repository")
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	_, err = conn.Exec(ctx, `
ALTER ROLE "elephant-repository" REPLICATION`)
	if err != nil {
		return fmt.Errorf("grant replication to applicaton user: %w", err)
	}

	return nil
}

// IsConstraintError checks if an error was caused by a specific constraint
// violation.
func isErrCode(err error, code string) bool {
	if err == nil {
		return false
	}

	var pgerr *pgconn.PgError

	ok := errors.As(err, &pgerr)
	if !ok {
		return false
	}

	return pgerr.Code == code
}
