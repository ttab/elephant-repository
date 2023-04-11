package internal

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/slog"
)

func PGTime(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{
		Time:  t,
		Valid: true,
	}
}

func PGTimeOrNull(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}

	return pgtype.Timestamptz{
		Time:  t,
		Valid: true,
	}
}

func PGTextOrNull(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}

	return pgtype.Text{
		String: s,
		Valid:  true,
	}
}

func PGBigintOrNull(n int64) pgtype.Int8 {
	if n == 0 {
		return pgtype.Int8{}
	}

	return pgtype.Int8{
		Int64: n,
		Valid: true,
	}
}

// SafeRollback rolls back a transaction and logs if the rollback fails. If the
// transaction already has been closed it's not treated as an error.
func SafeRollback(
	ctx context.Context, logger *slog.Logger, tx pgx.Tx, txName string,
) {
	err := tx.Rollback(context.Background())
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		logger.ErrorCtx(ctx, "failed to roll back",
			LogKeyError, err,
			LogKeyTransaction, txName)
	}
}

func SetConnStringVariables(conn string, vars url.Values) (string, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return "", fmt.Errorf("not a valid URI: %w", err)
	}

	if u.Scheme != "postgres" {
		return "", fmt.Errorf("%q is not a postgres:// URI", conn)
	}

	q := u.Query()

	for k, v := range vars {
		q[k] = v
	}

	u.RawQuery = q.Encode()

	return u.String(), nil
}

func IsConstraintError(err error, constraint string) bool {
	if err == nil {
		return false
	}

	var pgerr *pgconn.PgError

	ok := errors.As(err, &pgerr)
	if !ok {
		return false
	}

	return pgerr.ConstraintName == constraint
}
