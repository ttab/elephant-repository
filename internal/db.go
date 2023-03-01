package internal

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/slog"
)

func PGTime(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{
		Time:  t,
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
		logger.Error("failed to roll back", err,
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
