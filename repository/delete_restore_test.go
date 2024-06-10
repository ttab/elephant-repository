package repository_test

import (
	"log/slog"
	"testing"

	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine/test"
)

func TestDeleteRestore(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:   true,
		RunReplicator: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := test.Context(t)
}
