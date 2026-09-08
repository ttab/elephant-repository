package repository_test

import (
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	elephantrpc "github.com/ttab/elephantine/rpc"
	"github.com/ttab/elephantine/test"
)

// TestGenerationNonces pins that the nonce minted for a new generation of a
// document is a UUIDv7, and that the nonce of a generation created after an
// earlier one sorts after it. Nonces minted before v7 was introduced are
// random v4 and have no such ordering, so this only holds between generations
// that both were created after the switch.
func TestGenerationNonces(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t,
			"doc_read doc_write doc_delete eventlog_read",
			"core://unit/redaktionen"))

	ctx := t.Context()

	const (
		docUUID = "6f1c9e6a-1a4e-4a4f-9b0e-2b6a1a2f5d3c"
		docURI  = "article://test/nonce"
	)

	// Create generation A.
	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: baseDocument(docUUID, docURI),
	})
	test.Mustf(t, err, "create generation A")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "delete generation A")

	pollStart := time.Now()

	for {
		if time.Since(pollStart) > 10*time.Second {
			t.Fatal("timed out waiting for the write of generation B to succeed")
		}

		// Create generation B. The document cannot be recreated until
		// the delete has been processed.
		_, err = client.Update(ctx, &repository.UpdateRequest{
			Uuid:     docUUID,
			Document: baseDocument(docUUID, docURI),
		})
		if elephantrpc.IsCode(err, connect.CodeFailedPrecondition) {
			time.Sleep(100 * time.Millisecond)

			continue
		}

		test.Mustf(t, err, "create generation B")

		break
	}

	var (
		lastID int64
		nonces []uuid.UUID
	)

	pollStart = time.Now()

	// Read the eventlog until we have seen the nonces of both generations.
	for len(nonces) < 2 {
		if time.Since(pollStart) > 10*time.Second {
			t.Fatalf("timed out waiting for the nonces of both generations, got %d",
				len(nonces))
		}

		res, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			After:  lastID,
			WaitMs: 200,
		})
		test.Mustf(t, err, "read eventlog")

		for _, evt := range res.Items {
			lastID = evt.Id

			if evt.Uuid != docUUID || evt.DocumentNonce == "" {
				continue
			}

			nonce, err := uuid.Parse(evt.DocumentNonce)
			test.Mustf(t, err, "parse the nonce of event %d", evt.Id)

			if len(nonces) > 0 && nonces[len(nonces)-1] == nonce {
				continue
			}

			nonces = append(nonces, nonce)
		}
	}

	test.Equalf(t, 2, len(nonces),
		"expect one nonce per generation of the document")

	for i, nonce := range nonces {
		test.Equalf(t, 7, int(nonce.Version()),
			"expect the nonce of generation %d to be a UUIDv7", i+1)
	}

	// A consumer that uses the nonce in a sort key must see the newer
	// generation sort after the older one.
	if nonces[1].String() <= nonces[0].String() {
		t.Fatalf("expected the nonce of generation B (%s) to sort after the nonce of generation A (%s)",
			nonces[1], nonces[0])
	}
}
