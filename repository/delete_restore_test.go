package repository_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestDeleteRestore(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	regenerate := os.Getenv("REGENERATE") == "true"
	dataDir := filepath.Join("..", "testdata", t.Name())

	test.Must(t, os.MkdirAll(dataDir, 0o700),
		"ensure that we have a test data directory")

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:   true,
		RunReplicator: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t,
			"doc_read doc_write doc_delete doc_restore eventlog_read",
			"core://unit/redaktionen"))

	ctx := test.Context(t)

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	docA := baseDocument(docUUID, docURI)

	docA.Content = append(docA.Content, &newsdoc.Block{
		Type: "core/text",
		Role: "heading-1",
		Data: map[string]string{
			"text": "Capitalist realism: a decade in review",
		},
	})

	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docA,
	})
	test.Must(t, err, "create article")

	docA2 := test.CloneMessage(docA)

	docA2.Content = append(docA2.Content, &newsdoc.Block{
		Type: "core/text",
		Data: map[string]string{
			"text": "I can't even...",
		},
	})

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docA2,
	})
	test.Must(t, err, "update article")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "delete article")

	deletesA, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "list deletes")

	test.Equal(t, 1, len(deletesA.Deletes), "expect one delete record")

	docB := test.CloneMessage(docA)

	docB.Content = append(docB.Content, &newsdoc.Block{
		Type: "core/text",
		Data: map[string]string{
			"text": "I can imagine a future.",
		},
	})

	pollStart := time.Now()

	var genBCreated bool

	for !genBCreated {
		time.Sleep(100 * time.Millisecond)

		_, err = client.Update(ctx, &repository.UpdateRequest{
			Uuid:     docUUID,
			Document: docB,
		})

		switch {
		// The document is not in a state to be recreated until the
		// delete has been processed.
		case elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition):
		case err != nil:
			t.Fatalf("unexpected error when creation generation B of the doc: %v", err)
		case time.Since(pollStart) > 10*time.Second:
			t.Fatal("timed out waiting for write of generation B to succeed")
		default:
			genBCreated = true

			continue
		}
	}

	docB2 := test.CloneMessage(docB)

	docB2.Language = "en-gb"

	docB2.Content = append(docB.Content, &newsdoc.Block{
		Type: "core/text",
		Data: map[string]string{
			"text": "Because I don't accept the current state of affairs as the natural order of things.",
		},
	})

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docB2,
		Status: []*repository.StatusUpdate{
			{Name: "usable"},
			{Name: "done", Version: 1},
		},
	})
	test.Must(t, err, "update generation B")

	// Attempt to restore when a document exists. Here we expect failure.
	_, err = client.Restore(ctx, &repository.RestoreRequest{
		Uuid:           docUUID,
		DeleteRecordId: deletesA.Deletes[0].Id,
		Acl: []*repository.ACLEntry{
			{
				Uri:         "core://unit/redaktionen",
				Permissions: []string{"r", "w"},
			},
		},
	})
	test.IsTwirpError(t, err, twirp.AlreadyExists)

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "delete article the second time")

	deletesB, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "list deletes")

	test.Equal(t, 2, len(deletesB.Deletes), "expect two delete records")

	record := deletesA.Deletes[0]

	var restoreStarted bool

	for !restoreStarted {
		_, err = client.Restore(ctx, &repository.RestoreRequest{
			Uuid:           docUUID,
			DeleteRecordId: record.Id,
			Acl: []*repository.ACLEntry{
				{Permissions: []string{"r", "w"}},
			},
		})
		if elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition) {
			// This means that the delete hasn't finished
			// processing, and therefore cannot be restored.
			time.Sleep(100 * time.Millisecond)

			continue
		} else if err != nil {
			t.Fatalf("failed to start restore: %v", err)
		}

		restoreStarted = true
	}

	var doc *newsdoc.Document

	for doc == nil {
		res, err := client.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docUUID,
		})

		switch {
		case elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition):
			// Restore is still processing
			continue
		case err != nil:
			t.Fatalf("failed to read restored document: %v", err)
		}

		doc = res.Document
	}

	test.EqualMessage(t, docB2, doc,
		"expect to get a document that is equal to v2 of generation B of the document")

	var (
		lastID      int64
		gotFinEvent bool
		events      []*repository.EventlogItem
	)

	eventPollStarted := time.Now()

	for !gotFinEvent {
		if time.Since(eventPollStarted) > 10*time.Second {
			t.Fatal("timed out waiting for restore finished event")
		}

		res, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			After:  lastID,
			WaitMs: 200,
		})
		if err != nil {
			t.Fatalf("read eventlog: %v", err)
		}

		for _, evt := range res.Items {
			gotFinEvent = gotFinEvent || evt.Event == "restore_finished"

			events = append(events, evt)

			lastID = evt.Id
		}
	}

	goldenPath := filepath.Join(dataDir, "eventlog.json")

	if regenerate {
		err := elephantine.MarshalFile(goldenPath, events)
		test.Must(t, err, "update golden file for eventlog")
	}

	var wantEvents []*repository.EventlogItem

	err = elephantine.UnmarshalFile(goldenPath, &wantEvents)
	test.Must(t, err, "read golden file for eventlog")

	diff := cmp.Diff(
		&repository.GetEventlogResponse{Items: wantEvents},
		&repository.GetEventlogResponse{Items: events},
		protocmp.Transform(),
		protocmp.IgnoreFields(&repository.EventlogItem{}, "timestamp"),
	)
	if diff != "" {
		t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
	}
}
