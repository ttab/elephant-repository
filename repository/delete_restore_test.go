package repository_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	elephantrpc "github.com/ttab/elephantine/rpc"
	"github.com/ttab/elephantine/test"
)

func TestDeleteRestore(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	regenerate := regenerateTestFixtures()
	dataDir := filepath.Join("..", "testdata", t.Name())

	test.Mustf(t, os.MkdirAll(dataDir, 0o700),
		"ensure that we have a test data directory")

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
	})

	scope := "doc_read doc_write doc_delete doc_restore eventlog_read"
	unit := "core://unit/redaktionen"

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t,
			scope, unit))

	clientB := tc.DocumentsClient(t,
		itest.Claims(t,
			strings.ToLower(t.Name())+"-b",
			scope, unit))

	ctx := t.Context()

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

	// Create document (gen A).
	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docA,
		Acl: []*repository.ACLEntry{
			{
				Uri:         unit,
				Permissions: []string{"r", "w"},
			},
		},
	})
	test.Mustf(t, err, "create article")

	docA2 := test.CloneMessage(docA)

	docA2.Content = append(docA2.Content, &newsdoc.Block{
		Type: "core/text",
		Data: map[string]string{
			"text": "I can't even...",
		},
	})

	// Create version 2 (gen A).
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docA2,
	})
	test.Mustf(t, err, "update article")

	// Delete gen A.
	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "delete article")

	deletesA, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "list deletes")

	test.Equalf(t, 1, len(deletesA.Deletes), "expect one delete record")

	// Prepare a document B that we want to write once the delete has been
	// processed.
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
		if time.Since(pollStart) > 2*time.Second {
			t.Fatal("timed out waiting for write of generation B to succeed")
		}

		// Create document (gen B).
		_, err = client.Update(ctx, &repository.UpdateRequest{
			Uuid:     docUUID,
			Document: docB,
		})

		switch {
		// The document is not in a state to be recreated until the
		// delete has been processed.
		case elephantrpc.IsCode(err, connect.CodeFailedPrecondition):
			time.Sleep(100 * time.Millisecond)
		case err != nil:
			t.Fatalf("unexpected error when creation generation B of the doc: %v", err)
		default:
			genBCreated = true
		}
	}

	docB2 := test.CloneMessage(docB)

	docB2.Language = "en-gb"

	docB2.Content = append(docB2.Content, &newsdoc.Block{
		Type: "core/text",
		Data: map[string]string{
			"text": "Because I don't accept the current state of affairs as the natural order of things.",
		},
	})

	// Create version 2 (gen B) with language changed to en-gb, and setting
	// v1 as done and v2 as usable.
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docB2,
		Status: []*repository.StatusUpdate{
			{Name: "done", Version: 1},
			{Name: "usable"},
		},
	})
	test.Mustf(t, err, "update generation B")

	metaB, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get meta for generation B")

	if metaB.Meta.RestoredFromNonce != "" ||
		metaB.Meta.RestoredFromDeleteRecordId != 0 {
		t.Fatalf("generation B wasn't restored, but its meta says it was restored from %q (record %d)",
			metaB.Meta.RestoredFromNonce,
			metaB.Meta.RestoredFromDeleteRecordId)
	}

	// Attempt to restore when a document exists. Here we expect failure.
	_, err = client.Restore(ctx, &repository.RestoreRequest{
		Uuid:           docUUID,
		DeleteRecordId: deletesA.Deletes[0].Id,
	})
	test.IsRPCError(t, err, connect.CodeAlreadyExists)

	// Delete gen B.
	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "delete article the second time")

	deletesB, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "list deletes")

	test.Equalf(t, 2, len(deletesB.Deletes), "expect two delete records")

	// Pick out the last deleted document version (gen B).
	record := deletesB.Deletes[0]

	test.Equalf(t, 2, record.Id, "expect the first record to have the ID 2")

	var restoreStarted bool

	pollStart = time.Now()

	for !restoreStarted {
		if time.Since(pollStart) > 2*time.Second {
			t.Fatal("timed out waiting for document to become readable")
		}

		// Restore gen B as client B.
		_, err = clientB.Restore(ctx, &repository.RestoreRequest{
			Uuid:           docUUID,
			DeleteRecordId: record.Id,
			Acl: []*repository.ACLEntry{
				{
					Uri:         "core://unit/redaktionen",
					Permissions: []string{"r", "w"},
				},
			},
		})
		if elephantrpc.IsCode(err, connect.CodeFailedPrecondition) {
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

	pollStart = time.Now()

	for doc == nil {
		if time.Since(pollStart) > 2*time.Second {
			t.Fatal("timed out waiting for document to become readable")
		}

		// Read the current version of the document.
		res, err := client.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docUUID,
		})

		switch {
		case elephantrpc.IsCode(err, connect.CodeFailedPrecondition):
			time.Sleep(100 * time.Millisecond)

			// Restore is still processing
			continue
		case err != nil:
			t.Fatalf("failed to read restored document: %v", err)
		}

		doc = res.Document
	}

	test.EqualMessagef(t, docB2, doc,
		"expect to get a document that is equal to v2 of generation B of the document")

	var (
		lastID      int64
		gotFinEvent bool
		events      []*repository.EventlogItem
	)

	pollStart = time.Now()

	// Read the eventlog until we get a "restore_finished" event.
	for !gotFinEvent {
		if time.Since(pollStart) > 10*time.Second {
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

	// The events of the restore, everything after the delete of gen B,
	// name the generation and delete record they were restored from. No
	// event before them does.
	var restoredFrom string

	for _, evt := range events {
		if restoredFrom == "" {
			if evt.RestoredFromNonce != "" ||
				evt.RestoredFromDeleteRecordId != 0 {
				t.Errorf("event %d (%s) isn't part of a restore, but names %q (record %d)",
					evt.Id, evt.Event, evt.RestoredFromNonce,
					evt.RestoredFromDeleteRecordId)
			}

			if evt.Event == "delete_document" &&
				evt.DeleteRecordId == record.Id {
				restoredFrom = evt.DocumentNonce
			}

			continue
		}

		test.Equalf(t, restoredFrom, evt.RestoredFromNonce,
			"event %d (%s) names generation B's nonce", evt.Id, evt.Event)
		test.Equalf(t, record.Id, evt.RestoredFromDeleteRecordId,
			"event %d (%s) names generation B's delete record", evt.Id, evt.Event)

		if evt.DocumentNonce == restoredFrom {
			t.Errorf("event %d (%s) has the old generation's nonce as its document nonce",
				evt.Id, evt.Event)
		}
	}

	if restoredFrom == "" {
		t.Fatal("found no delete event for generation B in the eventlog")
	}

	metaRes, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get meta for the restored document")

	test.Equalf(t, restoredFrom, metaRes.Meta.RestoredFromNonce,
		"the meta names generation B's nonce")
	test.Equalf(t, record.Id, metaRes.Meta.RestoredFromDeleteRecordId,
		"the meta names generation B's delete record")

	goldenPath := filepath.Join(dataDir, "eventlog.json")

	test.MessageAgainstGolden(
		t, regenerate,
		&repository.GetEventlogResponse{
			Items: events,
		},
		goldenPath,
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"),
		ignoreUUIDField("restored_from_nonce"),
	)

	historyRes, err := client.GetHistory(ctx, &repository.GetHistoryRequest{
		Uuid:         docUUID,
		LoadStatuses: true,
	})
	test.Mustf(t, err, "get document history")

	historyGoldenPath := filepath.Join(dataDir, "history.json")

	test.MessageAgainstGolden(
		t, regenerate, historyRes, historyGoldenPath,
		test.IgnoreTimestamps{
			Fields: []string{"created", "restored_at"},
		},
	)
}
