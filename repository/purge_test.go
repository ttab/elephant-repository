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

func TestPurge(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	regenerate := regenerateTestFixtures()
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
			"doc_read doc_write doc_delete doc_restore doc_purge eventlog_read"))

	ctx := t.Context()

	const (
		docUUID = "366e9e80-1b9a-4ec6-8eed-a13922dc1e93"
		docURI  = "article://test/abc"
	)

	doc := baseDocument(docUUID, docURI)

	doc.Content = append(doc.Content, &newsdoc.Block{
		Type: "core/text",
		Role: "heading-1",
		Data: map[string]string{
			"text": "Surplus enjoyment",
		},
	})

	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "delete article")

	deletes, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "list deletes")

	test.Equal(t, 1, len(deletes.Deletes), "expect one delete record")

	deleteRec := deletes.Deletes[0]

	_, err = client.Purge(ctx, &repository.PurgeRequest{
		Uuid:           docUUID,
		DeleteRecordId: deleteRec.Id,
	})
	test.Must(t, err, "purge deleted document")

	var purgedDelete *repository.DeleteRecord

	deletePollStarted := time.Now()

	for purgedDelete == nil {
		if time.Since(deletePollStarted) > 10*time.Second {
			t.Fatal("timed out waiting for purge to finish")
		}

		// TODO: would be a good idea to be able to poll for the status
		// of purge and restore requests.
		deletes, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
			Uuid: docUUID,
		})
		test.Must(t, err, "list deletes")

		test.Equal(t, 1, len(deletes.Deletes), "expect one delete record")

		pd := deletes.Deletes[0]

		test.Equal(t, deleteRec.Id, pd.Id, "expect the same delete record")

		if pd.Purged != "" {
			purgedDelete = pd
		}
	}

	purgedPath := filepath.Join(dataDir, "purged_delete_record.json")

	if regenerate {
		it := newIncrementalTime()

		purgedDelete.Created = it.NextTimestamp(
			t, "created", purgedDelete.Created)
		purgedDelete.Finalised = it.NextTimestamp(
			t, "finalised", purgedDelete.Finalised)
		purgedDelete.Purged = it.NextTimestamp(
			t, "purged", purgedDelete.Purged)

		err := elephantine.MarshalFile(purgedPath, purgedDelete)
		test.Must(t, err, "update golden file for eventlog")
	}

	var wantDeleteRecord *repository.DeleteRecord

	err = elephantine.UnmarshalFile(purgedPath, &wantDeleteRecord)
	test.Must(t, err, "read golden file for purged delete record")

	diff := cmp.Diff(
		wantDeleteRecord, purgedDelete,
		protocmp.Transform(),
		protocmp.IgnoreFields(
			&repository.DeleteRecord{},
			"created", "finalised", "purged"),
	)
	if diff != "" {
		t.Fatalf("purged delete record mismatch (-want +got):\n%s", diff)
	}

	// Check that we can't restore a purged document.
	_, err = client.Restore(ctx, &repository.RestoreRequest{
		Uuid:           docUUID,
		DeleteRecordId: deleteRec.Id,
	})
	test.IsTwirpError(t, err, twirp.InvalidArgument)

	var (
		lastID int64
		events []*repository.EventlogItem
	)

	eventPollStarted := time.Now()

	// Read the eventlog until we're reasonably sure that we're not
	// generating surplus events.
	for {
		if time.Since(eventPollStarted) > 1*time.Second {
			break
		}

		res, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			After:  lastID,
			WaitMs: 200,
		})
		if err != nil {
			t.Fatalf("read eventlog: %v", err)
		}

		events = append(events, res.Items...)

		if len(res.Items) > 0 {
			lastID = res.Items[len(res.Items)-1].Id
		}
	}

	goldenPath := filepath.Join(dataDir, "eventlog.json")

	if regenerate {
		pureGold := make([]*repository.EventlogItem, len(events))

		it := newIncrementalTime()

		// Avoid git diff noise in the golden file when
		// regenerating. The time will be ignored in the diff test, but
		// the changed timestamps will be misleading in PRs.
		for i := range events {
			e := test.CloneMessage(events[i])

			e.Timestamp = it.NextTimestamp(
				t, "event timestamp", e.Timestamp)

			pureGold[i] = e
		}

		err := elephantine.MarshalFile(goldenPath, pureGold)
		test.Must(t, err, "update golden file for eventlog")
	}

	var wantEvents []*repository.EventlogItem

	err = elephantine.UnmarshalFile(goldenPath, &wantEvents)
	test.Must(t, err, "read golden file for eventlog")

	diff = cmp.Diff(
		&repository.GetEventlogResponse{Items: wantEvents},
		&repository.GetEventlogResponse{Items: events},
		protocmp.Transform(),
		protocmp.IgnoreFields(&repository.EventlogItem{}, "timestamp"),
	)
	if diff != "" {
		t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
	}
}

func newIncrementalTime() *incrementalTime {
	return &incrementalTime{
		t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

type incrementalTime struct {
	t time.Time
}

func (it *incrementalTime) NextTimestamp(
	t *testing.T,
	name string, original string,
) string {
	t.Helper()

	_, err := time.Parse(time.RFC3339, original)
	test.Must(t, err, "parse %s as a valid RFC3339 timestamp", name)

	it.t = it.t.Add(1 * time.Second)

	return it.t.Format(time.RFC3339)
}
