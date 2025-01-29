package repository_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-repository/internal"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine/test"
)

func TestIntegrationWorkflows(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	regenerate := os.Getenv("REGENERATE") == "true"

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunReplicator: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))
	wflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	ctx := test.Context(t)

	_, err := wflowClient.SetWorkflow(ctx, &repository.SetWorkflowRequest{
		Type: "core/article",
		Workflow: &repository.DocumentWorkflow{
			StepZero:           "draft",
			Checkpoint:         "usable",
			NegativeCheckpoint: "unpublished",
			Steps:              []string{"draft", "done", "approved", "withheld"},
		},
	})
	test.Must(t, err, "create workflow")

	waitDeadline := time.Now().Add(5 * time.Second)

	for {
		if time.Now().After(waitDeadline) {
			t.Fatal("timed out waiting for workflow to kick in")
		}

		_, exists := tc.WorkflowProvider.GetDocumentWorkflow("core/article")
		if exists {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	docRes, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "done", Version: docRes.Version},
		},
	})
	test.Must(t, err, "set done status")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "update article")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "approved", Version: docRes.Version},
		},
	})
	test.Must(t, err, "set approved status")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: docRes.Version},
		},
	})
	test.Must(t, err, "set usable status")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "update article after usable")

	events := collectEventlog(t, client, 12, 5*time.Second)
	eventsGolden := filepath.Join("testdata", t.Name(), "events.json")

	test.TestMessageAgainstGolden(t, regenerate, events, eventsGolden,
		ignoreCommonTimestamps())

	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get document meta")

	metaGolden := filepath.Join("testdata", t.Name(), "meta.json")

	test.TestMessageAgainstGolden(t, regenerate, meta, metaGolden,
		ignoreCommonTimestamps())
}

func ignoreCommonTimestamps() cmp.Option {
	return cmpopts.IgnoreMapEntries(func(k string, _ interface{}) bool {
		return k == createdField || k == modifiedField || k == timestampField
	})
}

func collectEventlog(
	t *testing.T, client repository.Documents,
	minCount int, timeout time.Duration,
) *repository.GetEventlogResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var (
		lastID    int64
		collected []*repository.EventlogItem
	)

	for len(collected) < minCount {
		events, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			// Overfetch by one so that we have a chance of getting
			// any extraneous events.
			BatchSize:   internal.MustInt32(minCount - len(collected) + 1),
			BatchWaitMs: 200,
			After:       lastID,
		})
		test.Must(t, err, "get eventlog")

		collected = append(collected, events.Items...)

		if len(events.Items) > 0 {
			last := len(events.Items) - 1
			lastID = events.Items[last].Id
		}
	}

	return &repository.GetEventlogResponse{
		Items: collected,
	}
}
