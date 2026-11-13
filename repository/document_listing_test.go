package repository_test

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine/test"
)

const (
	listDocPublishedUUID   = "10000000-0000-4000-8000-000000000000"
	listDocUnpublishedUUID = "20000000-0000-4000-8000-000000000000"
	listDocDraftUUID       = "30000000-0000-4000-8000-000000000000"
	listDocDeletedUUID     = "40000000-0000-4000-8000-000000000000"
	listDocPrivateUUID     = "50000000-0000-4000-8000-000000000000"

	listReaderUnit = "unit://test/listing-readers"
)

// listFixture sets up articles under a workflow with the checkpoint "usable"
// and the negative checkpoint "unpublished":
//
//   - published: v1 published as usable, then a v2 written.
//   - unpublished: v1 published as usable, then unpublished.
//   - draft: v1, never published.
//   - deleted: v1 published as usable, then deleted. Delete waits for the
//     archiver, which finalises the delete in the background, so the
//     document is either a "deleting" placeholder or gone when the tests
//     list.
//   - private: v1 published as usable, readable only by its creator.
//
// Every document but the private one is readable by listReaderUnit.
func listFixture(
	t *testing.T, opts testingServerOptions,
) (TestContext, repository.Documents) {
	t.Helper()

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	opts.RunArchiver = true
	opts.RunEventlogBuilder = true

	tc := testingAPIServer(t, logger, opts)

	workflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	_, err := workflowClient.SetWorkflow(ctx, &repository.SetWorkflowRequest{
		Type: "core/article",
		Workflow: &repository.DocumentWorkflow{
			StepZero:           "draft",
			Checkpoint:         "usable",
			NegativeCheckpoint: "unpublished",
			Steps:              []string{"draft", "done"},
		},
	})
	test.Mustf(t, err, "create article workflow")

	deadline := time.After(2 * time.Second)

	for {
		wf, ok := tc.WorkflowProvider.GetDocumentWorkflow("core/article")
		if ok && wf.Configuration.NegativeCheckpoint == "unpublished" {
			break
		}

		select {
		case <-deadline:
			t.Fatal("workflow didn't propagate in time")
		case <-time.After(5 * time.Millisecond):
		}
	}

	claims := itest.StandardClaims(t, "doc_read doc_write doc_delete")
	client := tc.DocumentsClient(t, claims)

	readers := []*repository.ACLEntry{
		{Uri: claims.Subject, Permissions: []string{"r", "w"}},
		{Uri: listReaderUnit, Permissions: []string{"r"}},
	}

	create := func(id string, acl []*repository.ACLEntry, publish bool) {
		t.Helper()

		req := repository.UpdateRequest{
			Uuid:     id,
			Document: baseDocument(id, "article://test/listing/"+id),
			Acl:      acl,
		}

		if publish {
			req.Status = []*repository.StatusUpdate{{Name: "usable"}}
		}

		_, err := client.Update(ctx, &req)
		test.Mustf(t, err, "create document %s", id)
	}

	create(listDocPublishedUUID, readers, true)
	create(listDocUnpublishedUUID, readers, true)
	create(listDocDraftUUID, readers, false)
	create(listDocDeletedUUID, readers, true)
	create(listDocPrivateUUID, nil, true)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: listDocPublishedUUID,
		Document: baseDocument(listDocPublishedUUID,
			"article://test/listing/"+listDocPublishedUUID),
	})
	test.Mustf(t, err, "write a second version")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: listDocUnpublishedUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: -1},
		},
	})
	test.Mustf(t, err, "unpublish document")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: listDocDeletedUUID,
	})
	test.Mustf(t, err, "delete document")

	return tc, client
}

// listKey describes an item as "<selector>:<uuid>@v<version>".
func listKey(item *repository.ListDocumentsItem) string {
	return fmt.Sprintf("%d:%s@v%d", item.Selector, item.Uuid, item.Version)
}

// listAll pages through a listing and returns the items of each page.
func listAll(
	t *testing.T, client repository.Documents,
	req *repository.ListDocumentsRequest,
) [][]*repository.ListDocumentsItem {
	t.Helper()

	var pages [][]*repository.ListDocumentsItem

	for {
		res, err := client.ListDocuments(t.Context(), req)
		test.Mustf(t, err, "list documents, page %d", len(pages)+1)

		pages = append(pages, res.Items)

		if res.Next == "" {
			return pages
		}

		if len(pages) > 20 {
			t.Fatal("the listing doesn't end")
		}

		req.After = res.Next
	}
}

func listKeys(pages [][]*repository.ListDocumentsItem) []string {
	var keys []string

	for _, page := range pages {
		for _, item := range page {
			keys = append(keys, listKey(item))
		}
	}

	return keys
}

func TestIntegrationListDocuments(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	tc, client := listFixture(t, testingServerOptions{})
	ctx := t.Context()

	cases := []struct {
		Name     string
		Selector *repository.ListDocumentsSelector
		Version  repository.ListVersion
		Want     []string
	}{
		{
			Name:     "TypeSelected",
			Selector: &repository.ListDocumentsSelector{Type: "core/article"},
			Want: []string{
				"0:" + listDocPublishedUUID + "@v2",
				"0:" + listDocUnpublishedUUID + "@v1",
				"0:" + listDocDraftUUID + "@v1",
				"0:" + listDocPrivateUUID + "@v1",
			},
		},
		{
			Name: "StatusSelected",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", Status: "usable",
			},
			Want: []string{
				"0:" + listDocPublishedUUID + "@v1",
				"0:" + listDocPrivateUUID + "@v1",
			},
		},
		{
			Name: "StatusCurrent",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", Status: "usable",
			},
			Version: repository.ListVersion_LIST_VERSION_CURRENT,
			Want: []string{
				"0:" + listDocPublishedUUID + "@v2",
				"0:" + listDocPrivateUUID + "@v1",
			},
		},
		{
			Name: "CheckpointSelected",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", WorkflowCheckpoint: "usable",
			},
			Want: []string{
				"0:" + listDocPublishedUUID + "@v1",
				"0:" + listDocPrivateUUID + "@v1",
			},
		},
		{
			Name: "CheckpointCurrent",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", WorkflowCheckpoint: "usable",
			},
			Version: repository.ListVersion_LIST_VERSION_CURRENT,
			Want: []string{
				"0:" + listDocPublishedUUID + "@v2",
				"0:" + listDocPrivateUUID + "@v1",
			},
		},
		{
			Name: "NegativeCheckpointCurrent",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", WorkflowCheckpoint: "unpublished",
			},
			Version: repository.ListVersion_LIST_VERSION_CURRENT,
			Want: []string{
				"0:" + listDocUnpublishedUUID + "@v1",
			},
		},
		{
			Name: "UnknownCheckpoint",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", WorkflowCheckpoint: "nonesuch",
			},
		},
		{
			Name: "UnknownStatus",
			Selector: &repository.ListDocumentsSelector{
				Type: "core/article", Status: "nonesuch",
			},
		},
		{
			Name:     "OtherType",
			Selector: &repository.ListDocumentsSelector{Type: "core/planning-item"},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			pages := listAll(t, client, &repository.ListDocumentsRequest{
				Selectors: []*repository.ListDocumentsSelector{c.Selector},
				Version:   c.Version,
			})

			test.EqualDiff(t, c.Want, listKeys(pages),
				"listed documents")
		})
	}

	t.Run("NegativeCheckpointSelected", func(t *testing.T) {
		_, err := client.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{{
				Type: "core/article", WorkflowCheckpoint: "unpublished",
			}},
		})
		test.IsRPCError(t, err, connect.CodeInvalidArgument)
	})

	t.Run("StatusAndCheckpoint", func(t *testing.T) {
		_, err := client.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{{
				Type:               "core/article",
				Status:             "usable",
				WorkflowCheckpoint: "usable",
			}},
			Version: repository.ListVersion_LIST_VERSION_CURRENT,
		})
		test.IsRPCError(t, err, connect.CodeInvalidArgument)
	})

	t.Run("NoSelectors", func(t *testing.T) {
		_, err := client.ListDocuments(ctx, &repository.ListDocumentsRequest{})
		test.IsRPCError(t, err, connect.CodeInvalidArgument)
	})

	t.Run("InvalidCursor", func(t *testing.T) {
		_, err := client.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article"},
			},
			After: "not-a-cursor",
		})
		test.IsRPCError(t, err, connect.CodeInvalidArgument)
	})

	t.Run("CursorAcrossSelectors", func(t *testing.T) {
		pages := listAll(t, client, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article", Status: "usable"},
				{Type: "core/article"},
			},
			Limit: 3,
		})

		test.Equalf(t, 3, len(pages[0]), "documents on the first page")

		test.EqualDiff(t, []string{
			"0:" + listDocPublishedUUID + "@v1",
			"0:" + listDocPrivateUUID + "@v1",
			"1:" + listDocPublishedUUID + "@v2",
			"1:" + listDocUnpublishedUUID + "@v1",
			"1:" + listDocDraftUUID + "@v1",
			"1:" + listDocPrivateUUID + "@v1",
		}, listKeys(pages), "listed documents")
	})

	t.Run("Subset", func(t *testing.T) {
		res, err := client.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article", Status: "usable"},
			},
			Subset: []string{".meta(type='core/newsvalue')@{value}"},
		})
		test.Mustf(t, err, "list with subset")

		test.Equalf(t, 2, len(res.Items), "listed documents")

		for i, item := range res.Items {
			if item.Document != nil {
				t.Fatalf("item %d: expected no document", i)
			}

			if len(item.Subset) != 1 {
				t.Fatalf("item %d: expected 1 subset entry, got %d",
					i, len(item.Subset))
			}

			test.Equalf(t, "3", item.Subset[0].Values["value"].GetValue(),
				"item %d: extracted newsvalue", i)
		}
	})

	t.Run("Meta", func(t *testing.T) {
		res, err := client.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article", Status: "usable"},
			},
			Limit: 1,
		})
		test.Mustf(t, err, "list without meta")

		if res.Items[0].Meta != nil {
			t.Fatal("expected no meta when not asked for")
		}

		res, err = client.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article", Status: "usable"},
			},
			IncludeMeta: true,
			Limit:       1,
		})
		test.Mustf(t, err, "list with meta")

		item := res.Items[0]

		test.Equalf(t, listDocPublishedUUID, item.Uuid, "listed document")
		test.Equalf(t, int64(1), item.Version, "returned version")
		test.Equalf(t, "core/article", item.Type, "returned type")

		if item.Document == nil {
			t.Fatal("expected the document")
		}

		// The meta describes the document as it is now, not the
		// returned version.
		test.Equalf(t, int64(2), item.Meta.GetCurrentVersion(),
			"current version in meta")
		test.Equalf(t, int64(1), item.Meta.GetHeads()["usable"].GetVersion(),
			"usable head in meta")
	})

	t.Run("RestrictedReader", func(t *testing.T) {
		claims := itest.Claims(t, "listing-reader", "doc_read",
			listReaderUnit)

		reader := tc.DocumentsClient(t, claims)

		pages := listAll(t, reader, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article"},
			},
		})

		test.EqualDiff(t, []string{
			"0:" + listDocPublishedUUID + "@v2",
			"0:" + listDocUnpublishedUUID + "@v1",
			"0:" + listDocDraftUUID + "@v1",
		}, listKeys(pages), "listed documents")
	})

	t.Run("ReadAllScope", func(t *testing.T) {
		reader := tc.DocumentsClient(t,
			itest.Claims(t, "listing-admin", "doc_read_all"))

		pages := listAll(t, reader, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article"},
			},
		})

		test.Equalf(t, 4, len(listKeys(pages)), "listed documents")
	})

	t.Run("MissingScope", func(t *testing.T) {
		writer := tc.DocumentsClient(t,
			itest.Claims(t, "listing-writer", "doc_write"))

		_, err := writer.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article"},
			},
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})

	t.Run("DocumentRestrictedScope", func(t *testing.T) {
		restricted := tc.DocumentsClient(t, itest.Claims(t,
			"listing-device", "doc_read:"+listDocPublishedUUID,
			listReaderUnit))

		_, err := restricted.ListDocuments(ctx, &repository.ListDocumentsRequest{
			Selectors: []*repository.ListDocumentsSelector{
				{Type: "core/article"},
			},
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})
}

func TestIntegrationListDocumentsByteCap(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// A cap of one byte lets every page hold exactly one document, since a
	// page always holds at least one.
	_, client := listFixture(t, testingServerOptions{
		ListDocumentsMaxBytes: 1,
	})

	pages := listAll(t, client, &repository.ListDocumentsRequest{
		Selectors: []*repository.ListDocumentsSelector{
			{Type: "core/article", Status: "usable"},
			{Type: "core/article"},
		},
	})

	for i, page := range pages[:len(pages)-1] {
		test.Equalf(t, 1, len(page), "documents on page %d", i+1)
	}

	test.EqualDiff(t, []string{
		"0:" + listDocPublishedUUID + "@v1",
		"0:" + listDocPrivateUUID + "@v1",
		"1:" + listDocPublishedUUID + "@v2",
		"1:" + listDocUnpublishedUUID + "@v1",
		"1:" + listDocDraftUUID + "@v1",
		"1:" + listDocPrivateUUID + "@v1",
	}, listKeys(pages), "listed documents")
}
