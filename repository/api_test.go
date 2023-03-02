package repository_test

import (
	"strings"
	"testing"
	"time"

	"github.com/ttab/elephant/internal/test"
	rpc "github.com/ttab/elephant/rpc/repository"
	"golang.org/x/exp/slog"
)

func baseDocument(uuid, uri string) *rpc.Document {
	return &rpc.Document{
		Uuid:  uuid,
		Title: "A bare-bones article",
		Type:  "core/article",
		Uri:   uri,
	}
}

func TestIntegrationBasicCrud(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	// TODO: We don't drain the archiver before finishing tests, so
	// sometimes an archiver related error will be logged out of band.
	tc := testingAPIServer(t, logger, true)

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	ctx := test.Context(t)

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	res, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	test.Equal(t, 1, res.Version, "expected this to be the first version")

	doc2 := test.CloneMessage(doc)

	doc2.Content = append(doc2.Content, &rpc.Block{
		Type: "core/heading-1",
		Data: map[string]string{
			"text": "The headline of the year",
		},
	})

	res, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc2,
	})
	test.Must(t, err, "update article")

	test.Equal(t, 2, res.Version, "expected this to be the second version")

	doc3 := test.CloneMessage(doc2)

	doc3.Content = append(doc3.Content, &rpc.Block{
		Type: "something/made-up",
		Data: map[string]string{
			"text": "Dunno what this is",
		},
	})

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc3,
	})
	test.MustNot(t, err, "expected unknown content block to fail validation")

	currentVersion, err := client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get the document")

	test.EqualMessage(t, doc2, currentVersion.Document,
		"expected the last document to be returned")

	firstVersion, err := client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:    docUUID,
		Version: 1,
	})
	test.Must(t, err, "get the first version of the document")

	test.EqualMessage(t, doc, firstVersion.Document,
		"expected the first document to be returned")

	_, err = client.Delete(ctx, &rpc.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "delete the document")

	_, err = client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "expected get to fail after delete")

	_, err = client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:    docUUID,
		Version: 1,
	})
	test.MustNot(t, err, "expected get of old version to fail after delete")
}

func TestIntegrationStatuses(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	ctx := test.Context(t)
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	tc := testingAPIServer(t, logger, false)

	workflowClient := tc.WorkflowsClient(t,
		test.StandardClaims(t, "workflow_admin"))

	_, err := workflowClient.UpdateStatus(ctx, &rpc.UpdateStatusRequest{
		Name: "usable",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &rpc.UpdateStatusRequest{
		Name: "done",
	})
	test.Must(t, err, "create done status")

	t0 := time.Now()
	workflowDeadline := time.After(1 * time.Second)

	// Wait until the workflow provider notices the change.
	for {
		if tc.WorkflowProvider.HasStatus("done") {
			t.Logf("had to wait %v for workflow upate",
				time.Since(t0))

			break
		}

		select {
		case <-workflowDeadline:
			t.Fatal("workflow didn't get updated in time")
		case <-time.After(1 * time.Millisecond):
		}
	}

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		Status: []*rpc.StatusUpdate{
			{Name: "usable"},
		},
	})
	test.Must(t, err, "create article")

	doc2 := test.CloneMessage(doc)
	doc2.Title = "Drafty McDraftface"

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc2,
	})
	test.Must(t, err, "update article")

	doc3 := test.CloneMessage(doc2)
	doc3.Title = "More appropriate title"

	res3, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc3,
		Status: []*rpc.StatusUpdate{
			{Name: "done"},
		},
	})
	test.Must(t, err, "update article with 'done' status")

	currentUsable, err := client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:   docUUID,
		Status: "usable",
	})
	test.Must(t, err, "fetch the currently published version")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "whatchacallit", Version: res3.Version},
		},
	})
	test.MustNot(t, err, "should fail to use unknown status")

	test.EqualMessage(t,
		&rpc.GetDocumentResponse{
			Version:  1,
			Document: doc,
		}, currentUsable,
		"expected the currently publised version to be unchanged")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: res3.Version},
		},
	})
	test.Must(t, err, "set version 3 to usable")
}

func TestIntegrationStatusRules(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	ctx := test.Context(t)
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	tc := testingAPIServer(t, logger, false)

	workflowClient := tc.WorkflowsClient(t,
		test.StandardClaims(t, "workflow_admin"))

	const (
		approvalName       = "must-be-approved"
		requirePublishName = "require-publish-scope"
		requireLegalName   = "require-legal-approval"
	)

	_, err := workflowClient.CreateStatusRule(ctx, &rpc.CreateStatusRuleRequest{
		Rule: &rpc.StatusRule{
			Name:        approvalName,
			Description: "Articles must be approved before they're published",
			Expression:  `Heads.approved.Version == Status.Version`,
			AppliesTo:   []string{"usable"},
			ForTypes:    []string{"core/article"},
		},
	})
	test.Must(t, err, "create approval rule")

	_, err = workflowClient.CreateStatusRule(ctx, &rpc.CreateStatusRuleRequest{
		Rule: &rpc.StatusRule{
			Name:        requireLegalName,
			Description: "Require legal signoff if requested",
			Expression: `
Heads.approved.Meta.legal_approval != "required"
or Heads.approved_legal.Version == Status.Version`,
			AppliesTo: []string{"usable"},
			ForTypes:  []string{"core/article"},
		},
	})
	test.Must(t, err, "create legal approval rule")

	_, err = workflowClient.CreateStatusRule(ctx, &rpc.CreateStatusRuleRequest{
		Rule: &rpc.StatusRule{
			Name:        "require-publish-scope",
			Description: "publish scope is required for setting usable",
			Expression:  `User.HasScope("publish")`,
			AccessRule:  true,
			AppliesTo:   []string{"usable"},
			ForTypes:    []string{"core/article"},
		},
	})
	test.Must(t, err, "create publish permission rule")

	_, err = workflowClient.UpdateStatus(ctx, &rpc.UpdateStatusRequest{
		Name: "approved_legal",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &rpc.UpdateStatusRequest{
		Name: "usable",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &rpc.UpdateStatusRequest{
		Name: "approved",
	})
	test.Must(t, err, "create approved status")

	t0 := time.Now()
	workflowDeadline := time.After(1 * time.Second)

	// Wait until the workflow provider notices the change.
	for {
		if tc.WorkflowProvider.HasStatus("approved") {
			t.Logf("had to wait %v for workflow upate",
				time.Since(t0))

			break
		}

		select {
		case <-workflowDeadline:
			t.Fatal("workflow didn't get updated in time")
		case <-time.After(1 * time.Millisecond):
		}
	}

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	editorClient := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete publish"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	createdRes, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	_, err = editorClient.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: createdRes.Version},
		},
	})
	test.MustNot(t, err, "don't publish document without approval")

	if !strings.Contains(err.Error(), approvalName) {
		t.Fatalf("error message should mention %q, got: %s",
			approvalName, err.Error())
	}

	_, err = editorClient.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "approved", Version: createdRes.Version},
		},
	})
	test.Must(t, err, "approve document")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: createdRes.Version},
		},
	})
	test.MustNot(t, err, "don't allow publish without the correct scope")

	if !strings.Contains(err.Error(), requirePublishName) {
		t.Fatalf("error message should mention %q, got: %s",
			requirePublishName, err.Error())
	}

	_, err = editorClient.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: createdRes.Version},
		},
	})
	test.Must(t, err, "publish the document")

	docV2 := test.CloneMessage(doc)

	docV2.Content = append(docV2.Content, &rpc.Block{
		Type: "core/paragraph",
		Data: map[string]string{
			"text": "Some sensitive stuff",
		},
	})

	updatedRes, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: docV2,
		Status: []*rpc.StatusUpdate{
			{
				Name: "approved",
				Meta: map[string]string{
					"legal_approval": "required",
				},
			},
		},
	})
	test.Must(t, err, "update article")

	_, err = editorClient.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: updatedRes.Version},
		},
	})
	test.MustNot(t, err,
		"should not publish the second version without legal approval")

	if !strings.Contains(err.Error(), requireLegalName) {
		t.Fatalf("error message should mention %q, got: %s",
			requireLegalName, err.Error())
	}

	_, err = editorClient.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "approved_legal", Version: updatedRes.Version},
		},
	})
	test.Must(t, err, "set the legal approval status")

	_, err = editorClient.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: updatedRes.Version},
		},
	})
	test.Must(t, err,
		"should publish the second version now that legal has approved")
}

func TestIntegrationACL(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	ctx := test.Context(t)
	tc := testingAPIServer(t, logger, false)

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	otherClaims := test.StandardClaims(t,
		"doc_read doc_write doc_delete",
		"unit://some/group")

	otherClaims.Subject = "user://test/other-user"

	otherClient := tc.DocumentsClient(t, otherClaims)

	const (
		docUUID  = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI   = "article://test/123"
		doc2UUID = "93bd1a10-8136-4ace-8722-f4cc10bdb3e9"
		doc2URI  = "article://test/123-b"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create first article")

	doc2 := baseDocument(doc2UUID, doc2URI)

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
		Acl: []*rpc.ACLEntry{
			{
				Uri:         "user://test/other-user",
				Permissions: []string{"r"},
			},
		},
	})
	test.Must(t, err, "create second article")

	_, err = otherClient.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "didn't expect the other user to have read access")

	_, err = otherClient.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: doc2UUID,
	})
	test.Must(t, err, "expected the other user to have access to document two")

	doc2.Title = "A better title, clearly"

	_, err = otherClient.Update(ctx, &rpc.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
	})
	test.MustNot(t, err,
		"didn't expect the other user to have write acces to document two")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Acl: []*rpc.ACLEntry{
			{
				Uri:         "unit://some/group",
				Permissions: []string{"r", "w"},
			},
		},
	})
	test.Must(t, err, "update ACL for document one")

	_, err = otherClient.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "expected other user to be able to read document one after ACL update")

	doc.Title = "The first doc is the best doc"

	_, err = otherClient.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "expected other user to be able to update document one after ACL update")
}
