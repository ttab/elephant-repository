package repository_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant/internal/test"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/testing/protocmp"
)

func baseDocument(uuid, uri string) *newsdoc.Document {
	return &newsdoc.Document{
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

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:   true,
		RunReplicator: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := test.Context(t)

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	res, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	test.Equal(t, 1, res.Version, "expected this to be the first version")

	doc2 := test.CloneMessage(doc)

	doc2.Content = append(doc2.Content, &newsdoc.Block{
		Type: "core/heading-1",
		Data: map[string]string{
			"text": "The headline of the year",
		},
	})

	res, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc2,
	})
	test.Must(t, err, "update article")

	test.Equal(t, 2, res.Version, "expected this to be the second version")

	docTypeShift := newsdoc.Document{
		Type: "core/place",
		Uri:  doc2.Uri,
	}

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: &docTypeShift,
	})
	test.MustNot(t, err, "expected type change to be disallowed")

	docBadBlock := test.CloneMessage(doc2)

	docBadBlock.Content = append(docBadBlock.Content, &newsdoc.Block{
		Type: "something/made-up",
		Data: map[string]string{
			"text": "Dunno what this is",
		},
	})

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docBadBlock,
	})
	test.MustNot(t, err, "expected unknown content block to fail validation")

	currentVersion, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get the document")

	test.EqualMessage(t, doc2, currentVersion.Document,
		"expected the last document to be returned")

	firstVersion, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:    docUUID,
		Version: 1,
	})
	test.Must(t, err, "get the first version of the document")

	test.EqualMessage(t, doc, firstVersion.Document,
		"expected the first document to be returned")

	t0 := time.Now()

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "delete the document")

	t.Logf("waited %v for delete", time.Since(t0))

	_, err = client.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "expected get to fail after delete")

	_, err = client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:    docUUID,
		Version: 1,
	})
	test.MustNot(t, err, "expected get of old version to fail after delete")

	var golden repository.GetEventlogResponse

	err = elephantine.UnmarshalFile(
		"testdata/TestIntegrationBasicCrud/eventlog.json",
		&golden)
	test.Must(t, err, "read golden file for expected eventlog items")

	events, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
		// One more event than we expect, so that we catch the
		// unexpected.
		BatchSize:   int32(len(golden.Items)) + 1,
		BatchWaitMs: 200,
	})
	test.Must(t, err, "get eventlog")

	diff := cmp.Diff(&golden, events,
		protocmp.Transform(),
		cmpopts.IgnoreMapEntries(func(k string, _ interface{}) bool {
			return k == "timestamp"
		}),
	)
	if diff != "" {
		t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
	}
}

func TestIntegrationPasswordGrant(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		SharedSecret: "very-secret-password",
	})

	form := url.Values{
		"grant_type": []string{"password"},
		"username":   []string{"John <user://example/john>"},
	}

	statusWithout, _ := requestToken(t, tc, "without password", form)

	test.Equal(t, http.StatusUnauthorized, statusWithout,
		"get status unauthorized back")

	form.Set("password", "someting-else")

	statusWrongPass, _ := requestToken(t, tc, "with wrong password", form)

	test.Equal(t, http.StatusUnauthorized, statusWrongPass,
		"get status unauthorized back")

	form.Set("password", "very-secret-password")

	statusCorrect, body := requestToken(t, tc, "with correct password", form)

	test.Equal(t, http.StatusOK, statusCorrect,
		"get an ok response")

	var responseData struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
	}

	err := json.Unmarshal(body, &responseData)
	test.Must(t, err, "decode token response")

	test.Equal(t, "Bearer", responseData.TokenType, "get correct token type")

	if responseData.ExpiresIn < 60 {
		t.Fatalf("expected token to be valid for more than 60 seconds, got %d",
			responseData.ExpiresIn)
	}

	if responseData.AccessToken == "" {
		t.Fatal("expected an access token")
	}

	if responseData.RefreshToken == "" {
		t.Fatal("expected a refresh token")
	}
}

func requestToken(
	t *testing.T, tc TestContext, name string, form url.Values,
) (int, []byte) {
	t.Helper()

	client := tc.Server.Client()

	req, err := http.NewRequest(
		http.MethodPost, tc.Server.URL+"/token",
		strings.NewReader(form.Encode()))
	test.Must(t, err, "create a token request %s", name)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	res, err := client.Do(req)
	test.Must(t, err, "perform a token request %s", name)

	body, err := io.ReadAll(res.Body)
	test.Must(t, err, "read response for request %s", name)

	defer func() {
		err := res.Body.Close()
		test.Must(t, err,
			"close response body for request %s", name)
	}()

	return res.StatusCode, body
}

func TestIntegrationStatus(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunReplicator: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := test.Context(t)

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

	const (
		pageSize    = 10
		numStatuses = 12
	)

	for n := 0; n < numStatuses; n++ {
		meta := map[string]string{
			"update_number": strconv.Itoa(n),
		}

		if n%3 == 0 {
			docRes, err = client.Update(ctx, &repository.UpdateRequest{
				Uuid:     docUUID,
				Document: doc,
			})
			test.Must(t, err, "update article")
		}

		_, err := client.Update(ctx, &repository.UpdateRequest{
			Uuid: docUUID,
			Status: []*repository.StatusUpdate{
				{Name: "done", Version: docRes.Version, Meta: meta},
				{Name: "usable", Version: docRes.Version, Meta: meta},
			},
		})
		test.Must(t, err, "set done and usable status %d", n+1)
	}

	fromHeadRes, err := client.GetStatusHistory(ctx, &repository.GetStatusHistoryRequest{
		Uuid: docUUID,
		Name: "usable",
	})
	test.Must(t, err, "get usable status history from head")

	test.Equal(t, pageSize, len(fromHeadRes.Statuses),
		"get the expected number of statuses")

	var minStatus int64 = numStatuses

	for i, s := range fromHeadRes.Statuses {
		test.Equal(t, int64(numStatuses-i), s.Id,
			"expect the %dnth status to have the correct ID", i+1)

		test.Equal(t, len(s.Meta), 1, "expect the status to have metadata")

		metaV := s.Meta["update_number"]

		test.Equal(t,
			strconv.Itoa(int(s.Id-1)), metaV,
			"expected the status to have a correct update_number metadata value")

		if s.Id < minStatus {
			minStatus = s.Id
		}
	}

	fromLastRes, err := client.GetStatusHistory(ctx,
		&repository.GetStatusHistoryRequest{
			Uuid:   docUUID,
			Name:   "usable",
			Before: minStatus,
		})
	test.Must(t, err, "get next page of status history")

	test.Equal(t, numStatuses-pageSize, len(fromLastRes.Statuses),
		"get the expected number of statuses")

	for i, s := range fromLastRes.Statuses {
		test.Equal(t, int64(numStatuses-i-pageSize), s.Id,
			"expect the %dnth status to have the correct ID", i+1)
	}

	var golden repository.GetEventlogResponse

	err = elephantine.UnmarshalFile(
		"testdata/TestIntegrationStatus/eventlog.json",
		&golden)
	test.Must(t, err, "read golden file for expected eventlog items")

	events, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
		// One more event than we expect, so that we catch the
		// unexpected.
		BatchSize:   int32(len(golden.Items)) + 1,
		BatchWaitMs: 200,
	})
	test.Must(t, err, "get eventlog")

	diff := cmp.Diff(&golden, events,
		protocmp.Transform(),
		cmpopts.IgnoreMapEntries(func(k string, _ interface{}) bool {
			return k == "timestamp"
		}),
	)
	if diff != "" {
		t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
	}
}

func TestIntegrationDeleteTimeout(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	ctx := test.Context(t)

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "expected the delete to time out")

	test.IsTwirpError(t, err, twirp.FailedPrecondition)
}

func TestIntegrationStatuses(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	ctx := test.Context(t)
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	tc := testingAPIServer(t, logger, testingServerOptions{})

	untrustedWorkflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "doc_read"))

	_, err := untrustedWorkflowClient.UpdateStatus(ctx,
		&repository.UpdateStatusRequest{
			Name: "usable",
		})
	test.MustNot(t, err,
		"be able to create statues without 'workflow_admin' scope")

	workflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Name: "usable",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
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
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		Status: []*repository.StatusUpdate{
			{Name: "usable"},
		},
	})
	test.Must(t, err, "create article")

	doc2 := test.CloneMessage(doc)
	doc2.Title = "Drafty McDraftface"

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc2,
	})
	test.Must(t, err, "update article")

	doc3 := test.CloneMessage(doc2)
	doc3.Title = "More appropriate title"

	res3, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc3,
		Status: []*repository.StatusUpdate{
			{Name: "done"},
		},
	})
	test.Must(t, err, "update article with 'done' status")

	currentUsable, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:   docUUID,
		Status: "usable",
	})
	test.Must(t, err, "fetch the currently published version")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "whatchacallit", Version: res3.Version},
		},
	})
	test.MustNot(t, err, "should fail to use unknown status")

	test.IsTwirpError(t, err, twirp.InvalidArgument)

	test.EqualMessage(t,
		&repository.GetDocumentResponse{
			Version:  1,
			Document: doc,
		}, currentUsable,
		"expected the currently publised version to be unchanged")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
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
	tc := testingAPIServer(t, logger, testingServerOptions{})

	workflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	const (
		approvalName       = "must-be-approved"
		requirePublishName = "require-publish-scope"
		requireLegalName   = "require-legal-approval"
	)

	_, err := workflowClient.CreateStatusRule(ctx, &repository.CreateStatusRuleRequest{
		Rule: &repository.StatusRule{
			Name:        approvalName,
			Description: "Articles must be approved before they're published",
			Expression:  `Heads.approved.Version == Status.Version`,
			AppliesTo:   []string{"usable"},
			ForTypes:    []string{"core/article"},
		},
	})
	test.Must(t, err, "create approval rule")

	_, err = workflowClient.CreateStatusRule(ctx, &repository.CreateStatusRuleRequest{
		Rule: &repository.StatusRule{
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

	_, err = workflowClient.CreateStatusRule(ctx, &repository.CreateStatusRuleRequest{
		Rule: &repository.StatusRule{
			Name:        "require-publish-scope",
			Description: "publish scope is required for setting usable",
			Expression:  `User.HasScope("publish")`,
			AccessRule:  true,
			AppliesTo:   []string{"usable"},
			ForTypes:    []string{"core/article"},
		},
	})
	test.Must(t, err, "create publish permission rule")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Name: "approved_legal",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Name: "usable",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
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
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	editorClient := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete publish"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	createdRes, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	_, err = editorClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: createdRes.Version},
		},
	})
	test.MustNot(t, err, "don't publish document without approval")

	test.IsTwirpError(t, err, twirp.InvalidArgument)

	if !strings.Contains(err.Error(), approvalName) {
		t.Fatalf("error message should mention %q, got: %s",
			approvalName, err.Error())
	}

	_, err = editorClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "approved", Version: createdRes.Version},
		},
	})
	test.Must(t, err, "approve document")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: createdRes.Version},
		},
	})
	test.MustNot(t, err, "don't allow publish without the correct scope")

	test.IsTwirpError(t, err, twirp.PermissionDenied)

	if !strings.Contains(err.Error(), requirePublishName) {
		t.Fatalf("error message should mention %q, got: %s",
			requirePublishName, err.Error())
	}

	_, err = editorClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: createdRes.Version},
		},
	})
	test.Must(t, err, "publish the document")

	docV2 := test.CloneMessage(doc)

	docV2.Content = append(docV2.Content, &newsdoc.Block{
		Type: "core/paragraph",
		Data: map[string]string{
			"text": "Some sensitive stuff",
		},
	})

	updatedRes, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: docV2,
		Status: []*repository.StatusUpdate{
			{
				Name: "approved",
				Meta: map[string]string{
					"legal_approval": "required",
				},
			},
		},
	})
	test.Must(t, err, "update article")

	_, err = editorClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: updatedRes.Version},
		},
	})
	test.MustNot(t, err,
		"should not publish the second version without legal approval")

	if !strings.Contains(err.Error(), requireLegalName) {
		t.Fatalf("error message should mention %q, got: %s",
			requireLegalName, err.Error())
	}

	_, err = editorClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "approved_legal", Version: updatedRes.Version},
		},
	})
	test.Must(t, err, "set the legal approval status")

	_, err = editorClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
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
	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	otherClaims := itest.StandardClaims(t,
		"doc_read doc_write doc_delete",
		"unit://some/group")

	otherClaims.Subject = "user://test/other-user"

	otherClient := tc.DocumentsClient(t, otherClaims)

	adminClaims := itest.StandardClaims(t, "doc_admin", "unit://some/group")
	adminClaims.Subject = "user://test/admin-user"

	adminClient := tc.DocumentsClient(t, adminClaims)

	const (
		docUUID  = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI   = "article://test/123"
		doc2UUID = "93bd1a10-8136-4ace-8722-f4cc10bdb3e9"
		doc2URI  = "article://test/123-b"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create first article")

	doc2 := baseDocument(doc2UUID, doc2URI)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
		Acl: []*repository.ACLEntry{
			{
				Uri:         "user://test/other-user",
				Permissions: []string{"r"},
			},
		},
	})
	test.Must(t, err, "create second article")

	_, err = otherClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "didn't expect the other user to have read access")

	otherP1, err := otherClient.GetPermissions(ctx, &repository.GetPermissionsRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get other users permissions for document one")

	test.EqualMessage(t, &repository.GetPermissionsResponse{}, otherP1,
		"expected the permissions response to verify that the other user doesn't have access")

	_, err = otherClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: doc2UUID,
	})
	test.Must(t, err, "expected the other user to have access to document two")

	otherP2, err := otherClient.GetPermissions(ctx, &repository.GetPermissionsRequest{
		Uuid: doc2UUID,
	})
	test.Must(t, err, "get other users permissions for document two")

	test.EqualMessage(t,
		&repository.GetPermissionsResponse{
			Permissions: map[string]string{
				"r": otherClaims.Subject,
			},
		}, otherP2,
		"expected the permissions response to verify that the other user has read access")

	doc2.Title = "A better title, clearly"

	_, err = otherClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
	})
	test.MustNot(t, err,
		"didn't expect the other user to have write acces to document two")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Acl: []*repository.ACLEntry{
			{
				Uri:         "unit://some/group",
				Permissions: []string{"r", "w"},
			},
		},
	})
	test.Must(t, err, "update ACL for document one")

	_, err = otherClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "expected other user to be able to read document one after ACL update")

	_, err = otherClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: doc2UUID,
	})
	test.Must(t, err, "expected the other user to have access to document two")

	otherP3, err := otherClient.GetPermissions(ctx, &repository.GetPermissionsRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get other users permissions for document one")

	test.EqualMessage(t,
		&repository.GetPermissionsResponse{
			Permissions: map[string]string{
				"r": "unit://some/group",
				"w": "unit://some/group",
			},
		}, otherP3,
		"expected the permissions response to verify that the other user has read/write access")

	doc.Title = "The first doc is the best doc"

	_, err = otherClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "expected other user to be able to update document one after ACL update")

	adminP, err := adminClient.GetPermissions(ctx, &repository.GetPermissionsRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get admin users permissions for document one")

	test.EqualMessage(t,
		&repository.GetPermissionsResponse{
			Permissions: map[string]string{
				"r": "scope://doc_admin",
				"w": "scope://doc_admin",
			},
		}, adminP,
		"expected the permissions response to reflect that admin gains access through scopes")
}

func TestDocumentLocking(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	ctx := test.Context(t)
	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write"))

	const (
		docUUID = "88f13bde-1a84-4151-8f2d-aaee3ae57c05"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create test article")

	_, err = client.Lock(ctx, &repository.LockRequest{
		Uuid: docUUID,
		Ttl:  5000,
	})
	test.Must(t, err, "lock the document")

	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "fetch document meta")
	fmt.Println(meta.Meta.Lock)
	test.NotNil(t, meta.Meta.Lock, "document should have a lock")

	// _, err = client.Lock(ctx, &repository.LockRequest{
	// 	Uuid:  docUUID,
	// 	Ttl:   5000,
	// 	Token: lock.Token,
	// })
	// test.Must(t, err, "update an existing lock")
	//
	// _, err = client.Lock(ctx, &repository.LockRequest{
	// 	Uuid:  docUUID,
	// 	Ttl:   5000,
	// 	Token: "another token",
	// })
	// test.MustNot(t, err, "steal an existing lock")
}
