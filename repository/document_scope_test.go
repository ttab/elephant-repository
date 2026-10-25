package repository_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repositorysocket"
	itest "github.com/ttab/elephant-repository/internal/test"
	repo "github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
)

func TestHasDocumentScope(t *testing.T) {
	const (
		docA = "8f0c5d4e-2b1a-4c3d-9e8f-7a6b5c4d3e2f"
		docB = "1d2c3b4a-5e6f-4a7b-8c9d-0e1f2a3b4c5d"
	)

	idA := uuid.MustParse(docA)

	cases := []struct {
		Name   string
		Scope  string
		Scopes []string
		Want   bool
	}{
		{
			"read restricted to the document", "doc_read:" + docA,
			[]string{"doc_read"},
			true,
		},
		{
			"write restricted to the document", "doc_write:" + docA,
			[]string{"doc_write"},
			true,
		},
		{
			"among other scopes", "eventlog_read doc_read:" + docA + " schema_read",
			[]string{"doc_read"},
			true,
		},
		{
			"upper case UUID", "doc_read:" + strings.ToUpper(docA),
			[]string{"doc_read"},
			true,
		},
		{
			"another document", "doc_read:" + docB,
			[]string{"doc_read"},
			false,
		},
		{
			"a scope the method doesn't accept", "doc_write:" + docA,
			[]string{"doc_read"},
			false,
		},
		{
			"unrestricted scope", "doc_read",
			[]string{"doc_read"},
			false,
		},
		{
			"empty suffix", "doc_read:",
			[]string{"doc_read"},
			false,
		},
		{
			"malformed suffix", "doc_read:not-a-uuid",
			[]string{"doc_read"},
			false,
		},
		{
			"unhyphenated UUID", "doc_read:" + strings.ReplaceAll(docA, "-", ""),
			[]string{"doc_read"},
			false,
		},
		{
			"urn UUID", "doc_read:urn:uuid:" + docA,
			[]string{"doc_read"},
			false,
		},
		{
			"braced UUID", "doc_read:{" + docA + "}",
			[]string{"doc_read"},
			false,
		},
		{
			"trailing segment", "doc_read:" + docA + ":x",
			[]string{"doc_read"},
			false,
		},
		{
			"admin can't be restricted", "doc_admin:" + docA,
			[]string{"doc_admin"},
			false,
		},
		{
			"read all can't be restricted", "doc_read_all:" + docA,
			[]string{"doc_read_all"},
			false,
		},
		{
			"delete can't be restricted", "doc_delete:" + docA,
			[]string{"doc_delete"},
			false,
		},
		{
			"tab is not a separator", "doc_read:" + docA + "\tdoc_write",
			[]string{"doc_read"},
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			claims := elephantine.JWTClaims{Scope: c.Scope}

			test.Equalf(t, c.Want,
				repo.HasDocumentScope(claims, idA, c.Scopes...),
				"check %q against %v", c.Scope, c.Scopes)
		})
	}

	test.Equalf(t, false,
		repo.HasDocumentScope(elephantine.JWTClaims{
			Scope: "doc_read:" + uuid.Nil.String(),
		}, uuid.Nil, "doc_read"),
		"the nil UUID names no document")
}

// TestIntegrationDocumentRestrictedScopes holds every method gated on the
// document scopes to a token whose scopes are restricted to one document, the
// way elephant-delegate narrows a session token for a device: allowed on that
// document, refused on another the user has the same ACL access to.
func TestIntegrationDocumentRestrictedScopes(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	ctx := t.Context()
	tc := testingAPIServer(t, logger, testingServerOptions{})

	const (
		user   = "owner"
		docA   = "5a0e8c1f-6d2b-4e3a-9f4c-1b2d3e4f5a6b"
		docB   = "7c1f9d2a-8e3b-4f4c-a05d-2c3e4f5a6b7c"
		docNew = "9e2a0b3c-4d5e-4f6a-8b7c-3d4e5f6a7b8c"
		// A document the user has no ACL access to.
		docOther = "b3c4d5e6-f7a8-4b9c-8d0e-1f2a3b4c5d6e"
	)

	subject := "user://test/" + user

	owner := tc.DocumentsClient(t,
		itest.Claims(t, user, "doc_read doc_write doc_delete"))

	for _, id := range []string{docA, docB} {
		_, err := owner.Update(ctx, &repository.UpdateRequest{
			Uuid:     id,
			Document: baseDocument(id, "article://test/"+id),
			Status:   []*repository.StatusUpdate{{Name: "usable"}},
		})
		test.Mustf(t, err, "create document %s", id)
	}

	stranger := tc.DocumentsClient(t,
		itest.Claims(t, "stranger", "doc_read doc_write"))

	_, err := stranger.Update(ctx, &repository.UpdateRequest{
		Uuid:     docOther,
		Document: baseDocument(docOther, "article://test/"+docOther),
	})
	test.Mustf(t, err, "create the stranger's document")

	restricted := func(scopes ...string) repository.Documents {
		return tc.DocumentsClient(t,
			itest.Claims(t, user, strings.Join(scopes, " ")))
	}

	readA := restricted("doc_read:" + docA)
	writeA := restricted("doc_write:" + docA)
	readWriteA := restricted("doc_read:"+docA, "doc_write:"+docA)

	reads := []struct {
		Method string
		Call   func(ctx context.Context, c repository.Documents, id string) error
	}{
		{"Get", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.Get(ctx, &repository.GetDocumentRequest{Uuid: id})

			return err
		}},
		{"GetMeta", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetMeta(ctx, &repository.GetMetaRequest{Uuid: id})

			return err
		}},
		{"GetHistory", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetHistory(ctx, &repository.GetHistoryRequest{Uuid: id})

			return err
		}},
		{"GetStatus", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetStatus(ctx, &repository.GetStatusRequest{
				Uuid: id, Name: "usable",
			})

			return err
		}},
		{"GetStatusHistory", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetStatusHistory(ctx, &repository.GetStatusHistoryRequest{
				Uuid: id, Name: "usable",
			})

			return err
		}},
		{"GetNilStatuses", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetNilStatuses(ctx, &repository.GetNilStatusesRequest{
				Uuid: id,
			})

			return err
		}},
		{"GetDeliverableInfo", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetDeliverableInfo(ctx, &repository.GetDeliverableInfoRequest{
				Uuid: id,
			})

			return err
		}},
		{"GetPermissions", func(ctx context.Context, c repository.Documents, id string) error {
			_, err := c.GetPermissions(ctx, &repository.GetPermissionsRequest{
				Uuid: id,
			})

			return err
		}},
	}

	for _, r := range reads {
		t.Run(r.Method, func(t *testing.T) {
			err := r.Call(ctx, readA, docA)
			test.Mustf(t, err, "read the document the scope names")

			err = r.Call(ctx, readA, docB)
			test.IsRPCError(t, err, connect.CodePermissionDenied)

			err = r.Call(ctx, readA, strings.ToUpper(docA))
			test.Mustf(t, err, "the request UUID is compared parsed")
		})
	}

	t.Run("GetPermissionsReportsTheACL", func(t *testing.T) {
		for _, c := range []repository.Documents{readA, writeA} {
			res, err := c.GetPermissions(ctx, &repository.GetPermissionsRequest{
				Uuid: docA,
			})
			test.Mustf(t, err, "get permissions")

			test.EqualMessagef(t, &repository.GetPermissionsResponse{
				Permissions: map[string]string{
					"r": subject,
					"w": subject,
				},
			}, res, "the user's own ACL access is reported")
		}

		_, err := writeA.GetPermissions(ctx, &repository.GetPermissionsRequest{
			Uuid: docB,
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})

	t.Run("ACLStillApplies", func(t *testing.T) {
		readOther := restricted("doc_read:"+docOther, "doc_write:"+docOther)

		_, err := readOther.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docOther,
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		perms, err := readOther.GetPermissions(ctx, &repository.GetPermissionsRequest{
			Uuid: docOther,
		})
		test.Mustf(t, err, "get permissions on the stranger's document")

		test.EqualMessagef(t, &repository.GetPermissionsResponse{}, perms,
			"the restricted scope grants no ACL access")

		doc := baseDocument(docOther, "article://test/"+docOther)
		doc.Title = "Taken over"

		_, err = readOther.Update(ctx, &repository.UpdateRequest{
			Uuid:     docOther,
			Document: doc,
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})

	t.Run("MalformedSuffix", func(t *testing.T) {
		for _, scope := range []string{
			"doc_read:" + strings.ReplaceAll(docA, "-", ""),
			"doc_read:urn:uuid:" + docA,
			"doc_read:" + docA + ":x",
			"doc_read:",
			"doc_admin:" + docA,
			"doc_read_all:" + docA,
		} {
			_, err := restricted(scope).Get(ctx, &repository.GetDocumentRequest{
				Uuid: docA,
			})
			test.IsRPCError(t, err, connect.CodePermissionDenied)
		}
	})

	t.Run("Update", func(t *testing.T) {
		for _, id := range []string{docA, docB} {
			doc := baseDocument(id, "article://test/"+id)
			doc.Title = "Updated through a restricted scope"

			_, err := writeA.Update(ctx, &repository.UpdateRequest{
				Uuid:     id,
				Document: doc,
			})
			if id == docA {
				test.Mustf(t, err, "update the document the scope names")
			} else {
				test.IsRPCError(t, err, connect.CodePermissionDenied)
			}
		}

		_, err := writeA.Update(ctx, &repository.UpdateRequest{
			Uuid:   docB,
			Status: []*repository.StatusUpdate{{Name: "done", Version: 1}},
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		_, err = readA.Update(ctx, &repository.UpdateRequest{
			Uuid:     docA,
			Document: baseDocument(docA, "article://test/"+docA),
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		// Creating a document is writing it, so a scope restricted to
		// a new UUID creates that document and no other.
		_, err = restricted("doc_write:"+docNew).Update(ctx,
			&repository.UpdateRequest{
				Uuid:     docNew,
				Document: baseDocument(docNew, "article://test/"+docNew),
			})
		test.Mustf(t, err, "create the document the scope names")
	})

	t.Run("Locks", func(t *testing.T) {
		lock, err := writeA.Lock(ctx, &repository.LockRequest{
			Uuid: docA, Ttl: 5000, App: "test",
		})
		test.Mustf(t, err, "lock the document the scope names")

		_, err = writeA.ExtendLock(ctx, &repository.ExtendLockRequest{
			Uuid: docA, Ttl: 5000, Token: lock.Token,
		})
		test.Mustf(t, err, "extend the lock")

		_, err = writeA.Unlock(ctx, &repository.UnlockRequest{
			Uuid: docA, Token: lock.Token,
		})
		test.Mustf(t, err, "unlock")

		_, err = writeA.Lock(ctx, &repository.LockRequest{
			Uuid: docB, Ttl: 5000, App: "test",
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		lockB, err := owner.Lock(ctx, &repository.LockRequest{
			Uuid: docB, Ttl: 5000, App: "test",
		})
		test.Mustf(t, err, "lock the other document as the owner")

		_, err = writeA.ExtendLock(ctx, &repository.ExtendLockRequest{
			Uuid: docB, Ttl: 5000, Token: lockB.Token,
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		_, err = writeA.Unlock(ctx, &repository.UnlockRequest{
			Uuid: docB, Token: lockB.Token,
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		_, err = owner.Unlock(ctx, &repository.UnlockRequest{
			Uuid: docB, Token: lockB.Token,
		})
		test.Mustf(t, err, "unlock the other document as the owner")
	})

	t.Run("GetWithLock", func(t *testing.T) {
		res, err := readWriteA.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docA,
			Lock: &repository.AcquireLock{Ttl: 5000, App: "test"},
		})
		test.Mustf(t, err, "get and lock the document the scopes name")

		_, err = readWriteA.Unlock(ctx, &repository.UnlockRequest{
			Uuid: docA, Token: res.Lock.Token,
		})
		test.Mustf(t, err, "unlock")

		_, err = readWriteA.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docB,
			Lock: &repository.AcquireLock{Ttl: 5000, App: "test"},
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		// Locking takes write access, which a read scope doesn't give.
		_, err = readA.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docA,
			Lock: &repository.AcquireLock{Ttl: 5000, App: "test"},
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)

		// Neither does write access restricted to another document.
		_, err = restricted("doc_read:"+docA, "doc_write:"+docB).Get(ctx,
			&repository.GetDocumentRequest{
				Uuid: docA,
				Lock: &repository.AcquireLock{Ttl: 5000, App: "test"},
			})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})

	t.Run("Delete", func(t *testing.T) {
		// doc_delete can't be restricted, so neither restricted scope
		// deletes anything.
		_, err := readWriteA.Delete(ctx, &repository.DeleteDocumentRequest{
			Uuid: docA,
		})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})

	t.Run("MultiDocumentMethodsRefuse", func(t *testing.T) {
		testRestrictedScopeRefusals(t, tc, user, docA)
	})
}

// testRestrictedScopeRefusals checks that methods that aren't about a single
// document refuse a token whose document scopes are restricted to docA, even
// when the request only names docA.
func testRestrictedScopeRefusals(
	t *testing.T, tc TestContext, user string, docA string,
) {
	t.Helper()

	ctx := t.Context()

	claims := itest.Claims(t, user, "doc_read:"+docA+" doc_write:"+docA)

	docs := tc.DocumentsClient(t, claims)
	workflows := tc.WorkflowsClient(t, claims)
	schemas := tc.SchemasClient(t, claims)

	calls := map[string]func() error{
		"Documents.BulkGet": func() error {
			_, err := docs.BulkGet(ctx, &repository.BulkGetRequest{
				Documents: []*repository.BulkGetReference{{Uuid: docA}},
			})

			return err
		},
		"Documents.GetMatching": func() error {
			_, err := docs.GetMatching(ctx, &repository.GetMatchingRequest{
				Type: "core/article",
			})

			return err
		},
		"Documents.GetStatusOverview": func() error {
			_, err := docs.GetStatusOverview(ctx, &repository.GetStatusOverviewRequest{
				Uuids: []string{docA},
			})

			return err
		},
		"Documents.BulkGetDeliverableInfo": func() error {
			_, err := docs.BulkGetDeliverableInfo(ctx,
				&repository.BulkGetDeliverableInfoRequest{
					Uuids: []string{docA},
				})

			return err
		},
		"Documents.GetAttachments": func() error {
			_, err := docs.GetAttachments(ctx, &repository.GetAttachmentsRequest{
				Documents:      []string{docA},
				AttachmentName: "image",
			})

			return err
		},
		"Documents.BulkUpdate": func() error {
			_, err := docs.BulkUpdate(ctx, &repository.BulkUpdateRequest{
				Updates: []*repository.UpdateRequest{{
					Uuid:     docA,
					Document: baseDocument(docA, "article://test/"+docA),
				}},
			})

			return err
		},
		"Documents.Validate": func() error {
			_, err := docs.Validate(ctx, &repository.ValidateRequest{
				Document: baseDocument(docA, "article://test/"+docA),
			})

			return err
		},
		"Documents.Prune": func() error {
			_, err := docs.Prune(ctx, &repository.PruneRequest{
				Document: baseDocument(docA, "article://test/"+docA),
			})

			return err
		},
		"Documents.GetSocketToken": func() error {
			_, err := docs.GetSocketToken(ctx, &repository.GetSocketTokenRequest{})

			return err
		},
		"Documents.Eventlog": func() error {
			_, err := docs.Eventlog(ctx, &repository.GetEventlogRequest{})

			return err
		},
		"Documents.CompactedEventlog": func() error {
			_, err := docs.CompactedEventlog(ctx,
				&repository.GetCompactedEventlogRequest{Until: 1})

			return err
		},
		"Workflows.GetStatuses": func() error {
			_, err := workflows.GetStatuses(ctx, &repository.GetStatusesRequest{
				Type: "core/article",
			})

			return err
		},
		"Workflows.GetWorkflow": func() error {
			_, err := workflows.GetWorkflow(ctx, &repository.GetWorkflowRequest{
				Type: "core/article",
			})

			return err
		},
		"Schemas.GetDocumentTypes": func() error {
			_, err := schemas.GetDocumentTypes(ctx,
				&repository.GetDocumentTypesRequest{})

			return err
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			test.IsRPCError(t, call(), connect.CodePermissionDenied)
		})
	}

	t.Run("Socket.GetDocuments", func(t *testing.T) {
		token, err := itest.AccessToken(tc.SigningKey, claims)
		test.Mustf(t, err, "create access token")

		conn := dialAuthenticatedSocket(t, tc, user, token)

		callID := makeCall(t, conn, &repositorysocket.Call{
			GetDocuments: &repositorysocket.GetDocuments{
				SetName: "articles",
				Type:    "core/article",
			},
		})

		resp, ok := readResponse(t, conn)
		if !ok {
			t.Fatal("socket was unexpectedly closed")
		}

		test.Equalf(t, callID, resp.CallId, "response to the call")
		test.NotNilf(t, resp.Error, "the call is refused")
		test.Equalf(t, connect.CodePermissionDenied.String(),
			resp.Error.ErrorCode, "refused for lack of a scope")
	})
}

// dialAuthenticatedSocket opens a websocket session with a socket token issued
// to an unrestricted session of the user, and authenticates it with token.
func dialAuthenticatedSocket(
	t *testing.T, tc TestContext, user string, token string,
) *websocket.Conn {
	t.Helper()

	ctx := t.Context()

	client := tc.DocumentsClient(t, itest.Claims(t, user, "doc_read"))

	tokenResp, err := client.GetSocketToken(ctx, &repository.GetSocketTokenRequest{})
	test.Mustf(t, err, "get socket token")

	serverURL, err := url.Parse(tc.Server.URL)
	test.Mustf(t, err, "parse server URL")

	wsURL := serverURL.JoinPath("websocket", tokenResp.Token)
	wsURL.Scheme = "ws"

	header := http.Header{
		"Origin": []string{"https://example.ecms.se"},
	}

	conn, wsResp, err := websocket.DefaultDialer.DialContext(
		ctx, wsURL.String(), header)
	test.Mustf(t, err, "dial websocket")

	t.Cleanup(func() {
		_ = conn.Close()

		err := wsResp.Body.Close()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close websocket response body: %v", err)
		}
	})

	callID := makeCall(t, conn, &repositorysocket.Call{
		Authenticate: &repositorysocket.Authenticate{Token: token},
	})

	authResp, ok := readResponse(t, conn)
	if !ok {
		t.Fatal("socket was unexpectedly closed")
	}

	test.Equalf(t, callID, authResp.CallId, "authentication response")

	if authResp.Error != nil {
		t.Fatalf("authenticate: %s: %s",
			authResp.Error.ErrorCode, authResp.Error.ErrorMessage)
	}

	return conn
}
