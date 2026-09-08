package repository_test

import (
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"

	"connectrpc.com/connect"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	elephantrpc "github.com/ttab/elephantine/rpc"
	"github.com/ttab/elephantine/test"
)

// TestIntegrationErrorParity runs the error paths the suite exercises over
// both stacks against the same server and checks that a caller cannot tell
// them apart. The handlers speak the Connect error vocabulary and the Twirp
// mount's interceptor translates on the way out, so this is what keeps that
// translation honest: code, message and every metadata key have to survive it.
func TestIntegrationErrorParity(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		Stack: stackTwirp,
	})

	connectTC := tc
	connectTC.Stack = stackConnect

	ctx := t.Context()

	writeClaims := itest.StandardClaims(t,
		"doc_read doc_write doc_delete")

	twirpClient := tc.DocumentsClient(t, writeClaims)
	connectClient := connectTC.DocumentsClient(t, writeClaims)

	// parity performs the same call against both stacks and compares the
	// two errors. The call is a closure rather than a request value because
	// several of the cases need a client of their own.
	parity := func(
		t *testing.T, code connect.Code,
		call func(client repository.Documents) error,
	) map[string]string {
		t.Helper()

		twirpErr := call(twirpClient)
		test.IsRPCError(t, twirpErr, code)

		connectErr := call(connectClient)
		test.IsRPCError(t, connectErr, code)

		test.ErrorParity(t, twirpErr, connectErr)

		return elephantrpc.Meta(connectErr)
	}

	t.Run("missing scope", func(t *testing.T) {
		noScope := itest.Claims(t, "nobody", "")

		_, twirpErr := tc.DocumentsClient(t, noScope).Get(
			ctx, &repository.GetDocumentRequest{})
		test.IsRPCError(t, twirpErr, connect.CodePermissionDenied)

		_, connectErr := connectTC.DocumentsClient(t, noScope).Get(
			ctx, &repository.GetDocumentRequest{})
		test.IsRPCError(t, connectErr, connect.CodePermissionDenied)

		test.ErrorParity(t, twirpErr, connectErr)

		meta := elephantrpc.Meta(connectErr)

		test.Equalf(t, "doc_read doc_read_all doc_admin",
			meta[elephantrpc.MetaRequiredScopes],
			"name the scopes that would have been accepted")
	})

	t.Run("not found", func(t *testing.T) {
		parity(t, connect.CodeNotFound,
			func(client repository.Documents) error {
				_, err := client.Get(ctx,
					&repository.GetDocumentRequest{
						Uuid: "3f2b1a5e-6f70-4bfe-bcf5-84b3a6a9e2ea",
					})

				return err
			})
	})

	t.Run("invalid argument", func(t *testing.T) {
		meta := parity(t, connect.CodeInvalidArgument,
			func(client repository.Documents) error {
				_, err := client.Get(ctx,
					&repository.GetDocumentRequest{})

				return err
			})

		test.Equalf(t, "uuid", meta["argument"],
			"name the offending argument in the metadata")
	})

	t.Run("validation errors", func(t *testing.T) {
		docUUID := "6b7f2a3c-9d41-4f0e-8c25-1a3b4c5d6e70"

		meta := parity(t, connect.CodeInvalidArgument,
			func(client repository.Documents) error {
				doc := baseDocument(docUUID,
					"article://test/error-parity-validation")

				doc.Meta = append(doc.Meta, &newsdoc.Block{
					Type: "core/not-a-declared-block",
				})

				_, err := client.Update(ctx,
					&repository.UpdateRequest{
						Uuid:     docUUID,
						Document: doc,
					})

				return err
			})

		// The individual errors are numbered from zero, and err_count
		// says how many of them there are.
		count, err := strconv.Atoi(meta["err_count"])
		test.Mustf(t, err, "read the err_count metadata")

		for i := range count {
			test.Equalf(t, false, meta[strconv.Itoa(i)] == "",
				"describe validation error %d", i)
		}

		test.Equalf(t, count, len(meta)-1,
			"carry nothing but err_count and the numbered errors")
	})

	t.Run("lock conflict", func(t *testing.T) {
		docUUID := "b2c1d0e9-8f7a-4b6c-9d5e-4f3a2b1c0d9e"

		doc := baseDocument(docUUID, "article://test/error-parity-lock")

		_, err := twirpClient.Update(ctx, &repository.UpdateRequest{
			Uuid:     docUUID,
			Document: doc,
		})
		test.Mustf(t, err, "create the document")

		_, err = twirpClient.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docUUID,
			Lock: &repository.AcquireLock{
				Ttl:     500,
				App:     "holder-app",
				Comment: "while editing",
			},
		})
		test.Mustf(t, err, "take the lock")

		// doc_admin bypasses the ACL check so that the second caller
		// reaches the lock and gets a conflict rather than a permission
		// error.
		rivalClaims := itest.Claims(t, "rival", "doc_admin")

		rivals := map[rpcStack]repository.Documents{
			stackTwirp:   tc.DocumentsClient(t, rivalClaims),
			stackConnect: connectTC.DocumentsClient(t, rivalClaims),
		}

		lock := func(stack rpcStack) error {
			_, err := rivals[stack].Get(ctx,
				&repository.GetDocumentRequest{
					Uuid: docUUID,
					Lock: &repository.AcquireLock{
						Ttl: 500,
						App: "rival-app",
					},
				})

			return err
		}

		twirpErr := lock(stackTwirp)
		test.IsRPCError(t, twirpErr, connect.CodeFailedPrecondition)

		connectErr := lock(stackConnect)
		test.IsRPCError(t, connectErr, connect.CodeFailedPrecondition)

		test.ErrorParity(t, twirpErr, connectErr)

		meta := elephantrpc.Meta(connectErr)

		test.Equalf(t, writeClaims.Subject,
			meta["lock_holder_sub"],
			"identify the lock holder")
		test.Equalf(t, "holder-app", meta["lock_app"],
			"name the holding application")
		test.Equalf(t, "while editing", meta["lock_comment"],
			"pass on the lock comment")
		test.Equalf(t, false, meta["lock_expires"] == "",
			"report when the lock expires")
		test.Equalf(t, false, meta["lock_exclusivity"] == "",
			"report the lock exclusivity")
	})

	t.Run("failed precondition", func(t *testing.T) {
		docUUID := "0e1d2c3b-4a59-4687-9a5b-6c7d8e9f0a1b"

		doc := baseDocument(docUUID, "article://test/error-parity-system")

		_, err := twirpClient.Update(ctx, &repository.UpdateRequest{
			Uuid:     docUUID,
			Document: doc,
		})
		test.Mustf(t, err, "create the document")

		// A delete waits for the archiver to finish with the document
		// before it takes it out, and the test server runs no archiver,
		// so the call always ends in the same failed precondition. That
		// is the same code the system lock and the workflow rules answer
		// with, and the delete is the one path that reaches it without
		// racing a background worker.
		parity(t, connect.CodeFailedPrecondition,
			func(client repository.Documents) error {
				_, err := client.Delete(ctx,
					&repository.DeleteDocumentRequest{
						Uuid: docUUID,
					})

				return err
			})
	})
}

// TestIntegrationErrorBodies pins the raw JSON error bodies of both stacks.
// The Twirp body is what existing consumers parse and may not move; the
// Connect body is what the ErrorMeta detail is rendered as, and is the shape
// elephant-chrome and the raw fetch callers have to read metadata out of.
func TestIntegrationErrorBodies(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		Stack: stackTwirp,
	})

	dataDir := filepath.Join("..", "testdata", t.Name())
	regenerate := regenerateTestFixtures()

	claims := itest.StandardClaims(t, "doc_read doc_write doc_delete")

	client := tc.authClient(t, tc.client, claims)

	// The failed_precondition case deletes this document. A delete waits for
	// the archiver to finish with the document before it takes it out, and
	// the test server runs no archiver, so both calls end in the same failed
	// precondition without either of them changing what the other sees.
	deleteUUID := "5c4d3e2f-1a0b-4c9d-8e7f-6a5b4c3d2e1f"

	_, err := tc.DocumentsClient(t, claims).Update(t.Context(),
		&repository.UpdateRequest{
			Uuid: deleteUUID,
			Document: baseDocument(deleteUUID,
				"article://test/error-bodies-delete"),
		})
	test.Mustf(t, err, "create the document to delete")

	call := func(t *testing.T, path string, body string) rpcResponse {
		t.Helper()

		return tc.postJSON(t, client, path, body)
	}

	for _, c := range []struct {
		Name   string
		Method string
		Body   string
	}{
		{
			// invalid_argument with an "argument" metadata key, which
			// is the most common shape in the API.
			Name:   "invalid_argument",
			Method: "Get",
			Body:   `{"uuid":""}`,
		},
		{
			// not_found carries no metadata, so this is the plain
			// shape of an error body on either stack.
			Name:   "not_found",
			Method: "Get",
			Body:   `{"uuid":"3f2b1a5e-6f70-4bfe-bcf5-84b3a6a9e2ea"}`,
		},
		{
			// failed_precondition is the one code the two stacks
			// answer with a different HTTP status, 412 on Twirp and
			// 400 on Connect, so the goldens are what stops either
			// from drifting. Document locks, system locks and
			// workflow rule violations all return it.
			Name:   "failed_precondition",
			Method: "Delete",
			Body:   `{"uuid":"` + deleteUUID + `"}`,
		},
	} {
		t.Run(c.Name, func(t *testing.T) {
			twirpRes := call(t,
				"/twirp/elephant.repository.Documents/"+c.Method,
				c.Body)

			test.AgainstGolden(t, regenerate, twirpRes,
				filepath.Join(dataDir, c.Name+"-twirp.json"))

			connectRes := call(t,
				"/elephant.repository.Documents/"+c.Method,
				c.Body)

			test.AgainstGolden(t, regenerate, connectRes,
				filepath.Join(dataDir, c.Name+"-connect.json"))
		})
	}

	// The refusal the authentication middleware writes is the one error body
	// in the API that no handler produces: the middleware answers before
	// either mount runs. Which of the two shapes it writes is decided by the
	// mount the request arrived on and not by the content type, since Twirp
	// and Connect both speak application/json, so if that ever stops working
	// every refusal renders as Connect. The status is 401 either way, which
	// is why only the body catches it.
	t.Run("unauthenticated", func(t *testing.T) {
		// tc.client carries no credentials, so this is a caller with no
		// Authorization header at all.
		twirpRes := tc.postJSON(t, tc.client,
			"/twirp/elephant.repository.Documents/Get", `{"uuid":""}`)

		test.AgainstGolden(t, regenerate, twirpRes,
			filepath.Join(dataDir, "unauthenticated-twirp.json"))

		connectRes := tc.postJSON(t, tc.client,
			"/elephant.repository.Documents/Get", `{"uuid":""}`)

		test.AgainstGolden(t, regenerate, connectRes,
			filepath.Join(dataDir, "unauthenticated-connect.json"))
	})

	// A token the parser rejects is refused by the same middleware, and has
	// to be rendered per stack the same way. Its message carries the
	// parser's own error, so it is held to the shape rather than pinned to
	// a golden.
	t.Run("invalid token", func(t *testing.T) {
		invalid := rawTokenClient(tc.client, "not-a-token")

		for _, c := range []struct {
			Path    string
			Message string
			Absent  string
		}{
			{
				Path:    "/twirp/elephant.repository.Documents/Get",
				Message: "msg",
				Absent:  "message",
			},
			{
				Path:    "/elephant.repository.Documents/Get",
				Message: "message",
				Absent:  "msg",
			},
		} {
			res := tc.postJSON(t, invalid, c.Path, `{"uuid":""}`)

			test.Equalf(t, http.StatusUnauthorized, res.Status,
				"refuse an invalid token on %s", c.Path)

			code, _ := res.Body["code"].(string)

			test.Equalf(t, "unauthenticated", code,
				"report an unauthenticated caller on %s", c.Path)

			_, spelled := res.Body[c.Message]

			test.Equalf(t, true, spelled,
				"spell the message %q on %s", c.Message, c.Path)

			_, other := res.Body[c.Absent]

			test.Equalf(t, false, other,
				"leave out the %q key on %s", c.Absent, c.Path)
		}
	})
}
