package repository_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"

	"connectrpc.com/connect"
	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	itest "github.com/ttab/elephant-repository/internal/test"
	repo "github.com/ttab/elephant-repository/repository"
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

// uncodedDocuments answers Get with an error that carries no RPC code, the
// shape of the handler returns that are still a plain fmt.Errorf on a failed
// query. The embedded interface is nil, so every other method panics if called,
// which is what keeps the stub honest about what it covers.
type uncodedDocuments struct {
	repository.Documents
}

func (uncodedDocuments) Get(
	_ context.Context, _ *repository.GetDocumentRequest,
) (*repository.GetDocumentResponse, error) {
	return nil, errors.New("something the handler did not code")
}

// TestUncodedErrorParity checks that a handler error with no RPC code is
// answered with internal on both stacks. Twirp codes it through
// twirp.InternalErrorWith and Connect would default it to unknown, so the
// Connect mount installs an interceptor that codes it the same way. Without
// that the two stacks disagree for every uncoded handler error, and
// code="unknown" stops meaning what docs/observability.md says it means.
func TestUncodedErrorParity(t *testing.T) {
	router := httprouter.New()

	// No AuthMiddleware: the stub asserts nothing about the caller, and the
	// point here is the error coding, not the authorization.
	err := repo.SetUpRouter(router,
		repo.WithDocumentsAPI(
			uncodedDocuments{}, repo.ServerOptions{}))
	test.Mustf(t, err, "set up the router")

	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	call := func(client repository.Documents) error {
		_, err := client.Get(t.Context(), &repository.GetDocumentRequest{
			Uuid: "e6cbd7a0-6ff6-4c76-9a2a-8f7e2b1c0d9e",
		})

		return err
	}

	twirpErr := call(repository.NewDocumentsProtobufClient(
		server.URL, server.Client()))
	test.IsRPCError(t, twirpErr, connect.CodeInternal)

	connectErr := call(repositoryconnect.NewDocumentsServiceClient(
		server.Client(), server.URL))
	test.IsRPCError(t, connectErr, connect.CodeInternal)
}

// errorResponse is the shape the error body golden files are stored in: the
// status the stack answered with, and the parsed body.
type errorResponse struct {
	Status int            `json:"status"`
	Body   map[string]any `json:"body"`
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

	call := func(t *testing.T, path string, body string) errorResponse {
		t.Helper()

		req, err := http.NewRequestWithContext(t.Context(),
			http.MethodPost, tc.Server.URL+path,
			bytes.NewBufferString(body))
		test.Mustf(t, err, "create the request")

		req.Header.Set("Content-Type", "application/json")

		res, err := client.Do(req)
		test.Mustf(t, err, "perform the request")

		defer func() {
			_ = res.Body.Close()
		}()

		data, err := io.ReadAll(res.Body)
		test.Mustf(t, err, "read the response body")

		out := errorResponse{Status: res.StatusCode}

		err = json.Unmarshal(data, &out.Body)
		test.Mustf(t, err, "unmarshal the response body %q", string(data))

		return out
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
}
