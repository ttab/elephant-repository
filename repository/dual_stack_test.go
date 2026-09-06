package repository_test

import (
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
)

// TestSocketErrorCodes locks down the error-code vocabulary the websocket
// protocol borrows from Twirp. The socket answers with the Twirp code strings
// today and will be switched to the Connect ones with the rest of the error
// flip; that switch may not change a single string a client sees, because the
// socket protocol has no version negotiation and a client matches on the
// string.
func TestSocketErrorCodes(t *testing.T) {
	for _, c := range []struct {
		Twirp   twirp.ErrorCode
		Connect connect.Code
	}{
		{Twirp: twirp.InvalidArgument, Connect: connect.CodeInvalidArgument},
		{Twirp: twirp.Unauthenticated, Connect: connect.CodeUnauthenticated},
		{Twirp: twirp.PermissionDenied, Connect: connect.CodePermissionDenied},
		{Twirp: twirp.NotFound, Connect: connect.CodeNotFound},
		{Twirp: twirp.Internal, Connect: connect.CodeInternal},
	} {
		test.Equalf(t, string(c.Twirp), c.Connect.String(),
			"keep the %q socket error code", c.Twirp)
	}
}

// TestIntegrationDualStackParity runs the same calls over both stacks against
// one server and checks that they answer the same way. It is what keeps the
// Twirp mount honest while the handlers still speak the Twirp error vocabulary
// and the Connect mount translates it.
func TestIntegrationDualStackParity(t *testing.T) {
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

	docUUID := "9c0d5b1d-6f70-4bfe-bcf5-84b3a6a9e2ea"

	writeClaims := itest.StandardClaims(t, "doc_read doc_write")

	twirpClient := tc.DocumentsClient(t, writeClaims)
	connectClient := connectTC.DocumentsClient(t, writeClaims)

	doc := baseDocument(docUUID, "article://test/dual-stack")

	_, err := twirpClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Mustf(t, err, "create the document over Twirp")

	// A successful call has to give the same answer on both stacks.
	twirpRes, err := twirpClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get the document over Twirp")

	connectRes, err := connectClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get the document over Connect")

	test.Equalf(t, twirpRes.Version, connectRes.Version,
		"report the same version on both stacks")
	test.Equalf(t, twirpRes.Document.Title, connectRes.Document.Title,
		"report the same document on both stacks")

	// The error cases are the interesting half: code, message and metadata
	// all have to survive the translation.
	noScopeClaims := itest.Claims(t, "nobody", "")

	for _, c := range []struct {
		Name    string
		Request *repository.GetDocumentRequest
		Code    connect.Code
	}{
		{
			Name:    "missing uuid",
			Request: &repository.GetDocumentRequest{},
			Code:    connect.CodeInvalidArgument,
		},
		{
			Name: "unknown document",
			Request: &repository.GetDocumentRequest{
				Uuid: "3f2b1a5e-6f70-4bfe-bcf5-84b3a6a9e2ea",
			},
			Code: connect.CodeNotFound,
		},
	} {
		t.Run(c.Name, func(t *testing.T) {
			_, twirpErr := twirpClient.Get(ctx, c.Request)
			test.IsRPCError(t, twirpErr, c.Code)

			_, connectErr := connectClient.Get(ctx, c.Request)
			test.IsRPCError(t, connectErr, c.Code)

			test.ErrorParity(t, twirpErr, connectErr)
		})
	}

	t.Run("missing scope", func(t *testing.T) {
		req := repository.GetDocumentRequest{Uuid: docUUID}

		_, twirpErr := tc.DocumentsClient(t, noScopeClaims).Get(ctx, &req)
		test.IsRPCError(t, twirpErr, connect.CodePermissionDenied)

		_, connectErr := connectTC.DocumentsClient(
			t, noScopeClaims).Get(ctx, &req)
		test.IsRPCError(t, connectErr, connect.CodePermissionDenied)

		// The scope error carries the scopes the method wanted as
		// metadata; that is the metadata path the ErrorMeta detail has
		// to preserve.
		test.ErrorParity(t, twirpErr, connectErr)
	})
}
