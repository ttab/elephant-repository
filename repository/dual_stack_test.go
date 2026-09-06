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
// protocol borrows from the RPC stacks. The socket now spells its codes with
// connect.Code, and every string it emits has to stay the one Twirp used,
// because the socket protocol has no version negotiation and a client matches
// on the string.
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

// TestIntegrationDualStackParity checks that a successful call answers the
// same way on both stacks. The error paths are covered in depth by
// TestIntegrationErrorParity.
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
}
