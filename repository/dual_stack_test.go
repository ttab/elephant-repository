package repository_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
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

// TestIntegrationSuccessBodies pins the raw JSON body of a successful call on
// both stacks. The two spell field names differently — Twirp marshals with
// UseProtoNames, so a field declared "ref_type" comes back as "ref_type", and
// Connect marshals with protojson's defaults, so the same field comes back as
// "refType" — and that is deliberate (decision 9): a service does not install a
// UseProtoNames codec to make Connect look like Twirp. These goldens are what
// stops either spelling from drifting without anyone noticing, and they are the
// thing to show a raw-fetch caller that is moving off /twirp/.
//
// Validate is the call used because it answers from the document in the request
// alone: no stored state, no timestamps, nothing that would make a golden
// depend on when it ran.
func TestIntegrationSuccessBodies(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		Stack: stackTwirp,
	})

	dataDir := filepath.Join("..", "testdata", t.Name())
	regenerate := regenerateTestFixtures()

	claims := itest.StandardClaims(t, "doc_read doc_write")

	client := tc.authClient(t, tc.client, claims)

	// A document with a block the schema does not declare, so that the
	// validation result carries an entity reference and its multi-word
	// ref_type field is populated.
	doc := baseDocument(
		"7b1a4c2d-8e3f-4a5b-9c6d-0e1f2a3b4c5d",
		"article://test/success-bodies")

	doc.Content = append(doc.Content, &newsdoc.Block{
		Type: "test/undeclared",
	})

	request, err := json.Marshal(map[string]any{
		"document": doc,
	})
	test.Mustf(t, err, "marshal the validate request")

	twirpRes := tc.postJSON(t, client,
		"/twirp/elephant.repository.Documents/Validate", string(request))

	test.AgainstGolden(t, regenerate, twirpRes,
		filepath.Join(dataDir, "validate-twirp.json"))

	connectRes := tc.postJSON(t, client,
		"/elephant.repository.Documents/Validate", string(request))

	test.AgainstGolden(t, regenerate, connectRes,
		filepath.Join(dataDir, "validate-connect.json"))
}

// TestIntegrationGRPC checks that the gRPC protocol Connect serves on the
// Connect paths is actually reachable. It is served over HTTP/2, which Go
// negotiates through the TLS ALPN handshake and nowhere else, so a plaintext
// listener that does not declare unencrypted HTTP/2 answers HTTP/1.1 only and
// every gRPC call fails on the connection rather than in the handler. Nothing
// else in the suite would notice: Twirp, Connect and gRPC-Web are all HTTP/1.1.
func TestIntegrationGRPC(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		Stack: stackConnect,
	})

	claims := itest.StandardClaims(t, "doc_read doc_write")

	client := tc.authClient(t, unencryptedHTTP2Client(), claims)

	docs := repositoryconnect.NewDocumentsServiceClient(
		client, tc.Server.URL, connect.WithGRPC())

	ctx := t.Context()

	docUUID := "2f6c1b3a-9d4e-4f5a-8b7c-1d2e3f4a5b6c"

	doc := baseDocument(docUUID, "article://test/grpc")

	_, err := docs.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Mustf(t, err, "create the document over gRPC")

	res, err := docs.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get the document over gRPC")

	test.Equalf(t, doc.Title, res.Document.Title,
		"return the document over gRPC")
}

// unencryptedHTTP2Client returns a client that speaks HTTP/2 without TLS, which
// is what a gRPC caller inside the cluster does.
func unencryptedHTTP2Client() *http.Client {
	var protocols http.Protocols

	protocols.SetUnencryptedHTTP2(true)

	return &http.Client{
		Transport: &http.Transport{
			Protocols: &protocols,
		},
	}
}
