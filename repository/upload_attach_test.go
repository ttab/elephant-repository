package repository_test

import (
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine/test"
)

func TestAssetUpload(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	regenerate := regenerateTestFixtures()
	dataDir := filepath.Join("..", "testdata", t.Name())

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete asset_upload eventlog_read"))

	up, err := client.CreateUpload(ctx, &repository.CreateUploadRequest{
		Name:        "my.txt",
		ContentType: "text/plain",
		Meta: map[string]string{
			"some": "random-prop",
		},
	})
	test.Must(t, err, "create upload")

	data := "Hello World\n"

	req, err := http.NewRequest(
		http.MethodPut,
		up.Url,
		strings.NewReader(data))
	test.Must(t, err, "create upload request")

	req.ContentLength = int64(len(data))

	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	test.Must(t, err, "make upload request")

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("error response from upload recipient: %s", res.Status)
	}

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	// Create a document and attach the uploaded document.
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		AttachObjects: map[string]string{
			"plaintext": up.Id,
		},
	})
	test.Must(t, err, "create document with an attachment")

	// Check that the document meta shows the attached object.
	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get meta post-attach")

	test.TestMessageAgainstGolden(t, regenerate, meta,
		filepath.Join(dataDir, "meta-post-attach.json"),
		test.IgnoreTimestamps{})

	// Update the document and detach the object.
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:          docUUID,
		Document:      doc,
		DetachObjects: []string{"plaintext"},
	})
	test.Must(t, err, "detach object")

	// Check that the document meta doesn't show the attached object.
	meta, err = client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get meta post-detach")

	test.TestMessageAgainstGolden(t, regenerate, meta,
		filepath.Join(dataDir, "meta-post-detach.json"),
		test.IgnoreTimestamps{})

	log, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
		After: -5,
	})
	test.Must(t, err, "get eventlog")

	test.TestMessageAgainstGolden(t, regenerate, log,
		filepath.Join(dataDir, "events.json"),
		test.IgnoreTimestamps{})
}
