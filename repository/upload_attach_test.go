package repository_test

import (
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
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
	test.Mustf(t, err, "create upload")

	data := "Hello World\n"

	req, err := http.NewRequest(
		http.MethodPut,
		up.Url,
		strings.NewReader(data))
	test.Mustf(t, err, "create upload request")

	req.ContentLength = int64(len(data))

	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	test.Mustf(t, err, "make upload request")

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
	test.Mustf(t, err, "create document with an attachment")

	// Check that the document meta shows the attached object.
	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get meta post-attach")

	test.MessageAgainstGolden(t, regenerate, meta,
		filepath.Join(dataDir, "meta-post-attach.json"),
		test.IgnoreTimestamps{},
		ignoreUUIDField("nonce"),
	)

	downloadLinks, err := client.GetAttachments(ctx,
		&repository.GetAttachmentsRequest{
			Documents:      []string{docUUID},
			AttachmentName: "plaintext",
			DownloadLink:   true,
		})
	test.Mustf(t, err, "get download links")

	test.Equalf(t, 1, len(downloadLinks.Attachments),
		"get download info for the attachment")

	test.Equalf(t, "my.txt", downloadLinks.Attachments[0].Filename,
		"get the correct file name back")

	test.Equalf(t, "text/plain", downloadLinks.Attachments[0].ContentType,
		"get the correct content type back")

	downloadRes, err := http.Get(downloadLinks.Attachments[0].DownloadLink)
	test.Mustf(t, err, "make download request")

	defer downloadRes.Body.Close()

	dowloadedData, err := io.ReadAll(downloadRes.Body)
	test.Mustf(t, err, "read download data")

	test.Equalf(t, data, string(dowloadedData), "get the correct data in download")

	// Update the document and detach the object.
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:          docUUID,
		Document:      doc,
		DetachObjects: []string{"plaintext"},
	})
	test.Mustf(t, err, "detach object")

	// Check that the document meta doesn't show the attached object.
	meta, err = client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "get meta post-detach")

	test.MessageAgainstGolden(t, regenerate, meta,
		filepath.Join(dataDir, "meta-post-detach.json"),
		test.IgnoreTimestamps{},
		ignoreUUIDField("nonce"))

	log := collectEventlog(t, client, 2, 5*time.Second)

	test.MessageAgainstGolden(t, regenerate, log,
		filepath.Join(dataDir, "events.json"),
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"),
	)
}

func TestAssetRestore(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	regenerate := regenerateTestFixtures()
	dataDir := filepath.Join("..", "testdata", t.Name())

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete doc_restore asset_upload eventlog_read"))

	up, err := client.CreateUpload(ctx, &repository.CreateUploadRequest{
		Name:        "my.txt",
		ContentType: "text/plain",
		Meta: map[string]string{
			"some": "random-prop",
		},
	})
	test.Mustf(t, err, "create upload")

	data := "Hello World\n"

	req, err := http.NewRequest(
		http.MethodPut,
		up.Url,
		strings.NewReader(data))
	test.Mustf(t, err, "create upload request")

	req.ContentLength = int64(len(data))

	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	test.Mustf(t, err, "make upload request")

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
	test.Mustf(t, err, "create document with an attachment")

	// Delete the document.
	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Mustf(t, err, "delete document with an attachment")

	deadline := time.Now().Add(5 * time.Second)

	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delete to be processed")
		}

		time.Sleep(100 * time.Millisecond)

		_, err := client.Get(ctx, &repository.GetDocumentRequest{
			Uuid: docUUID,
		})
		//nolint: gocritic
		if elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition) {
			continue
		} else if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			break
		} else if err != nil {
			t.Fatalf("failed to check document delete status: %v", err)
		}

		t.Fatal("document should not exist")
	}

	deleted, err := client.ListDeleted(ctx, &repository.ListDeletedRequest{
		Uuid: doc.Uuid,
	})
	test.Mustf(t, err, "get deleted versions of the document")

	test.Equalf(t, 1, len(deleted.Deletes), "get one delete entry")

	_, err = client.Restore(ctx, &repository.RestoreRequest{
		Uuid:           docUUID,
		DeleteRecordId: deleted.Deletes[0].Id,
	})
	test.Mustf(t, err, "request a restore")

	var (
		lastID      int64
		gotFinEvent bool
		events      []*repository.EventlogItem
	)

	eventPollStarted := time.Now()

	// Read the eventlog until we get a "restore_finished" event for our doc.
	for !gotFinEvent {
		if time.Since(eventPollStarted) > 10*time.Second {
			t.Fatal("timed out waiting for restore finished event")
		}

		res, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			After:  lastID,
			WaitMs: 200,
		})
		if err != nil {
			t.Fatalf("read eventlog: %v", err)
		}

		for _, evt := range res.Items {
			gotFinEvent = gotFinEvent || (evt.Uuid == docUUID &&
				evt.Event == "restore_finished")

			lastID = evt.Id
			events = append(events, evt)
		}
	}

	test.MessageAgainstGolden(t, regenerate,
		&repository.GetEventlogResponse{
			Items: events,
		},
		filepath.Join(dataDir, "events.json"),
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"))

	downloadLinks, err := client.GetAttachments(ctx,
		&repository.GetAttachmentsRequest{
			Documents:      []string{docUUID},
			AttachmentName: "plaintext",
			DownloadLink:   true,
		})
	test.Mustf(t, err, "get download links")

	test.Equalf(t, 1, len(downloadLinks.Attachments),
		"get download info for the attachment")

	downloadRes, err := http.Get(downloadLinks.Attachments[0].DownloadLink)
	test.Mustf(t, err, "make download request")

	defer downloadRes.Body.Close()

	dowloadedData, err := io.ReadAll(downloadRes.Body)
	test.Mustf(t, err, "read download data")

	test.Equalf(t, data, string(dowloadedData), "get the correct data in download")
}
