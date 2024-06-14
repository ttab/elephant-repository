package repository_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/tmaxmax/go-sse"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func baseDocument(uuid, uri string) *newsdoc.Document {
	return &newsdoc.Document{
		Uuid:     uuid,
		Title:    "A bare-bones article",
		Type:     "core/article",
		Uri:      uri,
		Language: "en",
	}
}

const (
	idField        = "id"
	timestampField = "timestamp"
)

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

	sseConn := tc.SSEConnect(t, []string{"firehose"},
		itest.StandardClaims(t, "eventlog_read"))

	sseChan := make(chan *repository.EventlogItem, 10)

	sseConn.SubscribeToAll(func(e sse.Event) {
		var evt repository.EventlogItem

		err := protojson.Unmarshal([]byte(e.Data), &evt)
		test.Must(t, err, "decode SSE event %q", e.LastEventID)

		sseChan <- &evt
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := test.Context(t)

	t.Run("NegativeEventlogAfterOnEmpty", func(t *testing.T) {
		log, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			After: -1,
		})

		test.Must(t, err, "get eventlog")
		test.Equal(t, 0, len(log.Items),
			"no eventlog items should be returned")
	})

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
			return k == timestampField
		}),
	)
	if diff != "" {
		t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
	}

	sseDeadline := time.After(5 * time.Second)

	var sseEvents []*repository.EventlogItem

	for {
		select {
		case <-sseDeadline:
			t.Fatal("timed out waiting for SSE events")
		case item := <-sseChan:
			sseEvents = append(sseEvents, item)
		}

		if len(sseEvents) == len(events.Items) {
			break
		}
	}

	sseDiff := cmp.Diff(events.Items, sseEvents,
		protocmp.Transform(),
	)
	if sseDiff != "" {
		t.Fatalf("sse <-> eventlog mismatch (-want +got):\n%s", diff)
	}
}

func TestIntegrationStatusPermissions(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	statusClaims := itest.Claims(t, "status-guy", "doc_write")
	statusClient := tc.DocumentsClient(t, statusClaims)

	randoClient := tc.DocumentsClient(t,
		itest.Claims(t, "random-guy", "doc_write"))

	ctx := test.Context(t)

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	// Create a document that Mr status-guy is allowed to set statuses on.
	res, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		Acl: []*repository.ACLEntry{
			{
				Uri:         statusClaims.Subject,
				Permissions: []string{"s"},
			},
		},
	})
	test.Must(t, err, "create article")

	// Set a status as Mr status-guy.
	_, err = statusClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{
				Name:    "done",
				Version: res.Version,
			},
		},
	})
	test.Must(t, err, "set article status")

	// Check that we're not letting any rando set statuses.
	_, err = randoClient.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{
				Name:    "usable",
				Version: res.Version,
			},
		},
	})
	test.MustNot(t, err, "let random user set article status")

	// Verify that we don't let Mr status-guy mess with the contents of
	// documents.
	_, err = statusClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	itest.IsTwirpError(t, err, twirp.PermissionDenied)

	// Don't allow a status to be set for a document that doesn't exist.
	_, err = statusClient.Update(ctx, &repository.UpdateRequest{
		Uuid: "a71c719b-d42e-4a60-95ea-3e6f7e897812",
		Status: []*repository.StatusUpdate{
			{
				Name:    "usable",
				Version: 1,
			},
		},
	})
	itest.IsTwirpError(t, err, twirp.NotFound)
}

func TestIntegrationDocumentLanguage(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	var expectedEvents int64

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:   false,
		RunReplicator: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	t.Run("NoLanguage", func(t *testing.T) {
		doc := baseDocument(
			"1d450529-fd98-4e4e-80c2-f70f9194737a",
			"example://1d450529-fd98-4e4e-80c2-f70f9194737a",
		)

		doc.Language = ""

		_, err := client.Update(test.Context(t), &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})
		test.Must(t, err, "update a document without a language")

		// Doc + ACL
		expectedEvents += 2
	})

	t.Run("InvalidLanguage", func(t *testing.T) {
		doc := baseDocument(
			"1d450529-fd98-4e4e-80c2-f70f9194737a",
			"example://1d450529-fd98-4e4e-80c2-f70f9194737a",
		)

		doc.Language = "ad"

		_, err := client.Update(test.Context(t), &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})
		test.MustNot(t, err, "update a document with a invalid language")
	})

	t.Run("InvalidCodeFormat", func(t *testing.T) {
		doc := baseDocument(
			"1d450529-fd98-4e4e-80c2-f70f9194737a",
			"example://1d450529-fd98-4e4e-80c2-f70f9194737a",
		)

		doc.Language = "sv-"

		_, err := client.Update(test.Context(t), &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})
		test.MustNot(t, err, "update a document with a malformed language code")
	})

	t.Run("LanguageAndRegion", func(t *testing.T) {
		ctx := test.Context(t)

		doc := baseDocument(
			"00668ca7-aca9-4e7e-8958-c67d48a3e0d2",
			"example://00668ca7-aca9-4e7e-8958-c67d48a3e0d2",
		)

		doc.Language = "en-GB"

		info1, err := client.Update(ctx, &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})
		test.Must(t, err, "update a document with a valid language and region")
		_, err = client.Update(ctx, &repository.UpdateRequest{
			Uuid: doc.Uuid,
			Status: []*repository.StatusUpdate{
				{
					Name:    "usable",
					Version: info1.Version,
				},
			},
		})
		test.Must(t, err, "set version one as usable")

		doc.Language = "en-US"

		info2, err := client.Update(ctx, &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})
		test.Must(t, err, "update a document with a new language")

		_, err = client.Update(ctx, &repository.UpdateRequest{
			Uuid: doc.Uuid,
			Status: []*repository.StatusUpdate{
				{
					Name:    "usable",
					Version: info2.Version,
				},
			},
		})
		test.Must(t, err, "set version two as usable")

		expectedEvents += 5

		// Wait until the expected events have been registered.
		_, err = client.Eventlog(ctx,
			&repository.GetEventlogRequest{
				After: expectedEvents - 1,
			})
		test.Must(t, err, "wait for eventlog")

		log, err := client.Eventlog(ctx,
			&repository.GetEventlogRequest{
				After: -5,
			})
		test.Must(t, err, "get eventlog")
		test.Equal(t, 5, len(log.Items),
			"get the expected number of events")

		var golden repository.GetEventlogResponse

		err = elephantine.UnmarshalFile(
			"testdata/TestIntegrationDocumentLanguage/eventlog.json",
			&golden)
		test.Must(t, err, "read golden file for expected eventlog items")

		diff := cmp.Diff(&golden, log,
			protocmp.Transform(),
			cmpopts.IgnoreMapEntries(func(k string, _ interface{}) bool {
				return k == timestampField ||
					k == idField
			}),
		)
		if diff != "" {
			t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestDocumentsServiceMetaDocuments(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:   true,
		RunReplicator: true,
	})

	ctx := test.Context(t)

	schema := tc.SchemasClient(t, itest.StandardClaims(t, "schema_admin"))

	spec := revisor.ConstraintSet{
		Version: 1,
		Name:    "test_metadata",
		Documents: []revisor.DocumentConstraint{
			{
				Name:     "Simple metadata",
				Declares: "test/metadata",
				Meta: []*revisor.BlockConstraint{
					{
						Declares: &revisor.BlockSignature{
							Type: "meta/annotation",
						},
						Attributes: revisor.ConstraintMap{
							"title": revisor.StringConstraint{},
						},
					},
				},
			},
		},
	}

	specPayload, err := json.Marshal(&spec)
	test.Must(t, err, "marshal meta schema")

	_, err = schema.Register(ctx, &repository.RegisterSchemaRequest{
		Activate: true,
		Schema: &repository.Schema{
			Name:    "test/metadata",
			Version: "v1.0.0",
			Spec:    string(specPayload),
		},
	})
	test.Must(t, err, "register metadata schema")

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	wMetaClient := tc.DocumentsClient(t,
		itest.Claims(t, "wmc", "meta_doc_write_all"))

	docA := baseDocument(
		"14f4ba22-f7c0-46bc-9c18-73f845a4f801", "article://test/a",
	)

	docB := baseDocument(
		"92ebfc38-5a1e-40c3-b316-3d06f488526e", "article://test/b",
	)

	metaDoc := newsdoc.Document{
		Type:  "test/metadata",
		Title: "A meta document",
		Meta: []*newsdoc.Block{
			{
				Type:  "meta/annotation",
				Title: "A meta block",
			},
		},
	}

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:               docA.Uuid,
		IfMatch:            -1,
		Document:           &metaDoc,
		UpdateMetaDocument: true,
	})
	isTwirpError(t, err, "create a meta doc for a document that doesn't exist",
		twirp.FailedPrecondition)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docA.Uuid,
		Document: docA,
	})
	test.Must(t, err, "create a basic article")

	_, err = wMetaClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     docB.Uuid,
		Document: docB,
	})
	isTwirpError(t, err,
		"client with metadata write all must not be able to create normal documents",
		twirp.PermissionDenied,
	)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docB.Uuid,
		Document: docB,
	})
	test.Must(t, err, "create a second basic article")

	_, err = wMetaClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     docB.Uuid,
		Document: docB,
	})
	isTwirpError(t, err,
		"client with metadata write all must not be able to update normal documents",
		twirp.PermissionDenied,
	)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:               docA.Uuid,
		IfMatch:            -1,
		Document:           &metaDoc,
		UpdateMetaDocument: true,
	})
	isTwirpError(t, err, "create a meta doc for a document that doesn't have a configured meta type",
		twirp.InvalidArgument)

	_, err = schema.RegisterMetaTypeUse(ctx, &repository.RegisterMetaTypeUseRequest{
		MainType: "core/article",
		MetaType: "test/metadata",
	})
	isTwirpError(t, err, "register the meta type for use before registering the type itself",
		twirp.InvalidArgument)

	_, err = schema.RegisterMetaType(ctx, &repository.RegisterMetaTypeRequest{
		Type:      "test/metadata",
		Exclusive: true,
	})
	test.Must(t, err, "register the meta type")

	_, err = schema.RegisterMetaTypeUse(ctx, &repository.RegisterMetaTypeUseRequest{
		MainType: "core/article",
		MetaType: "test/metadata",
	})
	test.Must(t, err, "register the meta type for use with articles")

	mRes, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:               docA.Uuid,
		IfMatch:            -1,
		Document:           &metaDoc,
		UpdateMetaDocument: true,
	})
	test.Must(t, err, "create a meta doc for a document")

	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: mRes.Uuid,
	})
	test.Must(t, err, "get meta information about meta doc")

	wantMeta := repository.DocumentMeta{
		CurrentVersion: 1,
		IsMetaDocument: true,
		MainDocument:   docA.Uuid,
	}

	ignoreTimeVariantValues := cmpopts.IgnoreMapEntries(
		func(k string, _ interface{}) bool {
			switch k {
			case "created", "modified":
				return true
			}

			return false
		})

	cmpDiff := cmp.Diff(&wantMeta, meta.Meta,
		protocmp.Transform(),
		ignoreTimeVariantValues,
	)
	if cmpDiff != "" {
		t.Fatalf("meta document meta mismatch (-want +got):\n%s", cmpDiff)
	}

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:               mRes.Uuid,
		IfMatch:            1,
		Document:           &metaDoc,
		UpdateMetaDocument: true,
	})
	isTwirpError(t, err, "must not create a meta doc for a meta doc",
		twirp.InvalidArgument)

	mDocV1, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid: mRes.Uuid,
	})
	test.Must(t, err, "be able to read meta document")

	wantDoc := proto.Clone(&metaDoc).(*newsdoc.Document)

	wantDoc.Uuid = mRes.Uuid
	// Generated from main document.
	wantDoc.Uri = fmt.Sprintf("system://%s/meta", docA.Uuid)
	// From default language.
	wantDoc.Language = "sv-se"

	test.EqualMessage(t, &repository.GetDocumentResponse{
		Document:       wantDoc,
		Version:        1,
		IsMetaDocument: true,
		MainDocument:   docA.Uuid,
	}, mDocV1, "get the expected document back")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docA.Uuid,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: 1},
		},
	})
	test.Must(t, err, "set a usable status")

	mDocV2 := proto.Clone(mDocV1.Document).(*newsdoc.Document)
	mDocV2.Title = "I am the the second"

	_, err = wMetaClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     mRes.Uuid,
		IfMatch:  1,
		Document: mDocV2,
	})
	test.Must(t, err, "be able to update a meta doc directly")

	sneakyDoc := proto.Clone(mDocV2).(*newsdoc.Document)

	sneakyDoc.Uri = fmt.Sprintf("core://article/%s", sneakyDoc.Uuid)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     sneakyDoc.Uuid,
		IfMatch:  2,
		Document: sneakyDoc,
	})
	isTwirpError(t, err, "must not be able to change meta doc into a normal doc",
		twirp.InvalidArgument)

	sneakyDoc2 := proto.Clone(docA).(*newsdoc.Document)

	sneakyDoc2.Uri = fmt.Sprintf("system://%s/meta", docB.Uuid)
	sneakyDoc2.Type = "test/metadata"

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docA.Uuid,
		IfMatch:  1,
		Document: sneakyDoc2,
	})
	isTwirpError(t, err, "must not be able to change a normal doc into a meta doc",
		twirp.InvalidArgument)

	docWithMetaDoc, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:         docA.Uuid,
		MetaDocument: repository.GetMetaDoc_META_INCLUDE,
	})
	test.Must(t, err, "be able to fetch document with meta doc")

	test.EqualMessage(t, &repository.GetDocumentResponse{
		Document: docA,
		Version:  1,
		Meta: &repository.MetaDocument{
			Version:  2,
			Document: mDocV2,
		},
	}, docWithMetaDoc, "get the expected document back")

	docWithStatus, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:         docA.Uuid,
		MetaDocument: repository.GetMetaDoc_META_INCLUDE,
		Status:       "usable",
	})
	test.Must(t, err, "be able to fetch document with meta doc by status")

	test.EqualMessage(t, &repository.GetDocumentResponse{
		Document: docA,
		Version:  1,
		// We expect the meta doc to reflect the version it had at the
		// time the status was set.
		Meta: &repository.MetaDocument{
			Version:  1,
			Document: mDocV1.Document,
		},
	}, docWithStatus, "get the expected document back for usable 1")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docA.Uuid,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: 1},
		},
	})
	test.Must(t, err, "set a second usable status")

	docWithStatus2, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:         docA.Uuid,
		MetaDocument: repository.GetMetaDoc_META_INCLUDE,
		Status:       "usable",
	})
	test.Must(t, err, "be able to fetch document with meta doc by status")

	test.EqualMessage(t, &repository.GetDocumentResponse{
		Document: docA,
		Version:  1,
		// We expect the meta doc to reflect the version it had at the
		// time the status was set.
		Meta: &repository.MetaDocument{
			Version:  2,
			Document: mDocV2,
		},
	}, docWithStatus2, "get the expected document back for usable 2")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docA.Uuid,
	})
	test.Must(t, err, "delete main document")

	readAllClient := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read_all"))

	// Check that we get not found or failed precondition for the deleted
	// docs.
	//
	// TODO: wait for transition from failed precondition to not found.

	_, err = readAllClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docA.Uuid,
	})
	isTwirpError(t, err, "get article", twirp.NotFound, twirp.FailedPrecondition)

	_, err = readAllClient.Get(ctx, &repository.GetDocumentRequest{
		Uuid: mDocV1.Document.Uuid,
	})
	isTwirpError(t, err, "get meta doc", twirp.NotFound, twirp.FailedPrecondition)
}

func isTwirpError(
	t *testing.T, err error, message string, code ...twirp.ErrorCode,
) {
	t.Helper()

	if err == nil {
		t.Fatalf("failed: %s: expected one of the error codes %q, didn't get an error",
			message, code)
	}

	var tErr twirp.Error

	ok := errors.As(err, &tErr)

	if !ok || !slices.Contains(code, tErr.Code()) {
		t.Fatalf("failed: %s: expected one of the error codes %q: got %v",
			message, code, err)
	}

	if testing.Verbose() {
		t.Logf("success: %s: got a %q error: %v",
			message, code, err)
	}
}

func TestIntegrationBulkCrud(t *testing.T) {
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

	docA := baseDocument(
		"14f4ba22-f7c0-46bc-9c18-73f845a4f801", "article://test/a",
	)

	docB := baseDocument(
		"3e885433-3f89-4f37-96d1-b3b9802b2bc6", "article://test/b",
	)

	res, err := client.BulkUpdate(ctx, &repository.BulkUpdateRequest{
		Updates: []*repository.UpdateRequest{
			{
				Uuid:     docA.Uuid,
				Document: docA,
			},
			{
				Uuid:     docB.Uuid,
				Document: docB,
				Status: []*repository.StatusUpdate{
					{Name: "usable"},
				},
			},
		},
	})
	test.Must(t, err, "create articles")

	test.EqualMessage(t,
		&repository.BulkUpdateResponse{
			Updates: []*repository.UpdateResponse{
				{
					Uuid:    docA.Uuid,
					Version: 1,
				},
				{
					Uuid:    docB.Uuid,
					Version: 1,
				},
			},
		},
		res,
		"expected this to be the first versions")

	resA, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docA.Uuid,
	})
	test.Must(t, err, "be able to load document A")

	test.EqualMessage(t, docA, resA.Document,
		"expect to get the same document A back")

	resB, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docB.Uuid,
	})
	test.Must(t, err, "be able to load document B")

	test.EqualMessage(t, docB, resB.Document,
		"expect to get the same document B back")

	const (
		testUserURI = "user://test/testintegrationbulkcrud"
	)

	ignoreTimeVariantValues := cmpopts.IgnoreMapEntries(
		func(k string, _ interface{}) bool {
			switch k {
			case "created", "modified":
				return true
			}

			return false
		})

	metaA, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docA.Uuid,
	})
	test.Must(t, err, "be able to load document A metadata")

	wantMetaA := repository.DocumentMeta{
		Acl: []*repository.ACLEntry{
			{
				Uri:         testUserURI,
				Permissions: []string{"r", "w"},
			},
		},
		CurrentVersion: 1,
	}

	cmpDiffA := cmp.Diff(&wantMetaA, metaA.Meta,
		protocmp.Transform(),
		ignoreTimeVariantValues,
	)
	if cmpDiffA != "" {
		t.Fatalf("document A meta mismatch (-want +got):\n%s", cmpDiffA)
	}

	metaB, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docB.Uuid,
	})
	test.Must(t, err, "be able to load document B metadata")

	wantMetaB := repository.DocumentMeta{
		Acl: []*repository.ACLEntry{
			{
				Uri:         testUserURI,
				Permissions: []string{"r", "w"},
			},
		},
		CurrentVersion: 1,
		Heads: map[string]*repository.Status{
			"usable": {
				Id:      1,
				Version: 1,
				Creator: testUserURI,
			},
		},
	}

	cmpDiffB := cmp.Diff(&wantMetaB, metaB.Meta,
		protocmp.Transform(),
		ignoreTimeVariantValues,
	)
	if cmpDiffB != "" {
		t.Fatalf("document A meta mismatch (-want +got):\n%s", cmpDiffB)
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
			return k == timestampField
		}),
	)
	if diff != "" {
		t.Fatalf("eventlog mismatch (-want +got):\n%s", diff)
	}

	var compactGolden repository.GetCompactedEventlogResponse

	err = elephantine.UnmarshalFile(
		"testdata/TestIntegrationStatus/compact_eventlog.json",
		&compactGolden)
	test.Must(t, err, "read golden file for expected compact eventlog items")

	lastEvt, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
		After: -1,
	})
	test.Must(t, err, "failed to get last event")

	if len(lastEvt.Items) != 1 {
		t.Fatalf("expected after=-1 to yield one event, got %d",
			len(lastEvt.Items))
	}

	compactEvents, err := client.CompactedEventlog(ctx,
		&repository.GetCompactedEventlogRequest{
			Until: lastEvt.Items[0].Id,
		})
	test.Must(t, err, "failed to get compacted eventlog")

	cmpDiff := cmp.Diff(&compactGolden, compactEvents,
		protocmp.Transform(),
		cmpopts.IgnoreMapEntries(func(k string, _ interface{}) bool {
			return k == timestampField
		}),
	)
	if cmpDiff != "" {
		t.Fatalf("compact eventlog mismatch (-want +got):\n%s", cmpDiff)
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
	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	const (
		docUUID = "88f13bde-1a84-4151-8f2d-aaee3ae57c05"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Lock(ctx, &repository.LockRequest{
		Uuid: docUUID,
		Ttl:  500,
	})
	test.MustNot(t, err, "lock non-existing article")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create test article")

	lock, err := client.Lock(ctx, &repository.LockRequest{
		Uuid:    docUUID,
		Ttl:     500,
		App:     "app",
		Comment: "my comment",
	})
	test.Must(t, err, "lock the document")

	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "fetch document meta")
	test.NotNil(t, meta.Meta.Lock, "document should have a lock")
	test.Equal(t, meta.Meta.Lock.Uri, "user://test/testdocumentlocking", "expected uri set")
	test.Equal(t, meta.Meta.Lock.App, "app", "expected app set")
	test.Equal(t, meta.Meta.Lock.Comment, "my comment", "expected comment set")

	_, err = client.Lock(ctx, &repository.LockRequest{
		Uuid: docUUID,
		Ttl:  500,
	})
	test.MustNot(t, err, "re-lock the document")

	_, err = client.Lock(ctx, &repository.LockRequest{
		Uuid: docUUID,
		Ttl:  500,
	})
	test.MustNot(t, err, "steal an existing lock")

	_, err = client.ExtendLock(ctx, &repository.ExtendLockRequest{
		Uuid:  docUUID,
		Ttl:   500,
		Token: "4ab0330e-7cd7-4a75-b1f9-5ee8b098e333",
	})
	test.MustNot(t, err, "extend a lock with the wrong token")

	_, err = client.ExtendLock(ctx, &repository.ExtendLockRequest{
		Uuid:  docUUID,
		Ttl:   1,
		Token: lock.Token,
	})
	test.Must(t, err, "extend an existing lock")

	time.Sleep(time.Millisecond * 100)

	_, err = client.ExtendLock(ctx, &repository.ExtendLockRequest{
		Uuid:  docUUID,
		Ttl:   500,
		Token: lock.Token,
	})
	test.MustNot(t, err, "re-lock an expired lock")

	lock, err = client.Lock(ctx, &repository.LockRequest{
		Uuid: docUUID,
		Ttl:  500,
	})
	test.Must(t, err, "create a new lock")

	meta, err = client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "fetch document meta")
	test.NotNil(t, meta.Meta.Lock, "document should have a lock")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.MustNot(t, err, "update a locked document")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:      docUUID,
		Document:  doc,
		LockToken: lock.Token,
	})
	test.Must(t, err, "update a locked document with the correct token")

	_, err = client.Unlock(ctx, &repository.UnlockRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "unlock a document without a token")

	_, err = client.Unlock(ctx, &repository.UnlockRequest{
		Uuid:  docUUID,
		Token: "4ab0330e-7cd7-4a75-b1f9-5ee8b098e333",
	})
	test.MustNot(t, err, "unlock a document with the wrong token")

	_, err = client.Unlock(ctx, &repository.UnlockRequest{
		Uuid:  docUUID,
		Token: lock.Token,
	})
	test.Must(t, err, "unlock the document with the correct token")

	_, err = client.Unlock(ctx, &repository.UnlockRequest{
		Uuid:  docUUID,
		Token: "4ab0330e-7cd7-4a75-b1f9-5ee8b098e333",
	})
	test.Must(t, err, "unlock an unlocked document with an arbitrary token")

	meta, err = client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "fetch document meta")

	if meta.Meta.Lock != nil {
		t.Fatalf("expected lock deleted, got: %v", meta.Meta.Lock)
	}

	lock, err = client.Lock(ctx, &repository.LockRequest{
		Uuid: docUUID,
		Ttl:  500,
	})
	test.Must(t, err, "create a new lock")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "delete a locked document")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid:      docUUID,
		LockToken: lock.Token,
	})
	test.Must(t, err, "delete a locked document with the correct token")

	_, err = client.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid:      "59b9d054-c0ec-4a3e-ab4c-67aa5a9b5b6e",
		LockToken: lock.Token,
	})
	test.Must(t, err, "unlock non-existing document")
}
