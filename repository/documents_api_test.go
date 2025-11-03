package repository_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
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

func basePlanningDocument(planningUUID, assignmentUUID, deliverableUUID, eventUUID string) *newsdoc.Document {
	d := &newsdoc.Document{
		Uuid:     planningUUID,
		Title:    "A planning-item",
		Type:     "core/planning-item",
		Uri:      "core://newscoverage/" + planningUUID,
		Language: "en",
		Meta: []*newsdoc.Block{
			{
				Type: "core/planning-item",
				Data: map[string]string{
					"start_date": "2025-04-28",
					"end_date":   "2025-04-28",
					"tentative":  "false",
				},
			},
			{
				Type:  "core/newsvalue",
				Value: "3",
			},
		},
	}

	if assignmentUUID != "" {
		a := &newsdoc.Block{
			Id:   assignmentUUID,
			Type: "core/assignment",
			Data: map[string]string{
				"full_day":   "false",
				"start":      "2025-04-27T22:00:00.000Z",
				"start_date": "2025-04-28",
				"end_date":   "2025-04-28",
				"public":     "true",
			},
			Meta: []*newsdoc.Block{
				{
					Type:  "core/assignment-type",
					Value: "text",
				},
			},
		}

		if deliverableUUID != "" {
			a.Links = []*newsdoc.Block{
				{
					Uuid: deliverableUUID,
					Type: "core/article",
					Rel:  "deliverable",
				},
			}
		}

		d.Meta = append(d.Meta, a)
	}

	if eventUUID != "" {
		d.Links = []*newsdoc.Block{
			{
				Uuid:  eventUUID,
				Type:  "core/event",
				Title: "An event",
				Rel:   "event",
			},
		}
	}

	return d
}

func baseDocument(uuid, uri string) *newsdoc.Document {
	return &newsdoc.Document{
		Uuid:     uuid,
		Title:    "A bare-bones article",
		Type:     "core/article",
		Uri:      uri,
		Language: "en",
		Meta: []*newsdoc.Block{
			{
				Type:  "core/newsvalue",
				Value: "3",
			},
		},
	}
}

func TestIntegrationBasicCrud(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	dataDir := filepath.Join("..", "testdata", t.Name())

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
		ConfigDirectory:    dataDir,
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

	mClient := tc.MetricsClient(t,
		itest.StandardClaims(t, "metrics_read"))

	ctx := t.Context()

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

	charCount, err := mClient.GetMetrics(ctx, &repository.GetMetricsRequest{
		Uuids: []string{doc.Uuid},
		Kinds: []string{"charcount"},
	})
	test.Must(t, err, "get initial charcount")

	test.EqualMessage(t,
		singleMetricResponse(doc.Uuid, "charcount", "", 0),
		charCount, "get the expected metrics")

	doc2 := test.CloneMessage(doc)

	doc2.Content = append(doc2.Content, &newsdoc.Block{
		Type: "core/text",
		Role: "heading-1",
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

	charCount2, err := mClient.GetMetrics(ctx, &repository.GetMetricsRequest{
		Uuids: []string{doc.Uuid},
		Kinds: []string{"charcount"},
	})
	test.Must(t, err, "get initial charcount")

	test.EqualMessage(t,
		singleMetricResponse(doc.Uuid, "charcount", "", 24),
		charCount2, "get the expected metrics for second version")

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

	const (
		planningUUID   = "88040be2-2e30-4f34-8a9e-3f4802bab8cd"
		assignmentUUID = "e9160776-fdb2-4658-a7d1-ef1b8a434096"
		eventUUID      = "887ade75-97d8-4b29-b15b-8fecb4525775"
	)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     planningUUID,
		Document: basePlanningDocument(planningUUID, assignmentUUID, docUUID, eventUUID),
	})
	test.Must(t, err, "create planning-item")

	deliverableInfo, err := client.GetDeliverableInfo(ctx, &repository.GetDeliverableInfoRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get deliverable info")
	test.EqualMessage(t, &repository.GetDeliverableInfoResponse{
		HasPlanningInfo: true,
		PlanningUuid:    planningUUID,
		AssignmentUuid:  assignmentUUID,
		EventUuid:       eventUUID,
	}, deliverableInfo, "get expected deliverable info")

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

	goldenEventlog := "testdata/TestIntegrationBasicCrud/eventlog.json"

	events, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
		BatchSize:   50,
		BatchWaitMs: 500,
	})
	test.Must(t, err, "get eventlog")

	test.TestMessageAgainstGolden(t, regenerateTestFixtures(),
		events, goldenEventlog,
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"),
	)

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
		t.Fatalf("sse <-> eventlog mismatch (-want +got):\n%s", sseDiff)
	}
}

func singleMetricResponse(uuid string, kind string, label string, value int64) *repository.GetMetricsResponse {
	return &repository.GetMetricsResponse{
		Documents: map[string]*repository.DocumentMetrics{
			uuid: {
				Metrics: []*repository.Metric{
					{
						Kind:  kind,
						Label: label,
						Value: value,
					},
				},
			},
		},
	}
}

// TestIntegrationCreateCollision was added as a regression test for the error:
// 'failed to update document: failed to create version in database: ERROR:
// duplicate key value violates unique constraint "document_version_pkey"
// (SQLSTATE 23505)'. This error occurred when two clients attempted to create a
// document with the same UUID at the same time. Document writes are serialised
// by locking the document row, and when a document is created there is no row
// to lock, so the collision is deferred to the document version insert.
//
// The bugfix detects when document version creation fails with a constraint
// error on the primary key, and reports that as a create conflict (if the
// document is being created) instead of an internal database error.
func TestIntegrationCreateCollision(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write"))

	ctx := t.Context()

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	// Create a signal channel that tells our eager workers to all go at the
	// same time.
	goGo := make(chan struct{})

	var wg sync.WaitGroup

	for range 3 {
		wg.Go(func() {
			<-goGo

			_, err := client.Update(ctx,
				&repository.UpdateRequest{
					Uuid:     docUUID,
					Document: doc,
					IfMatch:  -1,
				})

			switch {
			case elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition):
				t.Log("creation collision")
			case elephantine.IsTwirpErrorCode(err, twirp.AlreadyExists):
				t.Log("optimistic lock triggered")
			case err != nil:
				t.Errorf("failed to create document: %v", err)
			}
		})
	}

	close(goGo)

	wg.Wait()
}

func TestIntegrationStatusPermissions(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	statusClaims := itest.Claims(t, "status-guy", "doc_write")
	statusClient := tc.DocumentsClient(t, statusClaims)

	randoClient := tc.DocumentsClient(t,
		itest.Claims(t, "random-guy", "doc_write"))

	ctx := t.Context()

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

	ctx := t.Context()

	regenerate := regenerateTestFixtures()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	var expectedEvents int64

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        false,
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	t.Run("InvalidLanguage", func(t *testing.T) {
		doc := baseDocument(
			"1d450529-fd98-4e4e-80c2-f70f9194737a",
			"example://1d450529-fd98-4e4e-80c2-f70f9194737a",
		)

		doc.Language = "ad"

		_, err := client.Update(t.Context(), &repository.UpdateRequest{
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

		_, err := client.Update(t.Context(), &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})
		test.MustNot(t, err, "update a document with a malformed language code")
	})

	// Document without a language

	nlDoc := baseDocument(
		"1d450529-fd98-4e4e-80c2-f70f9194737a",
		"example://1d450529-fd98-4e4e-80c2-f70f9194737a",
	)

	nlDoc.Language = ""

	_, err := client.Update(t.Context(), &repository.UpdateRequest{
		Uuid:     nlDoc.Uuid,
		Document: nlDoc,
	})
	test.Must(t, err, "update a document without a language")

	// Doc + ACL
	expectedEvents += 2

	enDoc := baseDocument(
		"00668ca7-aca9-4e7e-8958-c67d48a3e0d2",
		"example://00668ca7-aca9-4e7e-8958-c67d48a3e0d2",
	)

	enDoc.Language = "en-GB"

	info1, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     enDoc.Uuid,
		Document: enDoc,
	})
	test.Must(t, err, "update a document with a valid language and region")
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: enDoc.Uuid,
		Status: []*repository.StatusUpdate{
			{
				Name:    "usable",
				Version: info1.Version,
			},
		},
	})
	test.Must(t, err, "set version one as usable")

	enDoc.Language = "en-US"

	info2, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     enDoc.Uuid,
		Document: enDoc,
	})
	test.Must(t, err, "update a document with a new language")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: enDoc.Uuid,
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
			After: 0,
		})
	test.Must(t, err, "get eventlog")
	test.Equal(t, int(expectedEvents), len(log.Items),
		"get the expected number of events")

	for i := range log.Items {
		log.Items[i].Timestamp = ""
	}

	test.TestMessageAgainstGolden(t, regenerate, log,
		"testdata/TestIntegrationDocumentLanguage/eventlog.json",
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"))
}

func ignoreUUIDField(name string) test.GoldenHelper {
	return test.IgnoreField[string]{
		Name:       name,
		DummyValue: uuid.UUID{}.String(),
		Validator: func(v string) error {
			_, err := uuid.Parse(v)

			return err //nolint: wrapcheck
		},
	}
}

func TestDocumentsServiceMetaDocuments(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	regenerate := regenerateTestFixtures()

	testData := filepath.Join("..", "testdata", t.Name())

	err := os.MkdirAll(testData, 0o700)
	test.Must(t, err, "ensure testdata dir")

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
		ConfigDirectory:    testData,
	})

	ctx := t.Context()

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
						Attributes: revisor.MakeConstraintMap(
							map[string]revisor.StringConstraint{
								"title": {},
							}),
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

	preUse, err := schema.GetMetaTypes(ctx,
		&repository.GetMetaTypesRequest{})
	test.Must(t, err, "get current meta types")

	test.EqualMessage(t, &repository.GetMetaTypesResponse{
		Types: []*repository.MetaTypeInfo{
			{
				Name: "test/metadata",
			},
		},
	}, preUse, "get the expected meta type")

	_, err = schema.RegisterMetaTypeUse(ctx, &repository.RegisterMetaTypeUseRequest{
		MainType: "core/article",
		MetaType: "test/metadata",
	})
	test.Must(t, err, "register the meta type for use with articles")

	postUse, err := schema.GetMetaTypes(ctx,
		&repository.GetMetaTypesRequest{})
	test.Must(t, err, "get current meta types")

	test.EqualMessage(t, &repository.GetMetaTypesResponse{
		Types: []*repository.MetaTypeInfo{
			{
				Name:   "test/metadata",
				UsedBy: []string{"core/article"},
			},
		},
	}, postUse, "get the expected meta type")

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
		CreatorUri:     "user://test/testdocumentsservicemetadocuments",
		UpdaterUri:     "user://test/testdocumentsservicemetadocuments",
	}

	ignoreTimeVariantValues := cmpopts.IgnoreMapEntries(
		func(k string, _ any) bool {
			switch k {
			case "created", "modified", "nonce":
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

	docWithMetaDocV1, err := client.Get(ctx, &repository.GetDocumentRequest{
		Uuid:                docA.Uuid,
		Version:             1,
		MetaDocument:        repository.GetMetaDoc_META_INCLUDE,
		MetaDocumentVersion: 1,
	})
	test.Must(t, err,
		"be able to fetch requested version of document with requested version of meta doc")

	test.EqualMessage(t, &repository.GetDocumentResponse{
		Document: docA,
		Version:  1,
		Meta: &repository.MetaDocument{
			Version:  1,
			Document: mDocV1.Document,
		},
	}, docWithMetaDocV1, "get the expected document back")

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

	events := collectEventlog(t, client, 10, 4*time.Second)

	test.TestMessageAgainstGolden(t, regenerate, events,
		filepath.Join(testData, "eventlog.json"),
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"),
	)
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

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := t.Context()

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
		func(k string, _ any) bool {
			switch k {
			case "created", "modified", "nonce":
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
		CreatorUri:     "user://test/testintegrationbulkcrud",
		UpdaterUri:     "user://test/testintegrationbulkcrud",
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
		CreatorUri: "user://test/testintegrationbulkcrud",
		UpdaterUri: "user://test/testintegrationbulkcrud",
	}

	cmpDiffB := cmp.Diff(&wantMetaB, metaB.Meta,
		protocmp.Transform(),
		ignoreTimeVariantValues,
	)
	if cmpDiffB != "" {
		t.Fatalf("document A meta mismatch (-want +got):\n%s", cmpDiffB)
	}
}

func TestIntegrationStatus(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	regenerate := regenerateTestFixtures()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := t.Context()

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
		pageSize = 10
	)

	numStatuses := 12

	for n := range numStatuses {
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

	// End with an unpublish
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{
				Name:    "usable",
				Version: -1,
				Meta: map[string]string{
					"update_number": strconv.Itoa(numStatuses),
				},
			},
		},
	})
	test.Must(t, err, "unpublish document")

	numStatuses++

	fromHeadRes, err := client.GetStatusHistory(ctx, &repository.GetStatusHistoryRequest{
		Uuid: docUUID,
		Name: "usable",
	})
	test.Must(t, err, "get usable status history from head")

	test.Equal(t, pageSize, len(fromHeadRes.Statuses),
		"get the expected number of statuses")

	minStatus := int64(numStatuses)

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

	nilStatuses, err := client.GetNilStatuses(ctx, &repository.GetNilStatusesRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get nil statuses")

	test.TestMessageAgainstGolden(
		t, regenerate, nilStatuses,
		filepath.Join("testdata", t.Name(), "nil-statuses.json"),
		test.IgnoreTimestamps{})

	// Load and compare document history.
	docHistory, err := client.GetHistory(ctx, &repository.GetHistoryRequest{
		Uuid:         docUUID,
		LoadStatuses: true,
	})
	test.Must(t, err, "load document history")

	docHistoryGolden := filepath.Join("testdata", t.Name(), "history.json")

	test.TestMessageAgainstGolden(t, regenerate, docHistory, docHistoryGolden,
		test.IgnoreTimestamps{})

	// Check that we got the expected events.
	events := collectEventlog(t, client, 31, 5*time.Second)

	eventsGolden := filepath.Join("testdata", t.Name(), "eventlog.json")

	test.TestMessageAgainstGolden(t, regenerate, events, eventsGolden,
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"))

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

	compactGolden := filepath.Join("testdata", t.Name(), "compact_eventlog.json")

	test.TestMessageAgainstGolden(t, regenerate, compactEvents, compactGolden,
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"))
}

func TestIntegrationDeleteTimeout(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete"))

	ctx := t.Context()

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

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	tc := testingAPIServer(t, logger, testingServerOptions{})

	untrustedWorkflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "doc_read"))

	_, err := untrustedWorkflowClient.UpdateStatus(ctx,
		&repository.UpdateStatusRequest{
			Type: "core/article",
			Name: "usable",
		})
	test.MustNot(t, err,
		"be able to create statues without 'workflow_admin' scope")

	workflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Type: "core/article",
		Name: "usable",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Type: "core/article",
		Name: "done",
	})
	test.Must(t, err, "create done status")

	t0 := time.Now()
	workflowDeadline := time.After(1 * time.Second)

	// Wait until the workflow provider notices the change.
	for {
		if tc.WorkflowProvider.HasStatus("core/article", "done") {
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

	ctx := t.Context()
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
			Type:        "core/article",
			Name:        approvalName,
			Description: "Articles must be approved before they're published",
			Expression:  `Heads.approved.Version == Status.Version`,
			AppliesTo:   []string{"usable"},
		},
	})
	test.Must(t, err, "create approval rule")

	_, err = workflowClient.CreateStatusRule(ctx, &repository.CreateStatusRuleRequest{
		Rule: &repository.StatusRule{
			Type:        "core/article",
			Name:        requireLegalName,
			Description: "Require legal signoff if requested",
			Expression: `
Heads.approved.Meta.legal_approval != "required"
or Heads.approved_legal.Version == Status.Version`,
			AppliesTo: []string{"usable"},
		},
	})
	test.Must(t, err, "create legal approval rule")

	_, err = workflowClient.CreateStatusRule(ctx, &repository.CreateStatusRuleRequest{
		Rule: &repository.StatusRule{
			Type:        "core/article",
			Name:        "require-publish-scope",
			Description: "publish scope is required for setting usable",
			Expression:  `User.HasScope("publish")`,
			AccessRule:  true,
			AppliesTo:   []string{"usable"},
		},
	})
	test.Must(t, err, "create publish permission rule")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Type: "core/article",
		Name: "approved_legal",
	})
	test.Must(t, err, "create approved_legal status")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Type: "core/article",
		Name: "usable",
	})
	test.Must(t, err, "create usable status")

	_, err = workflowClient.UpdateStatus(ctx, &repository.UpdateStatusRequest{
		Type: "core/article",
		Name: "approved",
	})
	test.Must(t, err, "create approved status")

	t0 := time.Now()
	workflowDeadline := time.After(1 * time.Second)

	// Wait until the workflow provider notices the change.
	for {
		if tc.WorkflowProvider.HasStatus("core/article", "approved") {
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
		Type: "core/text",
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

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	ctx := t.Context()
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

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	ctx := t.Context()
	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
		RunArchiver:        true,
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

func TestIntegrationStatsOverview(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	regenerate := regenerateTestFixtures()
	dataDir := filepath.Join(
		"..", "testdata", "TestIntegrationStatsOverview")

	if regenerate {
		test.Must(t, os.MkdirAll(dataDir, 0o700),
			"create test data directory")
	}

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write"))

	otherClaims := itest.StandardClaims(t,
		"doc_read",
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

	// The repository only returns modification times with second precision,
	// truncate out acceptable creation time range accordingly.
	creationStart := time.Now().Truncate(1 * time.Second)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Meta: map[string]string{
				"user": "defined",
			}},
			{Name: "done"},
		},
	})
	test.Must(t, err, "create first article")

	doc2 := baseDocument(doc2UUID, doc2URI)

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
		Status: []*repository.StatusUpdate{
			{Name: "usable"},
		},
		Acl: []*repository.ACLEntry{
			{
				Uri:         "user://test/other-user",
				Permissions: []string{"r"},
			},
		},
	})
	test.Must(t, err, "create second article")

	creationEnd := time.Now().Add(1 * time.Second).Truncate(1 * time.Second)

	type tCase struct {
		Name   string
		File   string
		Req    *repository.GetStatusOverviewRequest
		Client repository.Documents
	}

	cases := []tCase{
		{
			Name:   "creator",
			File:   "creator_overview.json",
			Client: client,
			Req: &repository.GetStatusOverviewRequest{
				Uuids:    []string{docUUID, doc2UUID},
				Statuses: []string{"usable", "done"},
				GetMeta:  true,
			},
		},
		{
			Name:   "other",
			File:   "other_overview.json",
			Client: otherClient,
			Req: &repository.GetStatusOverviewRequest{
				Uuids:    []string{docUUID, doc2UUID},
				Statuses: []string{"usable"},
			},
		},
		{
			Name:   "admin",
			File:   "admin_overview.json",
			Client: adminClient,
			Req: &repository.GetStatusOverviewRequest{
				Uuids:    []string{docUUID, doc2UUID},
				Statuses: []string{"usable"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx := t.Context()

			res, err := c.Client.GetStatusOverview(ctx, c.Req)
			test.Must(t, err, "fetch status overview")

			test.TestMessageAgainstGolden(t, regenerate, res,
				filepath.Join(dataDir, c.File),
				test.IgnoreTimestamps{})

			for _, item := range res.Items {
				mod, err := time.Parse(time.RFC3339, item.Modified)
				test.Must(t, err,
					"return a valid modified timestamp for %s",
					item.Uuid)

				t.Log(mod)

				// Checking the timestamp ranges not specific to
				// StatusOverview, but rather verifies the
				// behaviour of updates. So that's where any
				// error fixing efforts should be directed if
				// the range checks fail.
				if mod.Before(creationStart) || mod.After(creationEnd) {
					t.Fatalf("modified timestamp of %s must be between creation start and end", item.Uuid)
				}

				for status, s := range item.Heads {
					created, err := time.Parse(time.RFC3339, s.Created)
					test.Must(t, err,
						"return a valid created timestamp for %q of %s",
						status, item.Uuid)

					if created.Before(creationStart) || created.After(creationEnd) {
						t.Fatalf("creation timestamp of %s must be between creation start and end", item.Uuid)
					}
				}
			}
		})
	}
}

func TestUUIDNormalisation(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	ctx := t.Context()

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	// Uppercase request UUID.
	_, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     strings.ToUpper(docUUID),
		Document: doc,
	})
	test.Must(t, err, "create article, uppercase req.uuid")

	doc.Uuid = strings.ToUpper(doc.Uuid)

	// Uppercase doc UUID.
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "update article, uppercase doc.uuid")

	// Both uppercase.
	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     strings.ToUpper(docUUID),
		Document: doc,
	})
	test.Must(t, err, "update article, both uppercase")
}
