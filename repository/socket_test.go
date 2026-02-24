package repository_test

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	rpc "github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repositorysocket"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestIntegrationSocket(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	testdata := filepath.Join("..", "testdata")
	dataDir := filepath.Join(testdata, t.Name())
	docsDir := filepath.Join(dataDir, "documents")

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
		ConfigDirectory:    filepath.Join(dataDir, "config"),
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	writeDoc(t, client, docsDir, "loss_plan", []*rpc.StatusUpdate{
		{Name: "usable"},
	})

	lossUUID := writeDoc(t, client, docsDir, "loss", []*rpc.StatusUpdate{
		{Name: "done"},
	})

	serverURL, err := url.Parse(tc.Server.URL)
	test.Must(t, err, "parse server URL")

	token, err := itest.AccessToken(tc.SigningKey,
		itest.StandardClaims(t, "doc_read"))
	test.Must(t, err, "create access token")

	t.Run("BadOrigin", func(t *testing.T) {
		tokenResp, err := client.GetSocketToken(ctx, &rpc.GetSocketTokenRequest{})
		test.Must(t, err, "get socket token")

		wsURL := serverURL.JoinPath("websocket", tokenResp.Token)
		wsURL.Scheme = "ws"

		header := http.Header{
			"Origin": []string{"https://example.com"},
		}

		_, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), header)
		test.MustNot(t, err, "dial websocket")

		_ = resp.Body.Close()
	})

	tokenResp, err := client.GetSocketToken(ctx, &rpc.GetSocketTokenRequest{})
	test.Must(t, err, "get socket token")

	wsURL := serverURL.JoinPath("websocket", tokenResp.Token)
	wsURL.Scheme = "ws"

	header := http.Header{
		"Origin": []string{"https://example.ecms.se"},
	}

	conn, wsResp, err := websocket.DefaultDialer.Dial(wsURL.String(), header)
	test.Must(t, err, "dial websocket")

	t.Cleanup(func() {
		err := wsResp.Body.Close()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close websocket response body: %v", err)
		}
	})

	wsURL.Scheme = "http"

	// Immediately make a request with the same token to trigger rate limit.
	res, err := http.Get(wsURL.String())
	test.Must(t, err, "make throttling check request")

	_ = res.Body.Close()

	test.Equal(t, http.StatusTooManyRequests, res.StatusCode,
		"get a too many requests response")

	callID := makeCall(t, conn, &repositorysocket.Call{
		Authenticate: &repositorysocket.Authenticate{
			Token: token,
		},
	})

	authResp, ok := readResponse(t, conn)
	if !ok {
		t.Fatal("socket was unexpectedly closed")
	}

	if authResp.CallId != callID {
		t.Fatal("authentication response call ID mismatch")
	}

	const (
		subCall   = "d063e44a-866c-47c5-9ba5-f10bffd753ed"
		unsubCall = "b8b959d0-69fa-4403-b691-813318478f60"
	)

	resp := newResponseCollection()

	go resp.ReadResponses(t, conn)

	makeCall(t, conn, &repositorysocket.Call{
		CallId: subCall,
		GetDocuments: &repositorysocket.GetDocuments{
			SetName: "planning",
			Type:    "core/planning-item",
			Timespan: &rpc.Timespan{
				From: "2025-11-14T00:00:00Z",
				To:   "2025-11-14T23:59:59Z",
			},
			Include: []string{
				".meta(type='core/assignment').links(rel='deliverable')@{uuid}",
			},
		},
	})

	var keyResp []*repositorysocket.Response

	batch, err := resp.AwaitDocumentBatch(subCall, 2*time.Second)
	test.Must(t, err, "get initial document batch")

	initInclBatch, err := resp.AwaitInclusionBatch(subCall, 2*time.Second)
	test.Must(t, err, "get initial inclusion batch")

	subSuccess, err := resp.AwaitResponse(subCall, nil, 2*time.Second)
	test.Must(t, err, "subscribe to document set")

	keyResp = append(keyResp, batch, initInclBatch, subSuccess)

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: lossUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: 1},
		},
	})
	test.Must(t, err, "set usable status for loss article")

	status, err := resp.AwaitDocumentStatus(subCall, lossUUID, "usable", 1, 2*time.Second)
	test.Must(t, err, "get loss article status message")

	workflow, err := resp.AwaitWorkflowState(subCall, lossUUID, "usable", 2*time.Second)
	test.Must(t, err, "get loss article workflow message")

	keyResp = append(keyResp, status, workflow)

	beachPlanUUID := writeDoc(t, client, docsDir, "beach_plan_v1", nil)

	beachPlanV1, err := resp.AwaitDocumentUpdate(subCall, beachPlanUUID, 2*time.Second)
	test.Must(t, err, "get beach plan v1 document message")

	test.Equal(t, 1, beachPlanV1.DocumentUpdate.Event.Version, "get v1 of beach plan")

	beachUUID := writeDoc(t, client, docsDir, "beach", nil)

	writeDoc(t, client, docsDir, "beach_plan_v2", nil)

	beachPlanV2, err := resp.AwaitDocumentUpdate(subCall, beachPlanUUID, 2*time.Second)
	test.Must(t, err, "get beach plan v2 document message")

	test.Equal(t, 2, beachPlanV2.DocumentUpdate.Event.Version, "get v2 of beach plan")

	beachInclusion, err := resp.AwaitInclusionBatch(subCall, 2*time.Second)
	test.Must(t, err, "get beach plan inclusion batch message")

	keyResp = append(keyResp, beachPlanV1, beachPlanV2, beachInclusion)

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: beachUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: 1},
		},
	})
	test.Must(t, err, "set usable status for beach article")

	beachStatus, err := resp.AwaitDocumentStatus(subCall, beachUUID, "usable", 1, 2*time.Second)
	test.Must(t, err, "get beach article status message")

	beachWorkflow, err := resp.AwaitWorkflowState(subCall, beachUUID, "usable", 2*time.Second)
	test.Must(t, err, "get beach article workflow message")

	keyResp = append(keyResp, beachStatus, beachWorkflow)

	// Move the beach planning out of time range
	writeDoc(t, client, docsDir, "beach_plan_v3", nil)

	var (
		planRem bool
		artRem  bool
	)

	// Wait for the beach plan and article to be removed.
	_, err = resp.AwaitResponse(subCall, func(resp *repositorysocket.Response) bool {
		if resp.Removed == nil {
			return false
		}

		switch resp.Removed.DocumentUuid {
		case beachPlanUUID:
			planRem = true
		case beachUUID:
			artRem = true
		default:
			return false
		}

		keyResp = append(keyResp, resp)

		return planRem && artRem
	}, 2*time.Second)
	test.Must(t, err, "get remove messages")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: beachUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: 1},
		},
	})
	test.Must(t, err, "republish beach article")

	// Waiting a bit here to make sure that we don't get more events.
	_, err = resp.AwaitDocumentStatus(subCall, beachUUID, "usable", 2, 2*time.Second)
	test.MustNot(t, err, "get more events for beach article")

	makeCall(t, conn, &repositorysocket.Call{
		CallId: unsubCall,
		CloseDocumentSet: &repositorysocket.CloseDocumentSet{
			SetName: "planning",
		},
	})

	_, err = resp.AwaitResponse(unsubCall, nil, 2*time.Second)
	test.Must(t, err, "close document set")

	err = conn.WriteMessage(websocket.CloseMessage, nil)
	test.Must(t, err, "close socket gracefully")

	goldenOpts := []test.GoldenHelper{
		test.IgnoreTimestamps{},
		ignoreUUIDField("nonce"),
		ignoreUUIDField("document_nonce"),
	}

	test.TestMessagesAgainstGolden(t, regenerateTestFixtures(),
		keyResp, filepath.Join(dataDir, "messages.json"),
		goldenOpts...,
	)

	if os.Getenv("DUMP_MESSAGES") == "true" {
		// The messages are not completely stable, but they are useful for
		// reference when testing.
		err = elephantine.MarshalFile(
			filepath.Join(dataDir, "all_messages.json"),
			resp.Responses())
		test.Must(t, err, "write messages reference file")
	}
}

func TestIntegrationSocketPartial(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	// Reuse the TestIntegrationSocket testdata for config and documents.
	testdata := filepath.Join("..", "testdata")
	socketDir := filepath.Join(testdata, "TestIntegrationSocket")
	docsDir := filepath.Join(socketDir, "documents")

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
		ConfigDirectory:    filepath.Join(socketDir, "config"),
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))

	// Create beach_plan_v1 before subscribing so it appears in the
	// initial batch.
	beachPlanUUID := writeDoc(t, client, docsDir, "beach_plan_v1", nil)

	// Create the beach article so it can be included later when
	// beach_plan_v2 adds a deliverable link to it.
	beachUUID := writeDoc(t, client, docsDir, "beach", nil)

	serverURL, err := url.Parse(tc.Server.URL)
	test.Must(t, err, "parse server URL")

	token, err := itest.AccessToken(tc.SigningKey,
		itest.StandardClaims(t, "doc_read"))
	test.Must(t, err, "create access token")

	tokenResp, err := client.GetSocketToken(ctx, &rpc.GetSocketTokenRequest{})
	test.Must(t, err, "get socket token")

	wsURL := serverURL.JoinPath("websocket", tokenResp.Token)
	wsURL.Scheme = "ws"

	header := http.Header{
		"Origin": []string{"https://example.ecms.se"},
	}

	conn, wsResp, err := websocket.DefaultDialer.Dial(wsURL.String(), header)
	test.Must(t, err, "dial websocket")

	t.Cleanup(func() {
		err := wsResp.Body.Close()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close websocket response body: %v", err)
		}
	})

	callID := makeCall(t, conn, &repositorysocket.Call{
		Authenticate: &repositorysocket.Authenticate{
			Token: token,
		},
	})

	authResp, ok := readResponse(t, conn)
	if !ok {
		t.Fatal("socket was unexpectedly closed")
	}

	if authResp.CallId != callID {
		t.Fatal("authentication response call ID mismatch")
	}

	// --- Verify that malformed subset expressions are rejected ---

	badSubsetCall := makeCall(t, conn, &repositorysocket.Call{
		GetDocuments: &repositorysocket.GetDocuments{
			SetName: "bad-subset",
			Type:    "core/planning-item",
			Subset:  []string{".meta(type='broken"},
		},
	})

	badSubsetResp, ok := readResponse(t, conn)
	if !ok {
		t.Fatal("socket was unexpectedly closed after malformed subset")
	}

	if badSubsetResp.CallId != badSubsetCall {
		t.Fatal("malformed subset response call ID mismatch")
	}

	if badSubsetResp.Error == nil {
		t.Fatal("expected error response for malformed subset expression")
	}

	test.Equal(t, "invalid_argument", badSubsetResp.Error.ErrorCode,
		"malformed subset error code")

	if !strings.Contains(badSubsetResp.Error.ErrorMessage, "subset") {
		t.Fatalf("expected error message to mention subset, got: %s",
			badSubsetResp.Error.ErrorMessage)
	}

	const subCall = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

	resp := newResponseCollection()

	go resp.ReadResponses(t, conn)

	// Subscribe with subset for planning items and inclusion subset
	// for articles. Uses the doc annotation on the include expression
	// so that the full document is loaded for included articles.
	makeCall(t, conn, &repositorysocket.Call{
		CallId: subCall,
		GetDocuments: &repositorysocket.GetDocuments{
			SetName: "planning",
			Type:    "core/planning-item",
			Timespan: &rpc.Timespan{
				From: "2025-11-14T00:00:00Z",
				To:   "2025-11-14T23:59:59Z",
			},
			Include: []string{
				".meta(type='core/assignment').links(rel='deliverable')@{uuid:doc}",
			},
			Subset: []string{
				".meta(type='core/newsvalue')@{value}",
			},
			InclusionSubsets: map[string]string{
				"core/article": ".content(type='core/text' role='heading-1').data{text}",
			},
		},
	})

	// --- Verify initial batch uses subset ---

	batch, err := resp.AwaitDocumentBatch(subCall, 2*time.Second)
	test.Must(t, err, "get initial document batch")

	db := batch.DocumentBatch

	// Find the beach plan document in the batch.
	var beachPlanState *repositorysocket.DocumentState

	for _, doc := range db.Documents {
		if doc.Meta != nil && doc.Meta.Type == "core/planning-item" {
			beachPlanState = doc

			break
		}
	}

	if beachPlanState == nil {
		t.Fatal("beach plan not found in initial batch")
	}

	if beachPlanState.Document != nil {
		t.Fatal("expected Document to be nil when subset is active")
	}

	if len(beachPlanState.Subset) != 1 {
		t.Fatalf("expected 1 subset entry in initial batch, got %d",
			len(beachPlanState.Subset))
	}

	ev, ok := beachPlanState.Subset[0].Values["value"]
	if !ok {
		t.Fatal("expected 'value' key in initial batch subset")
	}

	test.Equal(t, "3", ev.Value, "extracted newsvalue from initial batch")

	_, err = resp.AwaitResponse(subCall, nil, 2*time.Second)
	test.Must(t, err, "subscribe to document set")

	// --- Write beach_plan_v2 to trigger update + inclusion ---

	// beach_plan_v2 adds a deliverable link to the beach article,
	// which triggers an inclusion change.
	writeDoc(t, client, docsDir, "beach_plan_v2", nil)

	// Verify DocumentUpdate has subset applied for the plan update.
	planUpdate, err := resp.AwaitDocumentUpdate(
		subCall, beachPlanUUID, 2*time.Second)
	test.Must(t, err, "get beach plan v2 update")

	du := planUpdate.DocumentUpdate

	test.Equal(t, int64(2), du.Event.Version, "beach plan v2 version")

	if du.Document != nil {
		t.Fatal("expected Document to be nil in update when subset is active")
	}

	if len(du.Subset) != 1 {
		t.Fatalf("expected 1 subset entry in update, got %d",
			len(du.Subset))
	}

	updateEV, ok := du.Subset[0].Values["value"]
	if !ok {
		t.Fatal("expected 'value' key in update subset")
	}

	test.Equal(t, "3", updateEV.Value,
		"extracted newsvalue from update")

	// --- Verify inclusion batch uses inclusion subset ---

	inclResp, err := resp.AwaitInclusionBatch(subCall, 2*time.Second)
	test.Must(t, err, "get inclusion batch after plan v2")

	ib := inclResp.InclusionBatch
	if len(ib.Documents) == 0 {
		t.Fatal("expected at least one included document")
	}

	// Find the beach article in the inclusion batch.
	var beachIncl *repositorysocket.InclusionDocument

	for _, doc := range ib.Documents {
		if doc.Uuid == beachUUID {
			beachIncl = doc

			break
		}
	}

	if beachIncl == nil {
		t.Fatal("beach article not found in inclusion batch")
	}

	if beachIncl.State == nil {
		t.Fatal("expected state for included document")
	}

	if beachIncl.State.Document != nil {
		t.Fatal("expected Document to be nil for included doc with inclusion subset")
	}

	if len(beachIncl.State.Subset) != 1 {
		t.Fatalf("expected 1 subset entry for included doc, got %d",
			len(beachIncl.State.Subset))
	}

	textEV, ok := beachIncl.State.Subset[0].Values["text"]
	if !ok {
		t.Fatal("expected 'text' key in inclusion subset")
	}

	test.Equal(t, "Svenska talangerna vann i VM-debuten",
		textEV.Value, "extracted heading from included article")

	err = conn.WriteMessage(websocket.CloseMessage, nil)
	test.Must(t, err, "close socket gracefully")
}

func TestIntegrationSocketEventlog(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	// Reuse the TestIntegrationSocket testdata for config and documents.
	testdata := filepath.Join("..", "testdata")
	dataDir := filepath.Join(testdata, t.Name())
	socketDir := filepath.Join(testdata, "TestIntegrationSocket")
	docsDir := filepath.Join(socketDir, "documents")

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver:        true,
		RunEventlogBuilder: true,
		ConfigDirectory:    filepath.Join(socketDir, "config"),
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write eventlog_read"))

	// Write beach_plan_v1 before subscribing so that we test
	// playback from the docstream buffer.
	beachPlanUUID := writeDoc(t, client, docsDir, "beach_plan_v1", nil)

	serverURL, err := url.Parse(tc.Server.URL)
	test.Must(t, err, "parse server URL")

	token, err := itest.AccessToken(tc.SigningKey,
		itest.StandardClaims(t, "eventlog_read doc_read"))
	test.Must(t, err, "create access token")

	tokenResp, err := client.GetSocketToken(ctx, &rpc.GetSocketTokenRequest{})
	test.Must(t, err, "get socket token")

	wsURL := serverURL.JoinPath("websocket", tokenResp.Token)
	wsURL.Scheme = "ws"

	header := http.Header{
		"Origin": []string{"https://example.ecms.se"},
	}

	conn, wsResp, err := websocket.DefaultDialer.Dial(wsURL.String(), header)
	test.Must(t, err, "dial websocket")

	t.Cleanup(func() {
		err := wsResp.Body.Close()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close websocket response body: %v", err)
		}
	})

	callID := makeCall(t, conn, &repositorysocket.Call{
		Authenticate: &repositorysocket.Authenticate{
			Token: token,
		},
	})

	authResp, ok := readResponse(t, conn)
	if !ok {
		t.Fatal("socket was unexpectedly closed")
	}

	if authResp.CallId != callID {
		t.Fatal("authentication response call ID mismatch")
	}

	const (
		evtCall   = "e1e2e3e4-f5f6-7890-abcd-ef1234567890"
		closeCall = "c1c2c3c4-d5d6-7890-abcd-ef1234567890"
	)

	resp := newResponseCollection()

	go resp.ReadResponses(t, conn)

	// Subscribe to the eventlog with after=0 to get buffer playback
	// (beach_plan_v1 should be in the buffer). Use type subsets to
	// test partial document extraction.
	afterZero := int64(0)

	makeCall(t, conn, &repositorysocket.Call{
		CallId: evtCall,
		GetEventlog: &repositorysocket.GetEventlog{
			Name:  "test-eventlog",
			After: &afterZero,
			DocumentTypes: []string{
				"core/planning-item",
				"core/article",
			},
			TypeSubsets: map[string]string{
				"core/planning-item": ".meta(type='core/newsvalue')@{value}",
				"core/article":       ".content(type='core/text' role='heading-1').data{text}",
			},
		},
	})

	// Await the subscription confirmation.
	_, err = resp.AwaitResponse(evtCall, nil, 2*time.Second)
	test.Must(t, err, "subscribe to eventlog")

	// Write the remaining beach documents.
	writeDoc(t, client, docsDir, "beach", nil)
	writeDoc(t, client, docsDir, "beach_plan_v2", nil)
	writeDoc(t, client, docsDir, "beach_plan_v3", nil)

	// Wait for the beach_plan_v3 event (the last expected event).
	_, err = resp.AwaitResponse(evtCall, func(r *repositorysocket.Response) bool {
		if r.Events == nil {
			return false
		}

		for _, item := range r.Events.Items {
			if item.Event != nil &&
				item.Event.Uuid == beachPlanUUID &&
				item.Event.Version == 3 {
				return true
			}
		}

		return false
	}, 5*time.Second)
	test.Must(t, err, "get beach plan v3 event")

	// Close the eventlog subscription.
	makeCall(t, conn, &repositorysocket.Call{
		CallId: closeCall,
		CloseEventlog: &repositorysocket.CloseEventlog{
			Name: "test-eventlog",
		},
	})

	_, err = resp.AwaitResponse(closeCall, nil, 2*time.Second)
	test.Must(t, err, "close eventlog subscription")

	err = conn.WriteMessage(websocket.CloseMessage, nil)
	test.Must(t, err, "close socket gracefully")

	// Collect all eventlog items from all Events responses into a
	// single normalized response for stable golden comparison
	// regardless of event batching.
	var allItems []*repositorysocket.EventlogItem

	for _, r := range resp.Responses() {
		if r.CallId == evtCall && r.Events != nil {
			allItems = append(allItems, r.Events.Items...)
		}
	}

	keyResp := []*repositorysocket.Response{
		{
			CallId: evtCall,
			Events: &repositorysocket.EventlogResponse{
				Items: allItems,
			},
		},
	}

	goldenOpts := []test.GoldenHelper{
		test.IgnoreTimestamps{},
		ignoreUUIDField("nonce"),
		ignoreUUIDField("document_nonce"),
	}

	test.TestMessagesAgainstGolden(t, regenerateTestFixtures(),
		keyResp, filepath.Join(dataDir, "messages.json"),
		goldenOpts...,
	)
}

type respNotification struct {
	Index    int
	Response *repositorysocket.Response
}

type responseCollection struct {
	m         sync.Mutex
	responses []*repositorysocket.Response

	lastAwait int

	notifyResp chan respNotification
}

func newResponseCollection() *responseCollection {
	return &responseCollection{
		notifyResp: make(chan respNotification, 64),
	}
}

func (rc *responseCollection) ReadResponses(t *testing.T, conn *websocket.Conn) {
	t.Helper()

	for {
		resp, ok := readResponse(t, conn)
		if !ok {
			return
		}

		rc.m.Lock()
		idx := len(rc.responses)
		rc.responses = append(rc.responses, resp)
		rc.m.Unlock()

		select {
		case rc.notifyResp <- respNotification{
			Index:    idx,
			Response: resp,
		}:
		case <-t.Context().Done():
			return
		}
	}
}

func (rc *responseCollection) Responses() []*repositorysocket.Response {
	rc.m.Lock()
	res := slices.Clone(rc.responses)
	rc.m.Unlock()

	return res
}

func (rc *responseCollection) AwaitDocumentBatch(
	callID string,
	timeout time.Duration,
) (*repositorysocket.Response, error) {
	resp, err := rc.AwaitResponse(callID, func(resp *repositorysocket.Response) bool {
		return resp.DocumentBatch != nil
	}, timeout)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (rc *responseCollection) AwaitInclusionBatch(
	callID string,
	timeout time.Duration,
) (*repositorysocket.Response, error) {
	resp, err := rc.AwaitResponse(callID, func(resp *repositorysocket.Response) bool {
		return resp.InclusionBatch != nil
	}, timeout)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (rc *responseCollection) AwaitDocumentStatus(
	callID string,
	docUUID string, status string, id int64,
	timeout time.Duration,
) (*repositorysocket.Response, error) {
	resp, err := rc.AwaitResponse(callID, func(resp *repositorysocket.Response) bool {
		if resp.DocumentUpdate == nil || resp.DocumentUpdate.Event == nil {
			return false
		}

		evt := resp.DocumentUpdate.Event

		return evt.Event == "status" && evt.Uuid == docUUID &&
			evt.Status == status && evt.StatusId == id
	}, timeout)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (rc *responseCollection) AwaitWorkflowState(
	callID string,
	docUUID string, state string,
	timeout time.Duration,
) (*repositorysocket.Response, error) {
	resp, err := rc.AwaitResponse(callID, func(resp *repositorysocket.Response) bool {
		if resp.DocumentUpdate == nil || resp.DocumentUpdate.Event == nil {
			return false
		}

		evt := resp.DocumentUpdate.Event

		return evt.Event == "workflow" && evt.Uuid == docUUID && evt.WorkflowState == state
	}, timeout)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (rc *responseCollection) AwaitDocumentUpdate(
	callID string,
	docUUID string,
	timeout time.Duration,
) (*repositorysocket.Response, error) {
	resp, err := rc.AwaitResponse(callID, func(resp *repositorysocket.Response) bool {
		if resp.DocumentUpdate == nil || resp.DocumentUpdate.Event == nil {
			return false
		}

		evt := resp.DocumentUpdate.Event

		return evt.Event == "document" && evt.Uuid == docUUID
	}, timeout)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (rc *responseCollection) AwaitResponse(
	callID string,
	check func(resp *repositorysocket.Response) bool,
	timeout time.Duration,
) (*repositorysocket.Response, error) {
	deadline := time.After(timeout)

	responses := rc.Responses()

	for _, resp := range responses[rc.lastAwait:] {
		if resp.CallId != callID {
			continue
		}

		if resp.Error != nil {
			return nil, fmt.Errorf("call failed: %s: %s",
				resp.Error.ErrorCode, resp.Error.ErrorMessage)
		}

		if (check == nil && resp.Handled) || (check != nil && check(resp)) {
			return resp, nil
		}
	}

	rc.lastAwait = len(responses)

	for {
		select {
		case <-deadline:
			return nil, errors.New("deadline exceeded")
		case n := <-rc.notifyResp:
			rc.lastAwait = n.Index + 1
			resp := n.Response

			if resp.CallId != callID {
				continue
			}

			if resp.Error != nil {
				return nil, fmt.Errorf("call failed: %s: %s",
					resp.Error.ErrorCode, resp.Error.ErrorMessage)
			}

			if (check == nil && resp.Handled) || (check != nil && check(resp)) {
				return resp, nil
			}
		}
	}
}

func makeCall(
	t *testing.T,
	conn *websocket.Conn,
	call *repositorysocket.Call,
) string {
	t.Helper()

	if call.CallId == "" {
		call.CallId = uuid.NewString()
	}

	data, err := proto.Marshal(call)
	test.Must(t, err, "marshal message")

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	test.Must(t, err, "write message")

	return call.CallId
}

func readResponse(
	t *testing.T, conn *websocket.Conn,
) (*repositorysocket.Response, bool) {
	t.Helper()

	msgType, data, err := conn.ReadMessage()
	if websocket.IsCloseError(err, websocket.CloseNoStatusReceived) {
		return nil, false
	}

	test.Must(t, err, "read message from websocket")

	var resp repositorysocket.Response

	switch msgType {
	case websocket.TextMessage:
		err := protojson.Unmarshal(data, &resp)
		test.Must(t, err, "unmarshal json response")
	case websocket.BinaryMessage:
		err := proto.Unmarshal(data, &resp)
		test.Must(t, err, "unmarshal protobuf response")
	case websocket.CloseMessage:
		t.Fatal("socket was closed")
	default:
		t.Fatalf("unexpected message type %d", msgType)
	}

	return &resp, true
}
