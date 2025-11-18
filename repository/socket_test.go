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

	tokenResp, err := client.GetSocketToken(ctx, &rpc.GetSocketTokenRequest{})
	test.Must(t, err, "get socket token")

	token, err := itest.AccessToken(tc.SigningKey,
		itest.StandardClaims(t, "doc_read"))
	test.Must(t, err, "create access token")

	serverURL, err := url.Parse(tc.Server.URL)
	test.Must(t, err, "parse server URL")

	wsURL := serverURL.JoinPath("websocket", tokenResp.Token)
	wsURL.Scheme = "ws"

	conn, wsResp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
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

	beachUUID := writeDoc(t, client, docsDir, "beach", nil)

	writeDoc(t, client, docsDir, "beach_plan_v2", nil)

	beachPlanV2, err := resp.AwaitDocumentUpdate(subCall, beachPlanUUID, 2*time.Second)
	test.Must(t, err, "get beach plan v2 document message")

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

type responseCollection struct {
	m         sync.Mutex
	responses []*repositorysocket.Response

	lastAwait int

	notifyResp chan *repositorysocket.Response
}

func newResponseCollection() *responseCollection {
	return &responseCollection{
		notifyResp: make(chan *repositorysocket.Response, 64),
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
		rc.responses = append(rc.responses, resp)
		rc.m.Unlock()

		select {
		case rc.notifyResp <- resp:
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
		case resp := <-rc.notifyResp:
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
