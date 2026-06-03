package repository_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-repository/internal"
	itest "github.com/ttab/elephant-repository/internal/test"
	repo "github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

type stubWorkflowLoader struct {
	statuses  []repo.DocumentStatus
	workflows []repo.DocumentWorkflow
}

func (l *stubWorkflowLoader) GetStatuses(
	_ context.Context, docType string,
) ([]repo.DocumentStatus, error) {
	if docType == "" {
		return l.statuses, nil
	}

	var matching []repo.DocumentStatus

	for _, s := range l.statuses {
		if s.Type == docType {
			matching = append(matching, s)
		}
	}

	return matching, nil
}

func (l *stubWorkflowLoader) GetStatusRules(
	_ context.Context,
) ([]repo.StatusRule, error) {
	return nil, nil
}

func (l *stubWorkflowLoader) SetDocumentWorkflow(
	_ context.Context, _ repo.DocumentWorkflow,
) error {
	return errors.New("not implemented")
}

func (l *stubWorkflowLoader) GetDocumentWorkflows(
	_ context.Context,
) ([]repo.DocumentWorkflow, error) {
	return l.workflows, nil
}

func (l *stubWorkflowLoader) GetDocumentWorkflow(
	_ context.Context, docType string,
) (repo.DocumentWorkflow, error) {
	for _, wf := range l.workflows {
		if wf.Type == docType {
			return wf, nil
		}
	}

	return repo.DocumentWorkflow{}, errors.New("not found")
}

func (l *stubWorkflowLoader) DeleteDocumentWorkflow(
	_ context.Context, _ string,
) error {
	return errors.New("not implemented")
}

func (l *stubWorkflowLoader) OnWorkflowUpdate(
	_ context.Context, _ chan repo.WorkflowEvent,
) {
}

func TestImplicitWorkflow(t *testing.T) {
	t.Parallel()

	loader := &stubWorkflowLoader{
		statuses: []repo.DocumentStatus{
			{Type: "core/article", Name: "draft"},
			{Type: "core/article", Name: "done"},
			{Type: "core/article", Name: "usable"},
			{Type: "core/article", Name: "retired", Disabled: true},
			{Type: "core/image", Name: "usable"},
		},
		workflows: []repo.DocumentWorkflow{
			{
				Type: "core/article-with-explicit",
				Configuration: repo.DocumentWorkflowConfiguration{
					StepZero:           "draft",
					Checkpoint:         "usable",
					NegativeCheckpoint: "unpublished",
					Steps:              []string{"draft", "done"},
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	workflows, err := repo.NewWorkflows(ctx,
		slog.New(test.NewLogHandler(t, slog.LevelInfo)), loader)
	test.Must(t, err, "build workflows")

	t.Run("implicit workflow with multiple statuses", func(t *testing.T) {
		wf, ok := workflows.GetDocumentWorkflow("core/article")
		test.Equal(t, true, ok,
			"implicit workflow should exist for type with statuses")
		test.Equal(t, "core/article", wf.Type, "workflow type")
		test.Equal(t, "", wf.Configuration.Checkpoint,
			"implicit workflow has no checkpoint")
		test.Equal(t, "", wf.Configuration.NegativeCheckpoint,
			"implicit workflow has no negative checkpoint")
		test.Equal(t, "", wf.Configuration.StepZero,
			"implicit workflow has no step zero")

		expected := []string{"draft", "done", "usable"}
		got := slices.Clone(wf.Configuration.Steps)
		slices.Sort(got)
		slices.Sort(expected)

		test.EqualDiff(t, expected, got,
			"implicit workflow steps mirror configured statuses (disabled excluded)")
	})

	t.Run("implicit workflow with single status", func(t *testing.T) {
		wf, ok := workflows.GetDocumentWorkflow("core/image")
		test.Equal(t, true, ok, "implicit workflow should exist")
		test.EqualDiff(t, []string{"usable"}, wf.Configuration.Steps,
			"single status becomes the only step")
	})

	t.Run("no implicit workflow when no statuses configured", func(t *testing.T) {
		_, ok := workflows.GetDocumentWorkflow("core/unknown")
		test.Equal(t, false, ok,
			"types without statuses get no workflow")
	})

	t.Run("explicit workflow takes precedence", func(t *testing.T) {
		wf, ok := workflows.GetDocumentWorkflow("core/article-with-explicit")
		test.Equal(t, true, ok, "explicit workflow should exist")
		test.Equal(t, "usable", wf.Configuration.Checkpoint,
			"explicit workflow keeps its checkpoint")
		test.EqualDiff(t, []string{"draft", "done"}, wf.Configuration.Steps,
			"explicit steps are preserved")
	})

	t.Run("implicit workflow steps drive state transitions", func(t *testing.T) {
		wf, ok := workflows.GetDocumentWorkflow("core/article")
		test.Equal(t, true, ok, "implicit workflow exists")

		state := wf.Start()
		test.Equal(t, "", state.Step, "start step is empty for implicit workflow")

		state = wf.Step(state, repo.WorkflowStep{
			Status: &repo.StatusUpdate{Name: "done", Version: 1},
		})
		test.Equal(t, "done", state.Step,
			"status transitions advance implicit step")

		state = wf.Step(state, repo.WorkflowStep{
			Status: &repo.StatusUpdate{Name: "usable", Version: 1},
		})
		test.Equal(t, "usable", state.Step,
			"every configured status counts as a step")
		test.Equal(t, "", state.LastCheckpoint,
			"implicit workflow never records a checkpoint")

		// A new version with no checkpoint configured must not reset
		// the recorded step back to empty.
		state = wf.Step(state, repo.WorkflowStep{Version: 2})
		test.Equal(t, "usable", state.Step,
			"version bumps don't reset step when no checkpoint is configured")
	})

	t.Run("explicit workflow without checkpoint", func(t *testing.T) {
		wf := repo.DocumentWorkflow{
			Type: "core/note",
			Configuration: repo.DocumentWorkflowConfiguration{
				Steps: []string{"draft", "done"},
			},
		}

		state := wf.Start()
		state = wf.Step(state, repo.WorkflowStep{
			Status: &repo.StatusUpdate{Name: "done", Version: 1},
		})
		test.Equal(t, "done", state.Step,
			"explicit workflow without checkpoint advances step")
		test.Equal(t, "", state.LastCheckpoint,
			"checkpoint-less workflow does not record a checkpoint")

		state = wf.Step(state, repo.WorkflowStep{Version: 2})
		test.Equal(t, "done", state.Step,
			"version bumps don't reset step without a checkpoint")
	})
}

func TestIntegrationWorkflows(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	regenerate := regenerateTestFixtures()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))
	wflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	ctx := t.Context()

	_, err := wflowClient.SetWorkflow(ctx, &repository.SetWorkflowRequest{
		Type: "core/article",
		Workflow: &repository.DocumentWorkflow{
			StepZero:           "draft",
			Checkpoint:         "usable",
			NegativeCheckpoint: "unpublished",
			Steps:              []string{"draft", "done", "approved", "withheld"},
		},
	})
	test.Must(t, err, "create workflow")

	waitDeadline := time.Now().Add(5 * time.Second)

	for {
		if time.Now().After(waitDeadline) {
			t.Fatal("timed out waiting for workflow to kick in")
		}

		_, exists := tc.WorkflowProvider.GetDocumentWorkflow("core/article")
		if exists {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

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

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "done", Version: docRes.Version},
		},
	})
	test.Must(t, err, "set done status")

	updateRes, err := client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "update article")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "approved", Version: updateRes.Version},
		},
	})
	test.Must(t, err, "set approved status")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		Status: []*repository.StatusUpdate{
			{Name: "usable"},
		},
	})
	test.Must(t, err, "set usable status")

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "update article after usable")

	events := collectEventlog(t, client, 8, 5*time.Second)
	eventsGolden := filepath.Join("testdata", t.Name(), "events.json")

	test.TestMessageAgainstGolden(t, regenerate, events, eventsGolden,
		test.IgnoreTimestamps{},
		ignoreUUIDField("document_nonce"))

	meta, err := client.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "get document meta")

	metaGolden := filepath.Join("testdata", t.Name(), "meta.json")

	test.TestMessageAgainstGolden(t, regenerate, meta, metaGolden,
		test.IgnoreTimestamps{},
		ignoreUUIDField("nonce"))
}

func TestIntegrationWorkflowEventEmission(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
		EmitWorkflowEvent:  true,
	})

	client := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write doc_delete eventlog_read"))
	wflowClient := tc.WorkflowsClient(t,
		itest.StandardClaims(t, "workflow_admin"))

	ctx := t.Context()

	_, err := wflowClient.SetWorkflow(ctx, &repository.SetWorkflowRequest{
		Type: "core/article",
		Workflow: &repository.DocumentWorkflow{
			StepZero:           "draft",
			Checkpoint:         "usable",
			NegativeCheckpoint: "unpublished",
			Steps:              []string{"draft", "done"},
		},
	})
	test.Must(t, err, "create workflow")

	waitDeadline := time.Now().Add(2 * time.Second)

	for {
		if time.Now().After(waitDeadline) {
			t.Fatal("timed out waiting for workflow to propagate")
		}

		wf, ok := tc.WorkflowProvider.GetDocumentWorkflow("core/article")
		if ok && wf.Configuration.Checkpoint == "usable" {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

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

	_, err = client.Update(ctx, &repository.UpdateRequest{
		Uuid: docUUID,
		Status: []*repository.StatusUpdate{
			{Name: "usable", Version: res.Version},
		},
	})
	test.Must(t, err, "set usable status")

	// doc + ACL + doc-workflow + status + status-workflow = 5 events.
	events := collectEventlog(t, client, 5, 5*time.Second)

	var (
		workflowEvents []*repository.EventlogItem
		statusEvent    *repository.EventlogItem
	)

	for _, e := range events.Items {
		switch e.Event {
		case "workflow":
			workflowEvents = append(workflowEvents, e)
		case "status":
			statusEvent = e
		}
	}

	test.Equal(t, 2, len(workflowEvents),
		"flag re-enables the standalone workflow events")

	if statusEvent == nil {
		t.Fatal("expected a status event")
	}

	test.Equal(t, "usable", statusEvent.WorkflowState,
		"workflow state is still folded onto the status event")
	test.Equal(t, "usable", statusEvent.WorkflowCheckpoint,
		"workflow checkpoint is still folded onto the status event")
}

func collectEventlog(
	t *testing.T, client repository.Documents,
	minCount int, timeout time.Duration,
) *repository.GetEventlogResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	var (
		lastID    int64
		collected []*repository.EventlogItem
	)

	for len(collected) < minCount {
		events, err := client.Eventlog(ctx, &repository.GetEventlogRequest{
			// Overfetch by one so that we have a chance of getting
			// any extraneous events.
			BatchSize:   internal.MustInt32(minCount - len(collected) + 1),
			BatchWaitMs: 200,
			After:       lastID,
		})
		test.Must(t, err, "get eventlog")

		collected = append(collected, events.Items...)

		if len(events.Items) > 0 {
			last := len(events.Items) - 1
			lastID = events.Items[last].Id
		}
	}

	return &repository.GetEventlogResponse{
		Items: collected,
	}
}
