package repository

import (
	"context"
	"log/slog"
	"time"

	"github.com/expr-lang/expr"
	"github.com/ttab/elephant-api/repository"
	"github.com/twitchtv/twirp"
)

type WorkflowsService struct {
	store WorkflowStore
}

func NewWorkflowsService(store WorkflowStore) *WorkflowsService {
	return &WorkflowsService{
		store: store,
	}
}

// Interface guard.
var _ repository.Workflows = &WorkflowsService{}

// CreateStatusRule creates or updates a status rule that should be applied when
// setting statuses.
func (s *WorkflowsService) CreateStatusRule(
	ctx context.Context, req *repository.CreateStatusRuleRequest,
) (*repository.CreateStatusRuleResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	if req.Rule == nil {
		return nil, twirp.RequiredArgumentError("rule")
	}

	if req.Rule.Type == "" {
		return nil, twirp.RequiredArgumentError("rule.type")
	}

	if req.Rule.Name == "" {
		return nil, twirp.RequiredArgumentError("rule.name")
	}

	if req.Rule.Description == "" {
		return nil, twirp.RequiredArgumentError("rule.description")
	}

	if req.Rule.Expression == "" {
		return nil, twirp.RequiredArgumentError("rule.expression")
	}

	if len(req.Rule.AppliesTo) == 0 {
		return nil, twirp.RequiredArgumentError("rule.applies_to")
	}

	_, err = expr.Compile(req.Rule.Expression,
		expr.Env(StatusRuleInput{}),
		expr.AsBool(),
	)
	if err != nil {
		return nil, twirp.InvalidArgumentError(
			"rule.expression", err.Error())
	}

	err = s.store.UpdateStatusRule(ctx, StatusRule{
		Type:        req.Rule.Type,
		Name:        req.Rule.Name,
		Description: req.Rule.Description,
		AccessRule:  req.Rule.AccessRule,
		AppliesTo:   req.Rule.AppliesTo,
		Expression:  req.Rule.Expression,
	})
	if err != nil {
		return nil, twirp.InternalErrorf("failed to store rule: %v", err)
	}

	return &repository.CreateStatusRuleResponse{}, nil
}

// DeleteStatusRule removes a status rule.
func (s *WorkflowsService) DeleteStatusRule(
	ctx context.Context, req *repository.DeleteStatusRuleRequest,
) (*repository.DeleteStatusRuleResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	err = s.store.DeleteStatusRule(ctx, req.Type, req.Name)
	if err != nil {
		return nil, twirp.InternalErrorf("failed to delete rule: %v", err)
	}

	return &repository.DeleteStatusRuleResponse{}, nil
}

// UpdateStatus creates or updates a status that can be used for documents.
func (s *WorkflowsService) UpdateStatus(
	ctx context.Context, req *repository.UpdateStatusRequest,
) (*repository.UpdateStatusResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	err = s.store.UpdateStatus(ctx, UpdateStatusRequest{
		Type:     req.Type,
		Name:     req.Name,
		Disabled: req.Disabled,
	})
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to update status: %w", err)
	}

	return &repository.UpdateStatusResponse{}, nil
}

// GetStatusRules returns all status rules.
func (s *WorkflowsService) GetStatusRules(
	ctx context.Context, _ *repository.GetStatusRulesRequest,
) (*repository.GetStatusRulesResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	res, err := s.store.GetStatusRules(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to read from store: %v", err)
	}

	list := make([]*repository.StatusRule, len(res))

	for i := range res {
		list[i] = &repository.StatusRule{
			Type:        res[i].Type,
			Name:        res[i].Name,
			Description: res[i].Description,
			AccessRule:  res[i].AccessRule,
			AppliesTo:   res[i].AppliesTo,
			Expression:  res[i].Expression,
		}
	}

	return &repository.GetStatusRulesResponse{
		Rules: list,
	}, nil
}

// GetStatuses lists all enabled statuses.
func (s *WorkflowsService) GetStatuses(
	ctx context.Context, req *repository.GetStatusesRequest,
) (*repository.GetStatusesResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeDocumentRead, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	res, err := s.store.GetStatuses(ctx, req.Type)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to read from store: %v", err)
	}

	list := make([]*repository.WorkflowStatus, len(res))

	for i := range res {
		list[i] = &repository.WorkflowStatus{
			Name: res[i].Name,
			Type: res[i].Type,
		}
	}

	return &repository.GetStatusesResponse{
		Statuses: list,
	}, nil
}

// DeleteWorkflow implements repository.Workflows.
func (s *WorkflowsService) DeleteWorkflow(
	ctx context.Context, req *repository.DeleteWorkflowRequest,
) (*repository.DeleteWorkflowResponse, error) {
	auth, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	err = s.store.DeleteDocumentWorkflow(ctx, req.Type)
	if err != nil {
		return nil, twirp.InternalErrorf("delete workflow: %v", err)
	}

	slog.Warn("document workflow deleted",
		"user", auth.Claims.Subject,
		"doc_type", req.Type,
	)

	return &repository.DeleteWorkflowResponse{}, nil
}

// GetWorkflow implements repository.Workflows.
func (s *WorkflowsService) GetWorkflow(
	ctx context.Context, req *repository.GetWorkflowRequest,
) (*repository.GetWorkflowResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeDocumentRead, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	wf, err := s.store.GetDocumentWorkflow(ctx, req.Type)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("no workflow defined")
	} else if err != nil {
		return nil, twirp.InternalErrorf("load workflow: %v", err)
	}

	flow := repository.DocumentWorkflow{
		StepZero:           wf.Configuration.StepZero,
		Checkpoint:         wf.Configuration.Checkpoint,
		NegativeCheckpoint: wf.Configuration.NegativeCheckpoint,
		Steps:              wf.Configuration.Steps,
	}

	return &repository.GetWorkflowResponse{
		Workflow:   &flow,
		Updated:    wf.Updated.Format(time.RFC3339),
		UpdaterUri: wf.UpdaterURI,
	}, nil
}

// SetWorkflow implements repository.Workflows.
func (s *WorkflowsService) SetWorkflow(
	ctx context.Context, req *repository.SetWorkflowRequest,
) (*repository.SetWorkflowResponse, error) {
	auth, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	if req.Workflow == nil {
		return nil, twirp.RequiredArgumentError("workflow")
	}

	if req.Workflow.StepZero == "" {
		return nil, twirp.RequiredArgumentError("workflow.step_zero")
	}

	if req.Workflow.Checkpoint == "" {
		return nil, twirp.RequiredArgumentError("workflow.checkpoint")
	}

	if req.Workflow.NegativeCheckpoint == "" {
		return nil, twirp.RequiredArgumentError("workflow.negative_checkpoint")
	}

	err = s.store.SetDocumentWorkflow(ctx, DocumentWorkflow{
		Type: req.Type,
		Configuration: DocumentWorkflowConfiguration{
			StepZero:           req.Workflow.StepZero,
			Checkpoint:         req.Workflow.Checkpoint,
			NegativeCheckpoint: req.Workflow.NegativeCheckpoint,
			Steps:              req.Workflow.Steps,
		},
		Updated:    time.Now(),
		UpdaterURI: auth.Claims.Subject,
	})
	if err != nil {
		return nil, twirp.InternalErrorf("store workflow: %v", err)
	}

	slog.Warn("document workflow updated",
		"user", auth.Claims.Subject,
		"doc_type", req.Type,
	)

	return &repository.SetWorkflowResponse{}, nil
}
