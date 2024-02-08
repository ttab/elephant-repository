package repository

import (
	"context"

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

	if len(req.Rule.ForTypes) == 0 {
		return nil, twirp.RequiredArgumentError("rule.for_types")
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
		Name:        req.Rule.Name,
		Description: req.Rule.Description,
		AccessRule:  req.Rule.AccessRule,
		AppliesTo:   req.Rule.AppliesTo,
		ForTypes:    req.Rule.ForTypes,
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

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	err = s.store.DeleteStatusRule(ctx, req.Name)
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

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	err = s.store.UpdateStatus(ctx, UpdateStatusRequest{
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
			Name:        res[i].Name,
			Description: res[i].Description,
			AccessRule:  res[i].AccessRule,
			AppliesTo:   res[i].AppliesTo,
			ForTypes:    res[i].ForTypes,
			Expression:  res[i].Expression,
		}
	}

	return &repository.GetStatusRulesResponse{
		Rules: list,
	}, nil
}

// GetStatuses lists all enabled statuses.
func (s *WorkflowsService) GetStatuses(
	ctx context.Context, _ *repository.GetStatusesRequest,
) (*repository.GetStatusesResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeWorkflowAdmin)
	if err != nil {
		return nil, err
	}

	res, err := s.store.GetStatuses(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to read from store: %v", err)
	}

	list := make([]*repository.WorkflowStatus, len(res))

	for i := range res {
		list[i] = &repository.WorkflowStatus{
			Name: res[i].Name,
		}
	}

	return &repository.GetStatusesResponse{
		Statuses: list,
	}, nil
}
