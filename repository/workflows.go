package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/internal"
	"golang.org/x/exp/slices"
	"golang.org/x/exp/slog"
)

type Workflows struct {
	m        sync.RWMutex
	vm       vm.VM
	statuses map[string]DocumentStatus
	rules    map[string][]compiledRule
}

type WorkflowLoader interface {
	GetStatuses(ctx context.Context) ([]DocumentStatus, error)
	GetStatusRules(ctx context.Context) ([]StatusRule, error)
	OnWorkflowUpdate(ctx context.Context, ch chan WorkflowEvent)
}

func NewWorkflows(
	ctx context.Context, logger *slog.Logger, loader WorkflowLoader,
) (*Workflows, error) {
	var w Workflows

	err := w.loadWorkflows(ctx, loader)
	if err != nil {
		return nil, fmt.Errorf("failed to load workflows: %w", err)
	}

	go w.reloadLoop(ctx, logger, loader)

	return &w, nil
}

func (w *Workflows) reloadLoop(
	ctx context.Context, logger *slog.Logger, loader WorkflowLoader,
) {
	recheckInterval := 5 * time.Minute

	sub := make(chan WorkflowEvent, 1)

	loader.OnWorkflowUpdate(ctx, sub)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(recheckInterval):
		case <-sub:
		}

		err := w.loadWorkflows(ctx, loader)
		if err != nil {
			// TODO: add handler that reacts to LogKeyCountMetric
			logger.ErrorCtx(ctx, "failed to refresh workfows", err,
				internal.LogKeyCountMetric, "elephant_workflow_refresh_failure_count")
		}
	}
}

func (w *Workflows) loadWorkflows(
	ctx context.Context, loader WorkflowLoader,
) error {
	statuses, err := loader.GetStatuses(ctx)
	if err != nil {
		return fmt.Errorf("failed to get statuses from loader: %w", err)
	}

	statusMap := make(map[string]DocumentStatus)

	for i := range statuses {
		statusMap[statuses[i].Name] = statuses[i]
	}

	rules, err := loader.GetStatusRules(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to get status rules from loader: %w", err)
	}

	ruleMap := make(map[string][]compiledRule)

	for i := range rules {
		p, err := expr.Compile(rules[i].Expression,
			expr.Env(StatusRuleInput{}),
			expr.AsBool(),
		)
		if err != nil {
			return fmt.Errorf(
				"failed to compile the rule %q expression %q: %w",
				rules[i].Name, rules[i].Expression, err)
		}

		compiled := compiledRule{
			StatusRule: rules[i],
			Exp:        p,
		}

		for _, status := range compiled.AppliesTo {
			ruleMap[status] = append(ruleMap[status], compiled)
		}
	}

	w.m.Lock()
	w.statuses = statusMap
	w.rules = ruleMap
	w.m.Unlock()

	return nil
}

func (w *Workflows) HasStatus(name string) bool {
	w.m.RLock()
	status, ok := w.statuses[name]
	w.m.RUnlock()

	return ok && !status.Disabled
}

type StatusRuleInput struct {
	Name        string
	Status      Status
	Update      DocumentUpdate
	Document    doc.Document
	VersionMeta doc.DataMap
	Heads       map[string]Status
	User        JWTClaims
}

type StatusRuleViolation struct {
	Name            string
	Description     string
	Error           string
	AccessViolation bool
}

type compiledRule struct {
	StatusRule
	Exp *vm.Program
}

func (w *Workflows) EvaluateRules(
	input StatusRuleInput,
) []StatusRuleViolation {
	w.m.RLock()
	rules, ok := w.rules[input.Name]
	w.m.RUnlock()

	if !ok || len(rules) == 0 {
		return nil
	}

	var violations []StatusRuleViolation

	for i := range rules {
		if !slices.Contains(rules[i].ForTypes, input.Document.Type) {
			continue
		}

		res, err := w.vm.Run(rules[i].Exp, input)
		valid, ok := res.(bool)

		if err != nil || !ok || !valid {
			v := StatusRuleViolation{
				Name:            rules[i].Name,
				Description:     rules[i].Description,
				AccessViolation: rules[i].AccessRule,
			}

			if err != nil {
				v.Error = err.Error()
			}

			violations = append(violations, v)
		}
	}

	return violations
}
