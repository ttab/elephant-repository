package repository

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
)

type Workflows struct {
	m            sync.RWMutex
	vm           vm.VM
	statuses     map[string]DocumentStatus
	typeStatuses map[string][]string
	rules        map[string][]compiledRule
	workflows    map[string]DocumentWorkflow
}

type WorkflowLoader interface {
	GetStatuses(ctx context.Context, docType string) ([]DocumentStatus, error)
	GetStatusRules(ctx context.Context) ([]StatusRule, error)
	SetDocumentWorkflow(ctx context.Context, workflow DocumentWorkflow) error
	GetDocumentWorkflows(ctx context.Context) ([]DocumentWorkflow, error)
	GetDocumentWorkflow(ctx context.Context, docType string) (DocumentWorkflow, error)
	DeleteDocumentWorkflow(ctx context.Context, docType string) error
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
			logger.ErrorContext(ctx, "failed to refresh workfows",
				elephantine.LogKeyError, err,
				elephantine.LogKeyCountMetric, "elephant_workflow_refresh_failure_count")
		}
	}
}

func (w *Workflows) loadWorkflows(
	ctx context.Context, loader WorkflowLoader,
) error {
	statuses, err := loader.GetStatuses(ctx, "")
	if err != nil {
		return fmt.Errorf("get statuses: %w", err)
	}

	statusMap := make(map[string]DocumentStatus)
	typeStatuses := make(map[string][]string)

	for i := range statuses {
		key := typeScopedKey(statuses[i].Type, statuses[i].Name)
		statusMap[key] = statuses[i]

		if statuses[i].Disabled {
			continue
		}

		typeStatuses[statuses[i].Type] = append(
			typeStatuses[statuses[i].Type], statuses[i].Name)
	}

	rules, err := loader.GetStatusRules(ctx)
	if err != nil {
		return fmt.Errorf(
			"get status rules from loader: %w", err)
	}

	ruleMap := make(map[string][]compiledRule)

	for i := range rules {
		p, err := expr.Compile(rules[i].Expression,
			expr.Env(StatusRuleInput{}),
			expr.AsBool(),
		)
		if err != nil {
			return fmt.Errorf(
				"compile the rule %q expression %q: %w",
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

	workflows, err := loader.GetDocumentWorkflows(ctx)
	if err != nil {
		return fmt.Errorf("get document workflows: %w", err)
	}

	workflowMap := make(map[string]DocumentWorkflow, len(workflows))

	for _, wf := range workflows {
		workflowMap[wf.Type] = wf
	}

	w.m.Lock()
	w.statuses = statusMap
	w.typeStatuses = typeStatuses
	w.rules = ruleMap
	w.workflows = workflowMap
	w.m.Unlock()

	return nil
}

// GetDocumentWorkflow returns the workflow for a document type. If no explicit
// workflow has been configured an implicit workflow is synthesised from the
// type's configured statuses: no checkpoint, all statuses accepted as steps.
// Returns false only when the type has neither an explicit workflow nor any
// configured statuses.
func (w *Workflows) GetDocumentWorkflow(docType string) (DocumentWorkflow, bool) {
	w.m.RLock()
	defer w.m.RUnlock()

	if wf, ok := w.workflows[docType]; ok {
		return wf, true
	}

	steps := w.typeStatuses[docType]
	if len(steps) == 0 {
		return DocumentWorkflow{}, false
	}

	stepsCopy := make([]string, len(steps))
	copy(stepsCopy, steps)

	return DocumentWorkflow{
		Type: docType,
		Configuration: DocumentWorkflowConfiguration{
			Steps: stepsCopy,
		},
	}, true
}

func (w *Workflows) HasStatus(docType string, name string) bool {
	key := typeScopedKey(docType, name)

	w.m.RLock()
	status, ok := w.statuses[key]
	w.m.RUnlock()

	return ok && !status.Disabled
}

// HasStatusRule reports whether a status rule with the given name is currently
// loaded for the document type. It is primarily intended for tests that need
// to wait for a rule to propagate from the database to the provider.
func (w *Workflows) HasStatusRule(docType string, name string) bool {
	w.m.RLock()
	defer w.m.RUnlock()

	for _, rules := range w.rules {
		for _, r := range rules {
			if r.Type == docType && r.Name == name {
				return true
			}
		}
	}

	return false
}

func typeScopedKey(docType string, name string) string {
	return docType + ":" + name
}

type StatusRuleInput struct {
	Name          string
	Status        Status
	Update        DocumentUpdate
	Document      newsdoc.Document
	VersionMeta   newsdoc.DataMap
	Heads         map[string]StatusHead
	User          elephantine.JWTClaims
	WorkflowState WorkflowState
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
		if rules[i].Type != input.Document.Type {
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
