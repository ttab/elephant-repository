package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ttab/elephant/internal"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
	"golang.org/x/exp/slog"
)

type Validator struct {
	m   sync.RWMutex
	val *revisor.Validator
}

type SchemaLoader interface {
	GetActiveSchemas(ctx context.Context) ([]*Schema, error)
	OnSchemaUpdate(ctx context.Context, ch chan SchemaEvent)
}

func NewValidator(
	ctx context.Context, logger *slog.Logger, loader SchemaLoader,
) (*Validator, error) {
	var v Validator

	err := v.loadSchemas(ctx, loader)
	if err != nil {
		return nil, fmt.Errorf("failed to load schemas: %w", err)
	}

	go v.reloadLoop(ctx, logger, loader)

	return &v, nil
}

func (v *Validator) reloadLoop(
	ctx context.Context, logger *slog.Logger, loader SchemaLoader,
) {
	recheckInterval := 5 * time.Minute

	sub := make(chan SchemaEvent, 1)

	loader.OnSchemaUpdate(ctx, sub)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(recheckInterval):
		case <-sub:
		}

		err := v.loadSchemas(ctx, loader)
		if err != nil {
			// TODO: add handler that reacts to LogKeyCountMetric
			logger.ErrorCtx(ctx, "failed to refresh schemas",
				internal.LogKeyError, err,
				internal.LogKeyCountMetric, "elephant_schema_refresh_failure_count")
		}
	}
}

func (v *Validator) loadSchemas(ctx context.Context, loader SchemaLoader) error {
	schemas, err := loader.GetActiveSchemas(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active schemas: %w", err)
	}

	var constraints []revisor.ConstraintSet

	for _, schema := range schemas {
		constraints = append(constraints, schema.Specification)
	}

	val, err := revisor.NewValidator(constraints...)
	if err != nil {
		return fmt.Errorf(
			"failed to create a validator from the constraints: %w", err)
	}

	v.m.Lock()
	v.val = val
	v.m.Unlock()

	return nil
}

func (v *Validator) ValidateDocument(document *newsdoc.Document) []revisor.ValidationResult {
	v.m.RLock()
	val := v.val
	v.m.RUnlock()

	return val.ValidateDocument(document)
}

func (v *Validator) GetValidator() *revisor.Validator {
	v.m.RLock()
	defer v.m.RUnlock()

	return v.val
}
