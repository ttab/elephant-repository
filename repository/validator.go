package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/revisor"
	"github.com/ttab/elephant/revisor/constraints"
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
	ctx context.Context, logger *logrus.Logger, loader SchemaLoader,
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
	ctx context.Context, logger *logrus.Logger, loader SchemaLoader,
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
			// TODO: this should be a metric that we can alert on as
			// well. Look at slog, could we have a log level that
			// triggers a counter metric? Would be neat.
			logger.WithContext(ctx).WithError(
				err,
			).Error("failed to refresh schemas")
		}
	}
}

func (v *Validator) loadSchemas(ctx context.Context, loader SchemaLoader) error {
	core, err := constraints.CoreSchema()
	if err != nil {
		return fmt.Errorf("failed to get core schema: %w", err)
	}

	schemas, err := loader.GetActiveSchemas(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active schemas: %w", err)
	}

	constraints := []revisor.ConstraintSet{
		core,
	}

	for _, schema := range schemas {
		if schema.Name == "core" {
			constraints[0] = schema.Specification
		} else {
			constraints = append(constraints, schema.Specification)
		}
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

func (v *Validator) ValidateDocument(document *doc.Document) []revisor.ValidationResult {
	v.m.RLock()
	val := v.val
	v.m.RUnlock()

	return val.ValidateDocument(document)
}
