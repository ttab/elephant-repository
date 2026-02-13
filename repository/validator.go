package repository

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
)

const (
	LogKeyDeprecationLabel = "deprecation_label"
	LogKeyEntityRef        = "entity_ref"
)

type Validator struct {
	m                           sync.RWMutex
	val                         *revisor.Validator
	enforcedDeprecations        EnforcedDeprecations
	logger                      *slog.Logger
	deprecationsCounter         prometheus.CounterVec
	docsWithDeprecationsCounter prometheus.CounterVec
	stopChannel                 chan struct{}
	cancel                      func()

	refreshChan chan chan struct{}
}

type ValidatorStore interface {
	GetActiveSchemas(ctx context.Context) ([]*Schema, error)
	OnSchemaUpdate(ctx context.Context, ch chan SchemaEvent)
	GetEnforcedDeprecations(ctx context.Context) (EnforcedDeprecations, error)
	OnDeprecationUpdate(ctx context.Context, ch chan DeprecationEvent)
	GetTypeConfigurations(ctx context.Context) (map[string]TypeConfiguration, error)
	OnTypeConfigured(ctx context.Context, ch chan TypeConfiguredEvent)
}

func NewValidator(
	ctx context.Context, logger *slog.Logger,
	loader ValidatorStore, metricsRegisterer prometheus.Registerer,
) (*Validator, error) {
	ctx, cancel := context.WithCancel(ctx)

	v := Validator{
		logger:      logger,
		stopChannel: make(chan struct{}),
		cancel:      cancel,
		refreshChan: make(chan chan struct{}),
	}

	v.deprecationsCounter = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_deprecations_total",
			Help: "Number of encountered deprecations",
		}, []string{"label"})
	if err := metricsRegisterer.Register(v.deprecationsCounter); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	v.docsWithDeprecationsCounter = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_docs_with_deprecations_total",
			Help: "Number of encountered documents with deprecations",
		}, []string{"doc_type"})
	if err := metricsRegisterer.Register(v.docsWithDeprecationsCounter); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	err := v.loadSchemas(ctx, loader)
	if err != nil {
		return nil, fmt.Errorf("failed to load schemas: %w", err)
	}

	err = v.loadDeprecations(ctx, loader)
	if err != nil {
		return nil, fmt.Errorf("failed to load deprecations: %w", err)
	}

	go v.reloadLoop(ctx, logger, loader)

	return &v, nil
}

func (v *Validator) RefreshSchemas(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ch := make(chan struct{})

	select {
	case v.refreshChan <- ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (v *Validator) reloadLoop(
	ctx context.Context, logger *slog.Logger, loader ValidatorStore,
) {
	defer close(v.stopChannel)

	recheckInterval := 5 * time.Minute

	schemaSub := make(chan SchemaEvent, 1)
	deprecationSub := make(chan DeprecationEvent, 1)
	typeConfSub := make(chan TypeConfiguredEvent, 1)

	loader.OnSchemaUpdate(ctx, schemaSub)
	loader.OnDeprecationUpdate(ctx, deprecationSub)
	loader.OnTypeConfigured(ctx, typeConfSub)

	for {
		var refreshChan chan struct{}

		select {
		case <-ctx.Done():
			return
		case <-time.After(recheckInterval):
		case <-schemaSub:
		case <-deprecationSub:
		case <-typeConfSub:
		case refreshChan = <-v.refreshChan:
		}

		err := v.loadSchemas(ctx, loader)
		if err != nil {
			// TODO: add handler that reacts to LogKeyCountMetric
			logger.ErrorContext(ctx, "failed to refresh schemas",
				elephantine.LogKeyError, err,
				elephantine.LogKeyCountMetric, "elephant_schema_refresh_failure_count")
		}

		err = v.loadDeprecations(ctx, loader)
		if err != nil {
			// TODO: add handler that reacts to LogKeyCountMetric
			logger.ErrorContext(ctx, "failed to refresh deprecations",
				elephantine.LogKeyError, err,
				elephantine.LogKeyCountMetric, "elephant_deprecation_refresh_failure_count")
		}

		if refreshChan != nil {
			close(refreshChan)
		}
	}
}

func (v *Validator) loadSchemas(ctx context.Context, loader ValidatorStore) error {
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

	variants, err := loadVariants(ctx, loader)
	if err != nil {
		return fmt.Errorf("load variants: %w", err)
	}

	if len(variants) > 0 {
		val = val.WithVariants(variants...)
	}

	v.m.Lock()
	v.val = val
	v.m.Unlock()

	return nil
}

func loadVariants(
	ctx context.Context, loader ValidatorStore,
) ([]revisor.Variant, error) {
	configs, err := loader.GetTypeConfigurations(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"get type configurations: %w", err)
	}

	variantTypes := make(map[string][]string)

	for docType, conf := range configs {
		for _, v := range conf.Variants {
			variantTypes[v] = append(variantTypes[v], docType)
		}
	}

	variants := make([]revisor.Variant, 0, len(variantTypes))

	for name, types := range variantTypes {
		variants = append(variants, revisor.Variant{
			Name:  name,
			Types: types,
		})
	}

	return variants, nil
}

func (v *Validator) loadDeprecations(ctx context.Context, loader ValidatorStore) error {
	deprecations, err := loader.GetEnforcedDeprecations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get enforced deprecations: %w", err)
	}

	v.m.Lock()
	v.enforcedDeprecations = deprecations
	v.m.Unlock()

	return nil
}

func (v *Validator) ValidateDocument(
	ctx context.Context, document *newsdoc.Document,
) ([]revisor.ValidationResult, error) {
	v.m.RLock()
	val := v.val
	v.m.RUnlock()

	//nolint: wrapcheck
	return val.ValidateDocument(ctx, document,
		revisor.WithDeprecationHandler(v.deprecationHandler))
}

func (v *Validator) deprecationHandler(
	ctx context.Context, doc *newsdoc.Document,
	deprecation revisor.Deprecation, deprecationContext revisor.DeprecationContext,
) (revisor.DeprecationDecision, error) {
	v.m.RLock()
	enforced := v.enforcedDeprecations[deprecation.Label]
	v.m.RUnlock()

	if !enforced {
		var entityRef string

		if deprecationContext.Entity != nil {
			entityRef = deprecationContext.Entity.String()
		}

		v.logger.WarnContext(ctx, "use of deprecated value",
			elephantine.LogKeyDocumentUUID, doc.UUID,
			LogKeyDeprecationLabel, deprecation.Label,
			LogKeyEntityRef, entityRef)

		v.deprecationsCounter.WithLabelValues(deprecation.Label).Inc()
		v.docsWithDeprecationsCounter.WithLabelValues(doc.Type).Inc()
	}

	return revisor.DeprecationDecision{
		Enforce: enforced,
	}, nil
}

func (v *Validator) GetValidator() *revisor.Validator {
	v.m.RLock()
	defer v.m.RUnlock()

	return v.val
}

func (v *Validator) Stop() {
	v.cancel()
	<-v.stopChannel
}
