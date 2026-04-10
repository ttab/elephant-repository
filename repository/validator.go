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
	activeGenerationID          int64
	pendingVal                  *revisor.Validator
	pendingGenerationID         int64
	enforcedDeprecations        EnforcedDeprecations
	logger                      *slog.Logger
	deprecationsCounter         prometheus.CounterVec
	docsWithDeprecationsCounter prometheus.CounterVec
	pendingFailureCounter       prometheus.CounterVec
	stopChannel                 chan struct{}
	cancel                      func()

	refreshChan chan chan struct{}
}

type ValidatorStore interface {
	GetActiveSchemas(ctx context.Context) ([]*Schema, error)
	GetActiveGenerationID(ctx context.Context) (int64, error)
	GetPendingGenerationSchemas(ctx context.Context) ([]*Schema, error)
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
		return nil, fmt.Errorf("register deprecations metric: %w", err)
	}

	v.docsWithDeprecationsCounter = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_docs_with_deprecations_total",
			Help: "Number of encountered documents with deprecations",
		}, []string{"doc_type"})
	if err := metricsRegisterer.Register(v.docsWithDeprecationsCounter); err != nil {
		return nil, fmt.Errorf("register docs with deprecations metric: %w", err)
	}

	v.pendingFailureCounter = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_pending_validation_failures_total",
			Help: "Number of validation failures against the pending schema generation",
		}, []string{"doc_type", "error"})
	if err := metricsRegisterer.Register(v.pendingFailureCounter); err != nil {
		return nil, fmt.Errorf("register pending validation metric: %w", err)
	}

	err := v.loadSchemas(ctx, loader)
	if err != nil {
		return nil, fmt.Errorf("load schemas: %w", err)
	}

	err = v.loadDeprecations(ctx, loader)
	if err != nil {
		return nil, fmt.Errorf("load deprecations: %w", err)
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
		return fmt.Errorf("get active schemas: %w", err)
	}

	var constraints []revisor.ConstraintSet

	for _, schema := range schemas {
		constraints = append(constraints, schema.Specification)
	}

	val, err := revisor.NewValidator(constraints...)
	if err != nil {
		return fmt.Errorf(
			"create a validator from the constraints: %w", err)
	}

	variants, err := loadVariants(ctx, loader)
	if err != nil {
		return fmt.Errorf("load variants: %w", err)
	}

	if len(variants) > 0 {
		val = val.WithVariants(variants...)
	}

	// Load active generation ID.
	genID, err := loader.GetActiveGenerationID(ctx)
	if err != nil {
		// Generation 0 means no generation exists yet.
		genID = 0
	}

	// Load pending generation validator if one exists.
	var pendingVal *revisor.Validator

	var pendingGenID int64

	pendingSchemas, err := loader.GetPendingGenerationSchemas(ctx)
	if err == nil && len(pendingSchemas) > 0 {
		var pendingConstraints []revisor.ConstraintSet

		for _, s := range pendingSchemas {
			pendingConstraints = append(pendingConstraints, s.Specification)
		}

		pVal, pErr := revisor.NewValidator(pendingConstraints...)
		if pErr != nil {
			v.logger.WarnContext(ctx,
				"create pending validator",
				elephantine.LogKeyError, pErr)
		} else {
			if len(variants) > 0 {
				pVal = pVal.WithVariants(variants...)
			}

			pendingVal = pVal

			// We don't have a direct method to get the pending
			// generation ID from the store, but we can use 0 as a
			// sentinel. The pending generation exists, so we set a
			// non-zero value to indicate we have one. The exact ID
			// is not needed by the validator.
			pendingGenID = 1
		}
	}

	v.m.Lock()
	v.val = val
	v.activeGenerationID = genID
	v.pendingVal = pendingVal
	v.pendingGenerationID = pendingGenID
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
		return fmt.Errorf("get enforced deprecations: %w", err)
	}

	v.m.Lock()
	v.enforcedDeprecations = deprecations
	v.m.Unlock()

	return nil
}

// ValidateDocument validates a document against the active schema generation.
// If a pending generation exists, it also runs soft validation against it,
// logging and counting failures without rejecting the write.
func (v *Validator) ValidateDocument(
	ctx context.Context, document *newsdoc.Document,
) ([]revisor.ValidationResult, error) {
	v.m.RLock()
	val := v.val
	pendingVal := v.pendingVal
	v.m.RUnlock()

	results, err := val.ValidateDocument(ctx, document,
		revisor.WithDeprecationHandler(v.deprecationHandler))
	if err != nil {
		return nil, fmt.Errorf("validate document: %w", err)
	}

	// Soft validation against pending generation.
	if pendingVal != nil {
		pResults, pErr := pendingVal.ValidateDocument(ctx, document)
		if pErr != nil {
			v.logger.WarnContext(ctx,
				"pending generation validation error",
				elephantine.LogKeyError, pErr,
				elephantine.LogKeyDocumentUUID, document.UUID)
		} else {
			for _, r := range pResults {
				v.pendingFailureCounter.WithLabelValues(
					document.Type, r.Error).Inc()
			}

			if len(pResults) > 0 {
				v.logger.WarnContext(ctx,
					"pending generation validation failures",
					elephantine.LogKeyDocumentUUID, document.UUID,
					"error_count", len(pResults))
			}
		}
	}

	return results, nil
}

// ActiveGenerationID returns the ID of the currently active schema generation.
func (v *Validator) ActiveGenerationID() int64 {
	v.m.RLock()
	defer v.m.RUnlock()

	return v.activeGenerationID
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

// PruneDocument prunes a document using the active schema generation,
// removing non-conforming parts where possible.
func (v *Validator) PruneDocument(
	ctx context.Context, document *newsdoc.Document,
) ([]revisor.ValidationResult, error) {
	v.m.RLock()
	val := v.val
	v.m.RUnlock()

	results, err := val.Prune(ctx, document)
	if err != nil {
		return nil, fmt.Errorf("prune document: %w", err)
	}

	return results, nil
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
