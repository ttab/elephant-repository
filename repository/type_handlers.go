package repository

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
)

func NewTypeConfigurations(
	logger *slog.Logger,
	defaultTZ *time.Location,
) *TypeConfigurations {
	return &TypeConfigurations{
		logger:       logger,
		defaultTZ:    defaultTZ,
		initWait:     make(chan struct{}),
		tsExtractors: map[string]*DocumentTimespanExtractor{},
	}
}

// TypeConfigurations keeps track of the current type configurations and
// provides methods for applying the configurations on documents.
type TypeConfigurations struct {
	logger    *slog.Logger
	defaultTZ *time.Location

	initOnce    sync.Once
	initialised bool
	initWait    chan struct{}

	m             sync.RWMutex
	confs         map[string]TypeConfiguration
	tsExtractors  map[string]*DocumentTimespanExtractor
	lblExtractors map[string]*LabelsExtractor
}

// Run initialises the configurations and listens for updates. Blocks until the
// context is cancelled.
func (th *TypeConfigurations) Run(ctx context.Context, store *PGDocStore) error {
	updates := make(chan TypeConfiguredEvent, 1)

	store.OnTypeConfigured(ctx, updates)

	for {
		var retryChan <-chan time.Time

		err := th.refreshConfig(ctx, store)
		switch {
		case err != nil && !th.initialised:
			return fmt.Errorf("load current config: %w", err)
		case err != nil:
			// If we have a working configuration we'll keep using
			// it instead of "crashing".
			th.logger.ErrorContext(ctx,
				"failed to refresh document type configuration",
				elephantine.LogKeyError, err)

			retryChan = time.After(10 * time.Second)
		default:
			th.initOnce.Do(func() {
				close(th.initWait)
				th.initialised = true
			})
		}

		select {
		case <-ctx.Done():
			return nil
		case <-updates:
		case <-retryChan:
		}
	}
}

func (th *TypeConfigurations) refreshConfig(
	ctx context.Context,
	store *PGDocStore,
) error {
	th.m.Lock()
	defer th.m.Unlock()

	configs, err := store.GetTypeConfigurations(ctx)
	if err != nil {
		return fmt.Errorf("read configs from DB: %w", err)
	}

	tsEx := make(map[string]*DocumentTimespanExtractor, len(configs))
	lblEx := make(map[string]*LabelsExtractor, len(configs))
	confs := make(map[string]TypeConfiguration, len(configs))

	for t, c := range configs {
		if len(c.TimeExpressions) > 0 {
			te, err := NewDocumentTimespanExtractor(
				c.TimeExpressions, th.defaultTZ)
			if err != nil {
				return fmt.Errorf(
					"create timespan extractor for %q: %w",
					t, err)
			}

			tsEx[t] = te
		}

		if len(c.LabelExpressions) > 0 {
			le, err := NewLabelsExtractor(c.LabelExpressions)
			if err != nil {
				return fmt.Errorf("create labels extractor for %q: %w",
					t, err)
			}

			lblEx[t] = le
		}

		confs[t] = c
	}

	th.tsExtractors = tsEx
	th.lblExtractors = lblEx
	th.confs = confs

	return nil
}

// TimespansForDocument calculates the timespans for a given document.
func (th *TypeConfigurations) TimespansForDocument(
	ctx context.Context, doc newsdoc.Document,
) ([]Timespan, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-th.initWait:
	}

	th.m.RLock()
	ex, ok := th.tsExtractors[doc.Type]
	th.m.RUnlock()

	if !ok {
		return nil, nil
	}

	spans, err := ex.Extract(doc)
	if err != nil {
		return nil, err
	}

	return spans, nil
}

// LabelsForDocument calculates the timespans for a given document.
func (th *TypeConfigurations) LabelsForDocument(
	ctx context.Context, doc newsdoc.Document,
) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-th.initWait:
	}

	th.m.RLock()
	ex, ok := th.lblExtractors[doc.Type]
	th.m.RUnlock()

	if !ok {
		return nil, nil
	}

	labels, err := ex.Extract(doc)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

// GetConfiguration returns the current configuration for a type.
func (th *TypeConfigurations) GetConfiguration(
	ctx context.Context, docType string,
) (TypeConfiguration, bool, error) {
	select {
	case <-ctx.Done():
		return TypeConfiguration{}, false, ctx.Err()
	case <-th.initWait:
	}

	th.m.RLock()
	c, ok := th.confs[docType]
	th.m.RUnlock()

	return c, ok, nil
}
