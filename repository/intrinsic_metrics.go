package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
)

type MetricRegisterer interface {
	RegisterMetricKind(
		ctx context.Context, name string, aggregation Aggregation,
	) error
	RegisterOrReplaceMetric(
		ctx context.Context, metric Metric,
	) error
	RegisterOrIncrementMetric(
		ctx context.Context, metric Metric,
	) error
}

type Measurement struct {
	Label string
	Value int64
}

type MetricCalculator interface {
	IsApplicableTo(doc newsdoc.Document) bool
	GetKind() MetricKind
	MeasureDocument(
		doc newsdoc.Document,
	) ([]Measurement, error)
}

func NewIntrinsicMetrics(
	logger *slog.Logger,
	calculators []MetricCalculator,
) *IntrinsicMetrics {
	return &IntrinsicMetrics{
		logger:      logger,
		calculators: calculators,
	}
}

type IntrinsicMetrics struct {
	logger      *slog.Logger
	calculators []MetricCalculator
}

func (im *IntrinsicMetrics) Setup(
	ctx context.Context,
	reg MetricRegisterer,
) error {
	for _, c := range im.calculators {
		kind := c.GetKind()

		err := reg.RegisterMetricKind(ctx, kind.Name, kind.Aggregation)
		if err != nil && !IsDocStoreErrorCode(err, ErrCodeExists) {
			return fmt.Errorf("register metric kind %q: %w", kind.Name, err)
		}
	}

	return nil
}

func (im *IntrinsicMetrics) MeasureDocument(
	ctx context.Context,
	reg MetricRegisterer,
	docUUID uuid.UUID,
	doc newsdoc.Document,
) {
	for _, c := range im.calculators {
		if !c.IsApplicableTo(doc) {
			continue
		}

		kind := c.GetKind()

		measures, err := c.MeasureDocument(doc)
		if err != nil {
			im.logger.ErrorContext(ctx, "failed to calculate intrinsic document metrics",
				elephantine.LogKeyError, err,
				elephantine.LogKeyDocumentUUID, docUUID.String(),
				elephantine.LogKeyName, kind.Name,
			)

			continue
		}

		for _, m := range measures {
			metric := Metric{
				UUID:  docUUID,
				Kind:  kind.Name,
				Label: m.Label,
				Value: m.Value,
			}

			var err error

			switch kind.Aggregation {
			case AggregationReplace:
				err = reg.RegisterOrReplaceMetric(ctx, metric)
			case AggregationIncrement:
				err = reg.RegisterOrIncrementMetric(ctx, metric)
			case AggregationNone:
				err = errors.New("no aggregation specified")
			default:
				err = fmt.Errorf("unknown metric aggregation: %v", kind.Aggregation)
			}

			if err != nil {
				im.logger.ErrorContext(ctx, "failed to register intrinsic document metric",
					elephantine.LogKeyError, err,
					elephantine.LogKeyDocumentUUID, docUUID.String(),
					elephantine.LogKeyName, kind.Name,
				)

				continue
			}
		}
	}
}
