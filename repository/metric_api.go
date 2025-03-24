package repository

import (
	context "context"
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/ttab/elephant-api/repository"
	"github.com/twitchtv/twirp"
)

const labelMaxlen = 64

var nonLabelChars = regexp.MustCompile(`[^_[:alnum:]]`)

func ValidateLabel(label string) error {
	if len(label) > labelMaxlen {
		return fmt.Errorf("label too long")
	}

	nlc := nonLabelChars.FindString(label)
	if nlc != "" {
		return fmt.Errorf("unsupported character %q in label", nlc)
	}

	return nil
}

func ToAggregation(ma repository.MetricAggregation) (Aggregation, error) {
	switch ma {
	case repository.MetricAggregation_NONE:
		return AggregationNone, nil
	case repository.MetricAggregation_REPLACE:
		return AggregationReplace, nil
	case repository.MetricAggregation_INCREMENT:
		return AggregationIncrement, nil
	}

	return AggregationNone, fmt.Errorf("unknown MetricAggregation %v", ma)
}

func ToMetricAggregation(a Aggregation) (repository.MetricAggregation, error) {
	switch a {
	case AggregationNone:
		return repository.MetricAggregation_NONE, nil
	case AggregationReplace:
		return repository.MetricAggregation_REPLACE, nil
	case AggregationIncrement:
		return repository.MetricAggregation_INCREMENT, nil
	}

	return repository.MetricAggregation_NONE, fmt.Errorf("unknown Aggregation %v", a)
}

type MetricsService struct {
	store MetricStore
}

func NewMetricsService(store MetricStore) *MetricsService {
	return &MetricsService{
		store: store,
	}
}

var _ repository.Metrics = &MetricsService{}

// GetMetrics implements repository.Metrics.
func (m *MetricsService) GetMetrics(
	ctx context.Context, req *repository.GetMetricsRequest,
) (*repository.GetMetricsResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeMetricsAdmin, ScopeMetricsRead)
	if err != nil {
		return nil, err
	}

	if len(req.Uuids) == 0 {
		return nil, twirp.RequiredArgumentError("uuids")
	}

	uuids := make([]uuid.UUID, len(req.Uuids))

	for i := range req.Uuids {
		u, err := uuid.Parse(req.Uuids[i])
		if err != nil {
			return nil, twirp.InvalidArgument.Errorf("invalid UUID: %w", err)
		}

		uuids[i] = u
	}

	metrics, err := m.store.GetMetrics(ctx,
		uuids, req.Kinds)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"read metrics from store: %w", err)
	}

	res := repository.GetMetricsResponse{
		Documents: make(map[string]*repository.DocumentMetrics),
	}

	for _, m := range metrics {
		docUUID := m.UUID.String()

		dm := res.Documents[docUUID]
		if dm == nil {
			dm = &repository.DocumentMetrics{}

			res.Documents[docUUID] = dm
		}

		dm.Metrics = append(dm.Metrics, &repository.Metric{
			Kind:  m.Kind,
			Label: m.Label,
			Value: m.Value,
		})
	}

	return &res, nil
}

// GetKinds implements repository.Metrics.
func (m *MetricsService) GetKinds(
	ctx context.Context,
	_ *repository.GetMetricKindsRequest,
) (*repository.GetMetricKindsResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeMetricsAdmin)
	if err != nil {
		return nil, err
	}

	var res repository.GetMetricKindsResponse

	kinds, err := m.store.GetMetricKinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metric kinds: %w", err)
	}

	for i := range kinds {
		agg, err := ToMetricAggregation((kinds[i].Aggregation))
		if err != nil {
			return nil, fmt.Errorf("failed to decode aggregation: %w", err)
		}

		res.Kinds = append(res.Kinds, &repository.MetricKind{
			Name:        kinds[i].Name,
			Aggregation: agg,
		})
	}

	return &res, nil
}

// DeleteKind implements repository.Metrics.
func (m *MetricsService) DeleteKind(
	ctx context.Context,
	req *repository.DeleteMetricKindRequest,
) (*repository.DeleteMetricKindResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeMetricsAdmin)
	if err != nil {
		return nil, err
	}

	err = m.store.DeleteMetricKind(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to delete metric kind: %w", err)
	}

	return &repository.DeleteMetricKindResponse{}, nil
}

// RegisterKind implements repository.Metrics.
func (m *MetricsService) RegisterKind(
	ctx context.Context,
	req *repository.RegisterMetricKindRequest,
) (*repository.RegisterMetricKindResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeMetricsAdmin)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	if req.Aggregation == 0 {
		return nil, twirp.RequiredArgumentError("aggregation")
	}

	agg, err := ToAggregation(req.Aggregation)
	if err != nil {
		return nil, fmt.Errorf("failed to decode aggregation: %w", err)
	}

	err = m.store.RegisterMetricKind(ctx, req.Name, agg)
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil, twirp.FailedPrecondition.Error(
			"metric kind already exists")
	} else if err != nil {
		return nil, fmt.Errorf("failed to register metric kind: %w", err)
	}

	return &repository.RegisterMetricKindResponse{}, nil
}

// RegisterMetric implements repository.Metrics.
func (m *MetricsService) RegisterMetric(
	ctx context.Context,
	req *repository.RegisterMetricRequest,
) (*repository.RegisterMetricResponse, error) {
	_, err := RequireAnyScope(ctx,
		ScopeMetricsAdmin, ScopeMetricsWrite,
		Subscope("metrics_write", req.Kind),
	)
	if err != nil {
		return nil, err
	}

	docUUID, err := validateRequiredUUIDParam(req.Uuid)
	if err != nil {
		return nil, err
	}

	if req.Kind == "" {
		return nil, twirp.RequiredArgumentError("kind")
	}

	err = ValidateLabel(req.Label)
	if err != nil {
		return nil, twirp.InvalidArgument.Errorf("invalid argument: %w", err)
	}

	kind, err := m.store.GetMetricKind(ctx, req.Kind)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.FailedPrecondition.Error(err.Error())
	} else if err != nil {
		return nil, fmt.Errorf("failed to get metric kind: %w", err)
	}

	switch kind.Aggregation {
	case AggregationReplace:
		err = m.store.RegisterOrReplaceMetric(ctx, Metric{
			UUID:  docUUID,
			Kind:  req.Kind,
			Label: req.Label,
			Value: req.Value,
		})

	case AggregationIncrement:
		err = m.store.RegisterOrIncrementMetric(ctx, Metric{
			UUID:  docUUID,
			Kind:  req.Kind,
			Label: req.Label,
			Value: req.Value,
		})

	case AggregationNone:
		return nil, fmt.Errorf("unknown metric kind aggregation: %v", kind.Aggregation)
	}

	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.FailedPrecondition.Error(err.Error())
	} else if err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &repository.RegisterMetricResponse{}, nil
}
