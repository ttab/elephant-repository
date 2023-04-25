package repository

import (
	context "context"
	"fmt"

	"github.com/ttab/elephant/rpc/repository"
	"github.com/twitchtv/twirp"
)

type MetricsService struct {
	store MetricStore
}

func NewMetricsService(store MetricStore) *MetricsService {
	return &MetricsService{
		store: store,
	}
}

// GetKinds implements repository.Metrics.
func (m *MetricsService) GetKinds(
	ctx context.Context,
	_ *repository.GetMetricKindsRequest,
) (*repository.GetMetricKindsResponse, error) {
	err := requireAnyScope(ctx, "metrics_admin") // TODO: correct scope
	if err != nil {
		return nil, err
	}

	var res repository.GetMetricKindsResponse

	kinds, err := m.store.GetMetricKinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metric kinds: %w", err)
	}

	for i := range kinds {
		fmt.Println(kinds[i])
		kind := repository.MetricKind{
			Name:        kinds[i].Name,
			Aggregation: repository.MetricAggregation(kinds[i].Aggregation),
			Labels:      []*repository.MetricLabel{},
		}
		for j := range kinds[i].Labels {
			kind.Labels = append(kind.Labels, &repository.MetricLabel{Name: kinds[i].Labels[j].Name})
		}

		res.Kinds = append(res.Kinds, &kind)
	}

	return &res, nil
}

// DeleteKind implements repository.Metrics.
func (m *MetricsService) DeleteKind(
	ctx context.Context,
	req *repository.DeleteMetricKindRequest,
) (*repository.DeleteMetricKindResponse, error) {
	err := requireAnyScope(ctx, "metrics_admin")
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
	err := requireAnyScope(ctx, "metrics_admin")
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	if req.Aggregation == 0 {
		return nil, twirp.RequiredArgumentError("aggregation")
	}

	err = m.store.RegisterMetricKind(ctx, req.Name, Aggregation(req.Aggregation))
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil, twirp.FailedPrecondition.Error(
			"metric kind already exists")
	} else if err != nil {
		return nil, fmt.Errorf("failed to register metric kind: %w", err)
	}

	return &repository.RegisterMetricKindResponse{}, nil
}

// GetLabels implements repository.Metrics.
func (m *MetricsService) GetLabels(
	ctx context.Context,
	_ *repository.GetMetricLabelsRequest,
) (*repository.GetMetricLabelsResponse, error) {
	err := requireAnyScope(ctx, "metrics_admin") // TODO: correct scope
	if err != nil {
		return nil, err
	}

	var res repository.GetMetricLabelsResponse

	labels, err := m.store.GetMetricLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metric labels: %w", err)
	}

	for i := range labels {
		res.Labels = append(res.Labels, &repository.MetricLabel{
			Name: labels[i].Name,
		})
	}

	return &res, nil
}

// DeleteLabel implements repository.Metrics.
func (m *MetricsService) DeleteLabel(
	ctx context.Context,
	req *repository.DeleteMetricLabelRequest,
) (*repository.DeleteMetricLabelResponse, error) {
	err := requireAnyScope(ctx, "metrics_admin")
	if err != nil {
		return nil, err
	}

	err = m.store.DeleteMetricLabel(ctx, req.Name, req.Kind)
	if err != nil {
		return nil, fmt.Errorf("failed to delete metric label: %w", err)
	}

	return &repository.DeleteMetricLabelResponse{}, nil
}

// RegisterLabel implements repository.Metrics.
func (m *MetricsService) RegisterLabel(
	ctx context.Context,
	req *repository.RegisterMetricLabelRequest,
) (*repository.RegisterMetricLabelResponse, error) {
	err := requireAnyScope(ctx, "metrics_admin")
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	err = m.store.RegisterMetricLabel(ctx, req.Name, req.Kind)
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil, twirp.FailedPrecondition.Error(
			"metric label already exists")
	} else if err != nil {
		return nil, fmt.Errorf("failed to register metric label: %w", err)
	}

	return &repository.RegisterMetricLabelResponse{}, nil
}

// RegisterMetric implements repository.Metrics.
func (m *MetricsService) RegisterMetric(
	ctx context.Context,
	req *repository.RegisterMetricRequest,
) (*repository.RegisterMetricResponse, error) {
	err := requireAnyScope(ctx, "metrics_admin", "metrics_write")
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

	kind, err := m.store.GetMetricKind(ctx, req.Kind)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.FailedPrecondition.Error(err.Error())
	} else if err != nil {
		return nil, fmt.Errorf("failed to get metric kind: %w", err)
	}

	switch kind.Aggregation {
	case AggregationREPLACE:
		err = m.store.RegisterOrReplaceMetric(ctx, Metric{
			UUID:  docUUID,
			Kind:  req.Kind,
			Label: req.Label,
			Value: req.Value,
		})

	case AggregationINCREMENT:
		err = m.store.RegisterOrIncrementMetric(ctx, Metric{
			UUID:  docUUID,
			Kind:  req.Kind,
			Label: req.Label,
			Value: req.Value,
		})

	case AggregationNONE:
		return nil, fmt.Errorf("unknown metric kind aggregation: %v", kind.Aggregation)
	}

	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.FailedPrecondition.Error(err.Error())
	} else if err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &repository.RegisterMetricResponse{}, nil
}
