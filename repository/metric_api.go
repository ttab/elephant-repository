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

// type Metric struct {
// 	Uuid  string
// 	Kind  MetricKind
// 	Label MetricLabel
// }
//
// type MetricKind struct {
// 	ID   int32
// 	Name string
// }
//
// type MetricLabel struct {
// 	ID   int32
// 	Name string
// }

func NewMetricsService(store MetricStore) *MetricsService {
	return &MetricsService{
		store: store,
	}
}

// GetKinds implements repository.Metrics
func (m *MetricsService) GetKinds(
	ctx context.Context,
	req *repository.GetMetricKindsRequest,
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
		res.Kinds = append(res.Kinds, &repository.MetricKind{
			Name: kinds[i].Name,
		})
	}

	return &res, nil
}

// DeleteKind implements repository.Metrics
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

// RegisterKind implements repository.Metrics
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

	err = m.store.RegisterMetricKind(ctx, req.Name)
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil, twirp.FailedPrecondition.Error(
			"metric kind already exists")
	} else if err != nil {
		return nil, fmt.Errorf("failed to register metric kind: %w", err)
	}

	return &repository.RegisterMetricKindResponse{}, nil
}
