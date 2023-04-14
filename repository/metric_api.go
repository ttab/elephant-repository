package repository

import (
	context "context"
	"fmt"

	"github.com/ttab/elephant/rpc/repository"
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
func (*MetricsService) GetKinds(
	ctx context.Context,
	req *repository.GetMetricKindsRequest,
) (*repository.GetMetricKindsResponse, error) {
	fmt.Println("HEJ")
	err := requireAnyScope(ctx, "metrics_admin") // TODO: correct scope
	if err != nil {
		return nil, err
	}

	panic("unimplemented")
}

// RegisterKind implements repository.Metrics
func (*MetricsService) RegisterKind(
	context.Context,
	*repository.RegisterMetricKindRequest,
) (*repository.RegisterSchemaKindResponse, error) {
	panic("unimplemented")
}
