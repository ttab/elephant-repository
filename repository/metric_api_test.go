package repository_test

import (
	"testing"

	"github.com/ttab/elephant/internal/test"
	"github.com/ttab/elephant/rpc/repository"
	"golang.org/x/exp/slog"
)

func TestIntegrationMetrics(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.MetricsClient(t, test.StandardClaims(t, "metrics_admin"))

	ctx := test.Context(t)

	// test kinds
	_, err := client.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name: "wordcount",
    Aggregation: repository.MetricAggregation_REPLACE,
	})
	test.Must(t, err, "register kind")

	if err != nil {
		test.MustNot(t, err, "fail to register kind")
	}

	kinds, err := client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name: "wordcount",
        Aggregation: repository.MetricAggregation_REPLACE,
			},
		},
	}, kinds, "get the list of registered metric kinds")

	_, err = client.DeleteKind(ctx, &repository.DeleteMetricKindRequest{
		Name: "wordcount",
	})
	test.Must(t, err, "delete kind")

	kinds, err = client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{},
	}, kinds, "get the empty list of registered metric kinds")

	// test labels
	_, err = client.RegisterLabel(ctx, &repository.RegisterMetricLabelRequest{
		Name: "wordcount",
	})
	test.Must(t, err, "register label")

	if err != nil {
		test.MustNot(t, err, "fail to register label")
	}

	labels, err := client.GetLabels(ctx, &repository.GetMetricLabelsRequest{})
	test.Must(t, err, "get labels")

	test.EqualMessage(t, &repository.GetMetricLabelsResponse{
		Labels: []*repository.MetricLabel{
			{
				Name: "wordcount",
			},
		},
	}, labels, "get the list of registered metric labels")

	_, err = client.DeleteLabel(ctx, &repository.DeleteMetricLabelRequest{
		Name: "wordcount",
	})
	test.Must(t, err, "delete label")

	labels, err = client.GetLabels(ctx, &repository.GetMetricLabelsRequest{})
	test.Must(t, err, "get labels")

	test.EqualMessage(t, &repository.GetMetricLabelsResponse{
		Labels: []*repository.MetricLabel{},
	}, labels, "get the empty list of registered metric labels")
}
