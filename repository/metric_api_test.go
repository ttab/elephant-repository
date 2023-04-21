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

	documentsClient := tc.DocumentsClient(t, test.StandardClaims(t, "doc_write"))

	ctx := test.Context(t)

	// test kinds
	_, err := client.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "wordcount",
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
				Name:        "wordcount",
				Aggregation: repository.MetricAggregation_REPLACE,
			},
		},
	}, kinds, "get the list of registered metric kinds")

	// test labels
	_, err = client.RegisterLabel(ctx, &repository.RegisterMetricLabelRequest{
		Name: "default",
		Kind: "wordcount",
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
				Name: "default",
				Kind: "wordcount",
			},
		},
	}, labels, "get the list of registered metric labels")

	_, err = client.DeleteLabel(ctx, &repository.DeleteMetricLabelRequest{
		Name: "default",
		Kind: "wordcount",
	})
	test.Must(t, err, "delete label")

	labels, err = client.GetLabels(ctx, &repository.GetMetricLabelsRequest{})
	test.Must(t, err, "get labels")

	test.EqualMessage(t, &repository.GetMetricLabelsResponse{
		Labels: []*repository.MetricLabel{},
	}, labels, "get the empty list of registered metric labels")

	_, err = client.DeleteKind(ctx, &repository.DeleteMetricKindRequest{
		Name: "wordcount",
	})
	test.Must(t, err, "delete kind")

	kinds, err = client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{},
	}, kinds, "get the empty list of registered metric kinds")

	// test register metric
	_, err = documentsClient.Update(ctx, &repository.UpdateRequest{
		Uuid: "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
		Document: &repository.Document{
			Type: "core/article",
			Uri:  "article://test/123",
		},
	})
	test.Must(t, err, "create test document")

	_, err = client.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "wordcount",
		Aggregation: repository.MetricAggregation_REPLACE,
	})
	test.Must(t, err, "recreate test kind")

	_, err = client.RegisterLabel(ctx, &repository.RegisterMetricLabelRequest{
		Name: "default",
		Kind: "wordcount",
	})
	test.Must(t, err, "refreate test label")

	_, err = client.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "revision",
		Aggregation: repository.MetricAggregation_REPLACE,
	})
	test.Must(t, err, "recreate test kind")

	_, err = client.RegisterLabel(ctx, &repository.RegisterMetricLabelRequest{
		Name: "feature",
		Kind: "revision",
	})
	test.Must(t, err, "recreate test label")

	_, err = client.RegisterMetric(ctx, &repository.RegisterMetricRequest{
		Uuid:  "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
		Kind:  "wordcount",
		Label: "default",
		Value: 123,
	})
	test.Must(t, err, "register the metric")

	_, err = client.RegisterMetric(ctx, &repository.RegisterMetricRequest{
		Uuid:  "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
		Kind:  "revision",
		Label: "default",
		Value: 2,
	})
	test.MustNot(t, err, "register a metric with an illegal kind/label combo")

	// test delete label
	_, err = client.DeleteLabel(ctx, &repository.DeleteMetricLabelRequest{
		Name: "default",
		Kind: "wordcount",
	})
	test.MustNot(t, err, "delete label in use")

	// test delete kind
	_, err = client.DeleteKind(ctx, &repository.DeleteMetricKindRequest{
		Name: "wordcount",
	})
	test.MustNot(t, err, "delete kind in use")
}
