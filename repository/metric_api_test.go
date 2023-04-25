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

	_, err = client.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "revisions",
		Aggregation: repository.MetricAggregation_INCREMENT,
	})
	test.Must(t, err, "register kind")

	kinds, err := client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name:        "revisions",
				Aggregation: repository.MetricAggregation_INCREMENT,
				Labels:      []*repository.MetricLabel{},
			},
			{
				Name:        "wordcount",
				Aggregation: repository.MetricAggregation_REPLACE,
				Labels:      []*repository.MetricLabel{},
			},
		},
	}, kinds, "get the list of registered metric kinds")

	// test labels
	_, err = client.RegisterLabel(ctx, &repository.RegisterMetricLabelRequest{
		Name: "default",
		Kind: "wordcount",
	})
	test.Must(t, err, "register label")

	_, err = client.RegisterLabel(ctx, &repository.RegisterMetricLabelRequest{
		Name: "extra",
		Kind: "wordcount",
	})
	test.Must(t, err, "register label")

	kinds, err = client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name:        "revisions",
				Aggregation: repository.MetricAggregation_INCREMENT,
				Labels:      []*repository.MetricLabel{},
			},
			{
				Name:        "wordcount",
				Aggregation: repository.MetricAggregation_REPLACE,
				Labels: []*repository.MetricLabel{
					{
						Name: "default",
					},
					{
						Name: "extra",
					},
				},
			},
		},
	}, kinds, "get the list of registered metric kinds and their labels")

	_, err = client.DeleteLabel(ctx, &repository.DeleteMetricLabelRequest{
		Name: "default",
		Kind: "wordcount",
	})
	test.Must(t, err, "delete label")

	kinds, err = client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name:        "revisions",
				Aggregation: repository.MetricAggregation_INCREMENT,
				Labels:      []*repository.MetricLabel{},
			},
			{
				Name:        "wordcount",
				Aggregation: repository.MetricAggregation_REPLACE,
				Labels: []*repository.MetricLabel{
					{
						Name: "extra",
					},
				},
			},
		},
	}, kinds, "get the list of registered metric kinds and their labels mninus the deleted")

	_, err = client.DeleteKind(ctx, &repository.DeleteMetricKindRequest{
		Name: "wordcount",
	})
	test.Must(t, err, "delete kind")

	kinds, err = client.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name:        "revisions",
				Aggregation: repository.MetricAggregation_INCREMENT,
				Labels:      []*repository.MetricLabel{},
			},
		},
	}, kinds, "get the list of registered metric kinds minus deleted")

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
