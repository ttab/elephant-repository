package repository_test

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	repo "github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

func TestValidateLabel(t *testing.T) {
	err := repo.ValidateLabel("Panda_123")
	test.Must(t, err, "validate simple string ")

	err = repo.ValidateLabel("panda(123)cub")
	test.MustNot(t, err, "validate invalid chars")

	err = repo.ValidateLabel(strings.Repeat("a", 1000))
	test.MustNot(t, err, "validate too long strings")
}

func TestIntegrationMetrics(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		NoCharcount: true,
	})

	clientAdmin := tc.MetricsClient(t,
		itest.StandardClaims(t, "metrics_admin"))

	documentsClient := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_write"))

	ctx := t.Context()

	// Test kind registration.

	_, err := clientAdmin.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "wordcount",
		Aggregation: repository.MetricAggregation_REPLACE,
	})
	test.Must(t, err, "register kind")

	_, err = clientAdmin.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "revisions",
		Aggregation: repository.MetricAggregation_INCREMENT,
	})
	test.Must(t, err, "register kind")

	_, err = clientAdmin.RegisterKind(ctx, &repository.RegisterMetricKindRequest{
		Name:        "revisions",
		Aggregation: repository.MetricAggregation_REPLACE,
	})
	test.MustNot(t, err, "register duplicate kind")

	kinds, err := clientAdmin.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name:        "revisions",
				Aggregation: repository.MetricAggregation_INCREMENT,
			},
			{
				Name:        "wordcount",
				Aggregation: repository.MetricAggregation_REPLACE,
			},
		},
	}, kinds, "get the list of registered metric kinds")

	_, err = clientAdmin.DeleteKind(ctx, &repository.DeleteMetricKindRequest{
		Name: "revisions",
	})
	test.Must(t, err, "delete kind")

	kinds, err = clientAdmin.GetKinds(ctx, &repository.GetMetricKindsRequest{})
	test.Must(t, err, "get kinds")

	test.EqualMessage(t, &repository.GetMetricKindsResponse{
		Kinds: []*repository.MetricKind{
			{
				Name:        "wordcount",
				Aggregation: repository.MetricAggregation_REPLACE,
			},
		},
	}, kinds, "get the list of registered metric kinds minus deleted")

	// Test register metric.

	_, err = documentsClient.Update(ctx, &repository.UpdateRequest{
		Uuid: "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
		Document: &newsdoc.Document{
			Type:     "core/article",
			Uri:      "article://test/123",
			Language: "en",
			Meta: []*newsdoc.Block{
				{
					Type:  "core/newsvalue",
					Value: "3",
				},
			},
		},
	})
	test.Must(t, err, "create test document")

	clientWrite := tc.MetricsClient(t,
		itest.StandardClaims(t, "metrics_write"))

	_, err = clientWrite.RegisterMetric(ctx,
		&repository.RegisterMetricRequest{
			Uuid:  "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
			Kind:  "wordcount",
			Label: "default",
			Value: 123,
		})
	test.Must(t, err, "register the metric")

	clientWriteWC := tc.MetricsClient(t,
		itest.StandardClaims(t, "metrics_write:wordcount"))

	_, err = clientWriteWC.RegisterMetric(ctx,
		&repository.RegisterMetricRequest{
			Uuid:  "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
			Kind:  "wordcount",
			Label: "default",
			Value: 123,
		})
	test.Must(t, err, "register the metric")

	clientWriteRev := tc.MetricsClient(t,
		itest.StandardClaims(t, "metrics_write:revisions"))

	_, err = clientWriteRev.RegisterMetric(ctx,
		&repository.RegisterMetricRequest{
			Uuid:  "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
			Kind:  "wordcount",
			Label: "default",
			Value: 123,
		})
	test.MustNot(t, err, "register the metric")

	// Test reading.

	clientRead := tc.MetricsClient(t,
		itest.StandardClaims(t, "metrics_read"))

	metricRes, err := clientRead.GetMetrics(ctx,
		&repository.GetMetricsRequest{
			Uuids: []string{"d98d2c21-980c-4c7f-b0b5-9ed9feba291b"},
			Kinds: []string{"wordcount"},
		})
	test.Must(t, err, "read metrics")

	test.EqualMessage(t, &repository.GetMetricsResponse{
		Documents: map[string]*repository.DocumentMetrics{
			"d98d2c21-980c-4c7f-b0b5-9ed9feba291b": {
				Metrics: []*repository.Metric{
					{
						Kind:  "wordcount",
						Label: "default",
						Value: 123,
					},
				},
			},
		},
	}, metricRes, "get the registered metrics")

	// Test delete kind.

	_, err = clientAdmin.DeleteKind(ctx, &repository.DeleteMetricKindRequest{
		Name: "wordcount",
	})
	test.Must(t, err, "delete kind in use")
}
