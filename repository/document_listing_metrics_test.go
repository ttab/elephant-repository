package repository_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

func TestDocumentsServiceMetricsLint(t *testing.T) {
	reg := prometheus.NewRegistry()

	// The constructor only registers metrics, so it needs none of its
	// dependencies.
	_, err := repository.NewDocumentsService(
		nil, nil, nil, nil, nil, "", nil, nil, nil, reg)
	test.Mustf(t, err, "create documents service")

	problems, err := testutil.GatherAndLint(reg)
	test.Mustf(t, err, "gather metrics")

	for _, p := range problems {
		t.Errorf("metric %s: %s", p.Metric, p.Text)
	}
}
