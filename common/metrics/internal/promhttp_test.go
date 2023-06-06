package internal_test

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/internal"
)

func TestPrintMetricsPage(t *testing.T) {
	t.Parallel()

	r := prometheus.NewRegistry()
	err := r.Register(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_counter",
		Help: "test help text",
	}))
	require.NoError(t, err)
	var b bytes.Buffer
	err = internal.PrintMetricsPage(r, &b)
	require.NoError(t, err)
	require.Contains(t, b.String(), "# HELP test_counter test help text")
}
