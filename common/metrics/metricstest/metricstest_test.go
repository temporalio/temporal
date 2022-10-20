package metricstest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/log"
	ossmetrics "go.temporal.io/server/common/metrics"
)

func TestBasic(t *testing.T) {
	logger := log.NewTestLogger()
	handler := MustNewHandler(logger)

	counterName := "counter1"
	counterTags := []ossmetrics.Tag{
		ossmetrics.StringTag("l2", "v2"),
		ossmetrics.StringTag("l1", "v1"),
	}
	counter := handler.WithTags(counterTags...).Counter(counterName)
	counter.Record(1)
	counter.Record(1)

	s1 := handler.MustSnapshot()
	require.Equal(t, float64(2), s1.MustCounter(counterName, counterTags...))

	gaugeName := "gauge1"
	gaugeTags := []ossmetrics.Tag{
		ossmetrics.StringTag("l3", "v3"),
		ossmetrics.StringTag("l4", "v4"),
	}
	gauge := handler.WithTags(gaugeTags...).Gauge(gaugeName)
	gauge.Record(-2)
	gauge.Record(10)

	s2 := handler.MustSnapshot()
	require.Equal(t, float64(2), s2.MustCounter(counterName, counterTags...))
	require.Equal(t, float64(10), s2.MustGauge(gaugeName, gaugeTags...))
}
