package metrics

import (
	"context"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/log"
)

func TestOpenTelemetryProviderExemplarPolicy(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXEMPLAR_FILTER", "always_on")
	reader := sdkmetrics.NewManualReader()
	config := openTelemetryOptimizationConfig()
	provider, err := newOpenTelemetryProvider(log.NewNoopLogger(), reader, nil, nil, nil, nil, &config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Shutdown(context.Background())) })

	counter, err := provider.GetMeter().Int64Counter("exemplar_policy_counter")
	require.NoError(t, err)
	histogram, err := provider.GetMeter().Int64Histogram("exemplar_policy_histogram", metric.WithUnit(Milliseconds))
	require.NoError(t, err)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{1},
		TraceFlags: trace.FlagsSampled,
	}))
	attributes := attribute.NewSet(attribute.String("namespace", "test"))
	option := metric.WithAttributeSet(attributes)
	counter.Add(ctx, 3, option)
	histogram.Record(ctx, 3, option)
	histogram.Record(ctx, 9, option)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))
	require.Len(t, collected.ScopeMetrics, 1)
	require.Len(t, collected.ScopeMetrics[0].Metrics, 2)
	for _, got := range collected.ScopeMetrics[0].Metrics {
		switch got.Name {
		case "exemplar_policy_counter":
			sum, ok := got.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			require.Equal(t, int64(3), sum.DataPoints[0].Value)
			require.True(t, sum.DataPoints[0].Attributes.Equals(&attributes))
			require.Empty(t, sum.DataPoints[0].Exemplars)
		case "exemplar_policy_histogram":
			hist, ok := got.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			require.Len(t, hist.DataPoints, 1)
			point := hist.DataPoints[0]
			require.Equal(t, uint64(2), point.Count)
			require.Equal(t, int64(12), point.Sum)
			require.Equal(t, []float64{1, 5, 10, 25}, point.Bounds)
			require.Equal(t, []uint64{0, 1, 1, 0, 0}, point.BucketCounts)
			require.True(t, point.Attributes.Equals(&attributes))
			require.Empty(t, point.Exemplars)
		default:
			t.Fatalf("unexpected metric %q", got.Name)
		}
	}
}

func BenchmarkOpenTelemetryProviderCollect(b *testing.B) {
	b.Setenv("OTEL_GO_X_CARDINALITY_LIMIT", "2000")
	for _, kind := range []string{"counter", "histogram", "mixed"} {
		// Keep each instrument below the SDK's default cardinality limit.
		for _, series := range []int{100, 1000} {
			b.Run(fmt.Sprintf("%s/%d_per_instrument", kind, series), func(b *testing.B) {
				state := populateOpenTelemetryBenchmark(b, kind, series)
				b.Cleanup(func() { _ = state.reader.Shutdown(context.Background()) })
				var collected metricdata.ResourceMetrics
				if err := state.reader.Collect(context.Background(), &collected); err != nil {
					b.Fatal(err)
				}
				checkOpenTelemetryBenchmarkSeries(b, collected, kind, series)
				collected = metricdata.ResourceMetrics{}
				runtime.GC()
				b.ReportAllocs()
				b.ResetTimer()
				cpuBefore := openTelemetryBenchmarkCPUTime(b)
				for b.Loop() {
					collected = metricdata.ResourceMetrics{}
					if err := state.reader.Collect(context.Background(), &collected); err != nil {
						b.Fatal(err)
					}
				}
				cpuAfter := openTelemetryBenchmarkCPUTime(b)
				b.ReportMetric(float64(cpuAfter-cpuBefore)/float64(b.N), "cpu-ns/op")
				runtime.KeepAlive(state)
			})
		}
	}
}

// BenchmarkOpenTelemetryProviderRetained reports live heap after an explicit GC.
// Run with -benchtime=1x so each result represents one populated provider.
func BenchmarkOpenTelemetryProviderRetained(b *testing.B) {
	b.Setenv("OTEL_GO_X_CARDINALITY_LIMIT", "2000")
	const series = 1000
	for _, kind := range []string{"counter", "histogram", "mixed"} {
		b.Run(fmt.Sprintf("%s/%d_per_instrument", kind, series), func(b *testing.B) {
			var retainedBytes int64
			var cpuTime int64
			b.ReportAllocs()
			b.StopTimer()
			for range b.N {
				openTelemetryBenchmarkKeepAlive = nil
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				b.StartTimer()
				cpuBefore := openTelemetryBenchmarkCPUTime(b)
				state := populateOpenTelemetryBenchmark(b, kind, series)
				cpuTime += openTelemetryBenchmarkCPUTime(b) - cpuBefore
				b.StopTimer()
				openTelemetryBenchmarkKeepAlive = state
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				retainedBytes += int64(after.HeapAlloc) - int64(before.HeapAlloc)
				runtime.KeepAlive(state)
			}
			b.ReportMetric(float64(cpuTime)/float64(b.N), "cpu-ns/op")
			metricSeries := series
			if kind == "mixed" {
				metricSeries *= 2
			}
			b.ReportMetric(float64(retainedBytes)/float64(b.N*metricSeries), "retained-B/series")
		})
	}
}

func checkOpenTelemetryBenchmarkSeries(b *testing.B, collected metricdata.ResourceMetrics, kind string, series int) {
	b.Helper()
	metrics := 0
	points := 0
	for _, scope := range collected.ScopeMetrics {
		for _, collectedMetric := range scope.Metrics {
			metrics++
			switch data := collectedMetric.Data.(type) {
			case metricdata.Sum[int64]:
				points += len(data.DataPoints)
			case metricdata.Histogram[int64]:
				points += len(data.DataPoints)
			default:
				b.Fatalf("unexpected metric data type %T", data)
			}
		}
	}
	wantMetrics := 1
	if kind == "mixed" {
		wantMetrics = 2
	}
	if metrics != wantMetrics || points != wantMetrics*series {
		b.Fatalf("collected %d metrics and %d series; want %d metrics and %d series", metrics, points, wantMetrics, wantMetrics*series)
	}
}

type openTelemetryBenchmarkState struct {
	reader   *sdkmetrics.ManualReader
	provider *openTelemetryProviderImpl
}

var openTelemetryBenchmarkKeepAlive *openTelemetryBenchmarkState

func openTelemetryBenchmarkCPUTime(b *testing.B) int64 {
	b.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return usage.Utime.Nano() + usage.Stime.Nano()
}

func populateOpenTelemetryBenchmark(b *testing.B, kind string, series int) *openTelemetryBenchmarkState {
	b.Helper()
	reader := sdkmetrics.NewManualReader()
	config := openTelemetryOptimizationConfig()
	provider, err := newOpenTelemetryProvider(log.NewNoopLogger(), reader, nil, nil, nil, nil, &config)
	if err != nil {
		b.Fatal(err)
	}
	handler, err := NewOtelMetricsHandler(log.NewNoopLogger(), provider, config, false)
	if err != nil {
		b.Fatal(err)
	}
	counter := handler.Counter("exemplar_benchmark_counter")
	timer := handler.Timer("exemplar_benchmark_timer")
	for i := range series {
		tags := []Tag{NamespaceTag(fmt.Sprintf("namespace-%d", i)), OperationTag("benchmark")}
		if kind != "histogram" {
			counter.Record(1, tags...)
		}
		if kind != "counter" {
			timer.Record(9*time.Millisecond, tags...)
		}
	}
	return &openTelemetryBenchmarkState{reader: reader, provider: provider}
}

func openTelemetryOptimizationConfig() ClientConfig {
	return ClientConfig{PerUnitHistogramBoundaries: map[string][]float64{
		Dimensionless: {1, 5, 10, 25},
		Bytes:         {1, 5, 10, 25},
		Milliseconds:  {1, 5, 10, 25},
		Seconds:       {1, 5, 10, 25},
	}}
}
