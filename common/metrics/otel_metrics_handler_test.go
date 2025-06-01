package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/mock/gomock"
)

var (
	minLatency = float64(1248)
	maxLatency = float64(5255)
	testBytes  = float64(1234567)
)

type testProvider struct {
	meter metric.Meter
}

func (t *testProvider) GetMeter() metric.Meter {
	return t.meter
}

func (t *testProvider) Stop(log.Logger) {}

func TestMeter(t *testing.T) {
	ctx := context.Background()
	rdr := sdkmetrics.NewManualReader()
	provider := sdkmetrics.NewMeterProvider(
		sdkmetrics.WithReader(rdr),
		sdkmetrics.WithView(
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindHistogram,
					Unit: Bytes,
				},
				sdkmetrics.Stream{
					Aggregation: sdkmetrics.AggregationExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[Bytes],
					},
				},
			),
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindHistogram,
					Unit: Dimensionless,
				},
				sdkmetrics.Stream{
					Aggregation: sdkmetrics.AggregationExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[Dimensionless],
					},
				},
			),
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindHistogram,
					Unit: Milliseconds,
				},
				sdkmetrics.Stream{
					Aggregation: sdkmetrics.AggregationExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[Milliseconds],
					},
				},
			),
		),
	)

	p, err := NewOtelMetricsHandler(
		log.NewTestLogger(),
		&testProvider{meter: provider.Meter("test")},
		defaultConfig,
		false,
	)
	require.NoError(t, err)
	recordMetrics(p)

	var got metricdata.ResourceMetrics
	err = rdr.Collect(ctx, &got)
	assert.Nil(t, err)

	want := []metricdata.Metrics{
		{
			Name: "hits",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value:     8,
						Exemplars: []metricdata.Exemplar[int64]{},
					},
				},
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
			},
		},
		{
			Name: "hits-tagged",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Attributes: attribute.NewSet(attribute.String("taskqueue", "__sticky__")),
						Value:      11,
						Exemplars:  []metricdata.Exemplar[int64]{},
					},
				},
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
			},
		},
		{
			Name: "hits-tagged-excluded",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{

						Attributes: attribute.NewSet(attribute.String("taskqueue", tagExcludedValue)),
						Value:      14,
						Exemplars:  []metricdata.Exemplar[int64]{},
					},
				},
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
			},
		},
		{
			Name: "latency",
			Data: metricdata.Histogram[int64]{
				DataPoints: []metricdata.HistogramDataPoint[int64]{
					{
						Count:        2,
						BucketCounts: []uint64{0, 0, 0, 1, 1, 0},
						Min:          metricdata.NewExtrema[int64](int64(minLatency)),
						Max:          metricdata.NewExtrema[int64](int64(maxLatency)),
						Sum:          6503,
						Exemplars:    []metricdata.Exemplar[int64]{},
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
			Unit: Milliseconds,
		},
		{
			Name: "temp",
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{
						Attributes: attribute.NewSet(attribute.String("location", "Mare Imbrium")),
						Value:      100,
						Exemplars:  []metricdata.Exemplar[float64]{},
					},
				},
			},
		},
		{
			Name: "transmission",
			Data: metricdata.Histogram[int64]{
				DataPoints: []metricdata.HistogramDataPoint[int64]{
					{
						Count:        1,
						BucketCounts: []uint64{0, 0, 1},
						Min:          metricdata.NewExtrema[int64](int64(testBytes)),
						Max:          metricdata.NewExtrema[int64](int64(testBytes)),
						Sum:          int64(testBytes),
						Exemplars:    []metricdata.Exemplar[int64]{},
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
			Unit: Bytes,
		},
	}
	if diff := cmp.Diff(want, got.ScopeMetrics[0].Metrics,
		cmp.Comparer(func(e1, e2 metricdata.Extrema[int64]) bool {
			v1, ok1 := e1.Value()
			v2, ok2 := e2.Value()
			return ok1 && ok2 && v1 == v2
		}),
		cmp.Comparer(func(a1, a2 attribute.Set) bool {
			return a1.Equals(&a2)
		}),
		cmpopts.SortSlices(func(x, y metricdata.Metrics) bool {
			return x.Name < y.Name
		}),
		cmpopts.IgnoreFields(metricdata.DataPoint[int64]{}, "StartTime", "Time"),
		cmpopts.IgnoreFields(metricdata.DataPoint[float64]{}, "StartTime", "Time"),
		cmpopts.IgnoreFields(metricdata.HistogramDataPoint[int64]{}, "StartTime", "Time", "Bounds"),
	); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}
}

func TestMeter_TimerInSeconds(t *testing.T) {
	ctx := context.Background()
	rdr := sdkmetrics.NewManualReader()
	provider := sdkmetrics.NewMeterProvider(
		sdkmetrics.WithReader(rdr),
		sdkmetrics.WithView(
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindHistogram,
					Unit: Seconds,
				},
				sdkmetrics.Stream{
					Aggregation: sdkmetrics.AggregationExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[Seconds],
					},
				},
			),
		),
	)

	shouldRecordTimerInSeconds := true
	p, err := NewOtelMetricsHandler(
		log.NewTestLogger(),
		&testProvider{meter: provider.Meter("test")},
		defaultConfig,
		shouldRecordTimerInSeconds,
	)
	require.NoError(t, err)
	recordTimer(p)

	var got metricdata.ResourceMetrics
	err = rdr.Collect(ctx, &got)
	assert.Nil(t, err)

	want := []metricdata.Metrics{
		{
			Name: "latency",
			Data: metricdata.Histogram[float64]{
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{
						Count:        2,
						BucketCounts: []uint64{0, 0, 0, 1, 1, 0},
						Min:          metricdata.NewExtrema[float64](float64(minLatency) / 1000),
						Max:          metricdata.NewExtrema[float64](float64(maxLatency) / 1000),
						Sum:          (minLatency + maxLatency) / 1000,
						Exemplars:    []metricdata.Exemplar[float64]{},
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
			Unit: Seconds,
		},
	}
	if diff := cmp.Diff(want, got.ScopeMetrics[0].Metrics,
		cmp.Comparer(func(e1, e2 metricdata.Extrema[float64]) bool {
			v1, ok1 := e1.Value()
			v2, ok2 := e2.Value()
			return ok1 && ok2 && v1 == v2
		}),
		cmp.Comparer(func(a1, a2 attribute.Set) bool {
			return a1.Equals(&a2)
		}),
		cmpopts.IgnoreFields(metricdata.HistogramDataPoint[float64]{}, "StartTime", "Time", "Bounds"),
	); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}
}

func recordMetrics(mp Handler) {
	hitsCounter := mp.Counter("hits")
	gauge := mp.Gauge("temp")
	histogram := mp.Histogram("transmission", Bytes)
	hitsTaggedCounter := mp.Counter("hits-tagged")
	hitsTaggedExcludedCounter := mp.Counter("hits-tagged-excluded")

	hitsCounter.Record(8)
	gauge.Record(100, StringTag("location", "Mare Imbrium"))
	histogram.Record(int64(testBytes))
	hitsTaggedCounter.Record(11, UnsafeTaskQueueTag("__sticky__"))
	hitsTaggedExcludedCounter.Record(14, UnsafeTaskQueueTag("filtered"))

	recordTimer(mp)
}

func recordTimer(mp Handler) {
	timer := mp.Timer("latency")
	timer.Record(time.Duration(minLatency) * time.Millisecond)
	timer.Record(time.Duration(maxLatency) * time.Millisecond)
}

type erroneousMeter struct {
	metric.Meter
	err error
}

func (t erroneousMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, t.err
}

func (t erroneousMeter) Int64Histogram(string, ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	return nil, t.err
}

func (t erroneousMeter) Float64ObservableGauge(string, ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	return nil, t.err
}

var testErr = errors.New("test error")

func TestOtelMetricsHandler_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	logger := log.NewMockLogger(ctrl)
	meter := erroneousMeter{err: testErr}
	provider := &testProvider{meter: meter}
	cfg := ClientConfig{}
	handler, err := NewOtelMetricsHandler(logger, provider, cfg, false)
	require.NoError(t, err)
	msg := "error getting metric"
	errTag := tag.Error(testErr)

	logger.EXPECT().Error(msg, tag.NewStringTag("MetricName", "counter"), errTag)
	handler.Counter("counter").Record(1)
	logger.EXPECT().Error(msg, tag.NewStringTag("MetricName", "timer"), errTag)
	handler.Timer("timer").Record(time.Second)
	logger.EXPECT().Error(msg, tag.NewStringTag("MetricName", "gauge"), errTag)
	handler.Gauge("gauge").Record(1.0)
	logger.EXPECT().Error(msg, tag.NewStringTag("MetricName", "histogram"), errTag)
	handler.Histogram("histogram", Bytes).Record(1)
}
