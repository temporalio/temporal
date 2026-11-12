package metricstest

import (
	"github.com/prometheus/client_golang/prometheus"
	exporters "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	testprometheus "go.temporal.io/testx/metricstest/prometheus"
)

type Handler struct {
	metrics.Handler
	reg *prometheus.Registry
}

type Snapshot struct {
	testprometheus.Snapshot
}

func NewHandler(logger log.Logger, clientConfig metrics.ClientConfig) (*Handler, error) {
	registry := prometheus.NewRegistry()
	exporter, err := exporters.New(exporters.WithRegisterer(registry))
	if err != nil {
		return nil, err
	}

	// Set any custom histogram bucket configuration.
	var views []sdkmetrics.View
	for _, u := range []string{metrics.Dimensionless, metrics.Bytes, metrics.Milliseconds} {
		views = append(views, sdkmetrics.NewView(
			sdkmetrics.Instrument{
				Kind: sdkmetrics.InstrumentKindHistogram,
				Unit: u,
			},
			sdkmetrics.Stream{
				Aggregation: sdkmetrics.AggregationExplicitBucketHistogram{
					Boundaries: clientConfig.PerUnitHistogramBoundaries[u],
				},
			},
		))
	}
	provider := sdkmetrics.NewMeterProvider(
		sdkmetrics.WithReader(exporter),
		sdkmetrics.WithView(views...),
	)
	meter := provider.Meter("temporal")

	otelHandler, err := metrics.NewOtelMetricsHandler(logger, &otelProvider{meter: meter}, clientConfig, false)
	if err != nil {
		return nil, err
	}
	metricsHandler := &Handler{
		Handler: otelHandler,
		reg:     registry,
	}

	return metricsHandler, nil
}

func (*Handler) Stop(log.Logger) {}

func (h *Handler) Snapshot() (Snapshot, error) {
	snapshot, err := testprometheus.Gather(h.reg)
	return Snapshot{Snapshot: snapshot}, err
}

var _ metrics.OpenTelemetryProvider = (*otelProvider)(nil)

type otelProvider struct {
	meter metric.Meter
}

func (m *otelProvider) GetMeter() metric.Meter {
	return m.meter
}

func (m *otelProvider) Stop(log.Logger) {}

func (s Snapshot) Counter(name string, tags ...metrics.Tag) (float64, error) {
	return s.Snapshot.Counter(name, metricLabels(tags))
}

func (s Snapshot) Gauge(name string, tags ...metrics.Tag) (float64, error) {
	return s.Snapshot.Gauge(name, metricLabels(tags))
}

func (s Snapshot) Histogram(name string, tags ...metrics.Tag) ([]HistogramBucket, error) {
	return s.Snapshot.Histogram(name, metricLabels(tags))
}

func metricLabels(tags []metrics.Tag) map[string]string {
	labels := make(map[string]string, len(tags))
	for _, tag := range tags {
		labels[tag.Key] = tag.Value
	}
	return labels
}
