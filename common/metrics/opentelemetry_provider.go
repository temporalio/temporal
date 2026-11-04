//go:build !temporal_minimal || temporal_with_prometheus || temporal_with_statsd

package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ OpenTelemetryProvider = (*openTelemetryProviderImpl)(nil)

type openTelemetryProviderImpl struct {
	meter          metric.Meter
	config         *PrometheusConfig
	server         *http.Server
	statsdExporter *statsdExporter
}

func newOpenTelemetryProvider(
	logger log.Logger,
	reader sdkmetrics.Reader,
	statsdConfig *StatsdConfig,
	statsdExporter *statsdExporter,
	prometheusConfig *PrometheusConfig,
	prometheusServer *http.Server,
	clientConfig *ClientConfig,
) (*openTelemetryProviderImpl, error) {
	var views []sdkmetrics.View
	for _, u := range []string{Dimensionless, Bytes, Milliseconds, Seconds} {
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
		sdkmetrics.WithReader(reader),
		sdkmetrics.WithView(views...),
	)
	meter := provider.Meter("temporal")
	reporter := &openTelemetryProviderImpl{
		meter:          meter,
		config:         prometheusConfig,
		server:         prometheusServer,
		statsdExporter: statsdExporter,
	}

	return reporter, nil
}

func (r *openTelemetryProviderImpl) GetMeter() metric.Meter {
	return r.meter
}

func (r *openTelemetryProviderImpl) Stop(logger log.Logger) {
	// Shutdown Prometheus server if it exists
	if r.server != nil {
		ctx, closeCtx := context.WithTimeout(context.Background(), time.Second)
		defer closeCtx()
		if err := r.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Prometheus metrics server shutdown failure.", tag.Address(r.config.ListenAddress), tag.Error(err))
		}
	}

	// Shutdown StatsD exporter if it exists
	if r.statsdExporter != nil {
		ctx, closeCtx := context.WithTimeout(context.Background(), time.Second)
		defer closeCtx()
		if err := r.statsdExporter.Shutdown(ctx); err != nil {
			logger.Error("StatsD exporter shutdown failure.", tag.Error(err))
		}
	}
}
