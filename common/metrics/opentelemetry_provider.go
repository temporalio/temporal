package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	exporters "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ OpenTelemetryProvider = (*openTelemetryProviderImpl)(nil)

type (
	OpenTelemetryProvider interface {
		Stop(logger log.Logger)
		GetMeter() metric.Meter
	}

	openTelemetryProviderImpl struct {
		meter          metric.Meter
		config         *PrometheusConfig
		server         *http.Server
		statsdExporter *statsdExporter
	}
)

// NewOpenTelemetryProviderWithStatsd creates a new OpenTelemetry provider with a StatsD exporter.
func NewOpenTelemetryProviderWithStatsd(
	logger log.Logger,
	statsdConfig *StatsdConfig,
	clientConfig *ClientConfig,
) (*openTelemetryProviderImpl, error) {
	// Set up StatsD exporter if config is provided
	if statsdConfig == nil {
		return nil, errors.New("statsd config is required to provide statsd metrics")
	}
	var err error
	statsdExp, err := NewStatsdExporter(statsdConfig, logger)
	if err != nil {
		logger.Error("Failed to initialize statsd exporter.", tag.Error(err))
		return nil, err
	}
	// Create a PeriodicReader with the StatsD exporter
	statsdReader := sdkmetrics.NewPeriodicReader(statsdExp)
	return newOpenTelemetryProvider(logger, statsdReader, statsdConfig, statsdExp, nil, nil, clientConfig)
}

// NewOpenTelemetryProviderWithPrometheus creates a new OpenTelemetry provider with a Prometheus exporter.
func NewOpenTelemetryProviderWithPrometheus(
	logger log.Logger,
	prometheusConfig *PrometheusConfig,
	clientConfig *ClientConfig,
	fatalOnListenerError bool,
) (*openTelemetryProviderImpl, error) {
	// Set up Prometheus exporter if config is provided
	if prometheusConfig == nil {
		return nil, errors.New("prometheus config is required to provide prometheus metrics")
	}
	reg := prometheus.NewRegistry()
	exporterOpts := []exporters.Option{exporters.WithRegisterer(reg)}
	if clientConfig.WithoutUnitSuffix {
		exporterOpts = append(exporterOpts, exporters.WithoutUnits())
	}
	if clientConfig.WithoutCounterSuffix {
		exporterOpts = append(exporterOpts, exporters.WithoutCounterSuffixes())
	}
	if clientConfig.Prefix != "" {
		exporterOpts = append(exporterOpts, exporters.WithNamespace(clientConfig.Prefix))
	}
	exporter, err := exporters.New(exporterOpts...)
	if err != nil {
		logger.Error("Failed to initialize prometheus exporter.", tag.Error(err))
		return nil, err
	}
	metricServer := initPrometheusListener(prometheusConfig, reg, logger, fatalOnListenerError)
	return newOpenTelemetryProvider(logger, exporter, nil, nil, prometheusConfig, metricServer, clientConfig)
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

func initPrometheusListener(
	config *PrometheusConfig,
	reg *prometheus.Registry,
	logger log.Logger,
	fatalOnListenerError bool,
) *http.Server {
	handlerPath := config.HandlerPath
	if handlerPath == "" {
		handlerPath = "/metrics"
	}

	handler := http.NewServeMux()
	handler.HandleFunc(handlerPath, promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}).ServeHTTP)

	if config.ListenAddress == "" {
		logger.Fatal("Listen address must be specified.", tag.Address(config.ListenAddress))
	}
	server := &http.Server{Addr: config.ListenAddress, Handler: handler}

	go func() {
		err := server.ListenAndServe()
		if err == http.ErrServerClosed {
			return
		}
		msg := "Failed to initialize prometheus listener."
		logger := log.With(logger, tag.Error(err), tag.Address(config.ListenAddress))
		if fatalOnListenerError {
			logger.Fatal(msg)
		} else {
			// For backward compatibility, we log as Warn instead of Error/Fatal
			// to match the behavior of tally framework.
			logger.Warn(msg)
		}
	}()

	return server
}

func (r *openTelemetryProviderImpl) GetMeter() metric.Meter {
	return r.meter
}

func (r *openTelemetryProviderImpl) Stop(logger log.Logger) {
	// Shutdown Prometheus server if it exists
	if r.server != nil {
		ctx, closeCtx := context.WithTimeout(context.Background(), time.Second)
		defer closeCtx()
		if err := r.server.Shutdown(ctx); !(err == nil || err == http.ErrServerClosed) {
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
