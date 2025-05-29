package metrics

import (
	"context"
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

func NewOpenTelemetryProvider(
	logger log.Logger,
	prometheusConfig *PrometheusConfig,
	clientConfig *ClientConfig,
	fatalOnListenerError bool,
	statsdConfig *StatsdConfig,
) (*openTelemetryProviderImpl, error) {
	var readers []sdkmetrics.Reader
	var metricServer *http.Server
	var statsdExp *statsdExporter

	// Set up Prometheus exporter if config is provided
	if prometheusConfig != nil {
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
		readers = append(readers, exporter)
		metricServer = initPrometheusListener(prometheusConfig, reg, logger, fatalOnListenerError)
	}

	// Set up StatsD exporter if config is provided
	if statsdConfig != nil {
		var err error
		statsdExp, err = NewStatsdExporter(statsdConfig, logger)
		if err != nil {
			logger.Error("Failed to initialize statsd exporter.", tag.Error(err))
			return nil, err
		}
		// Create a PeriodicReader with the StatsD exporter
		statsdReader := sdkmetrics.NewPeriodicReader(statsdExp)
		readers = append(readers, statsdReader)
	}

	// If no exporters are configured, log a warning
	if len(readers) == 0 {
		logger.Warn("No metric exporters configured (neither Prometheus nor StatsD)")
	}

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

	meterProviderOpts := []sdkmetrics.Option{sdkmetrics.WithView(views...)}
	for _, reader := range readers {
		meterProviderOpts = append(meterProviderOpts, sdkmetrics.WithReader(reader))
	}

	provider := sdkmetrics.NewMeterProvider(meterProviderOpts...)
	meter := provider.Meter("temporal")

	reporter := &openTelemetryProviderImpl{
		meter:          meter,
		config:         prometheusConfig,
		server:         metricServer,
		statsdExporter: statsdExp,
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
