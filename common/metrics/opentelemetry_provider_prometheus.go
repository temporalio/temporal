//go:build !temporal_minimal || temporal_with_prometheus

package metrics

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	exporters "go.opentelemetry.io/otel/exporters/prometheus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

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
		if errors.Is(err, http.ErrServerClosed) {
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
