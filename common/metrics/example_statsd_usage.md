# StatsD Support for OpenTelemetry Provider

This document shows how to use the new StatsD support in the Temporal OpenTelemetry metrics provider.

## Configuration

To enable StatsD support, you need to provide a `StatsdConfig` when creating the OpenTelemetry provider:

```go
package main

import (
    "context"
    "log"

    "go.temporal.io/server/common/metrics"
    "go.temporal.io/server/common/log"
)

func main() {
    logger := log.NewNoopLogger()

    // Configure StatsD
    statsdConfig := &metrics.StatsdConfig{
        HostPort: "localhost:8125",  // StatsD server address
        Prefix:   "temporal",        // Metric prefix
        Reporter: metrics.StatsdReporterConfig{
            TagSeparator: ",",       // Tag separator for StatsD tags
        },
    }

    // Configure client settings
    clientConfig := &metrics.ClientConfig{
        PerUnitHistogramBoundaries: map[string][]float64{
            metrics.Dimensionless: {1, 5, 10, 25, 50, 100},
            metrics.Bytes:         {1024, 4096, 16384, 65536},
            metrics.Milliseconds:  {1, 5, 10, 25, 50, 100, 250, 500, 1000},
            metrics.Seconds:       {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
        },
    }

    // Create OpenTelemetry provider with StatsD support
    provider, err := metrics.NewOpenTelemetryProvider(
        logger,
        nil,           // No Prometheus config
        clientConfig,
        false,         // Don't fatal on listener error
        statsdConfig,  // StatsD config
    )
    if err != nil {
        log.Fatal("Failed to create OpenTelemetry provider:", err)
    }
    defer provider.Stop(logger)

    // Get a meter and create metrics
    meter := provider.GetMeter()

    // Create a counter
    counter, err := meter.Int64Counter("requests_total")
    if err != nil {
        log.Fatal("Failed to create counter:", err)
    }

    // Create a histogram
    histogram, err := meter.Float64Histogram("request_duration")
    if err != nil {
        log.Fatal("Failed to create histogram:", err)
    }

    // Use the metrics
    counter.Add(context.Background(), 1)
    histogram.Record(context.Background(), 0.123)

    // Metrics will be sent to StatsD automatically via the PeriodicReader
}
```

## Using Both Prometheus and StatsD

You can also configure both Prometheus and StatsD exporters simultaneously:

```go
// Configure both Prometheus and StatsD
prometheusConfig := &metrics.PrometheusConfig{
    ListenAddress: "127.0.0.1:9090",
    HandlerPath:   "/metrics",
}

statsdConfig := &metrics.StatsdConfig{
    HostPort: "localhost:8125",
    Prefix:   "temporal",
    Reporter: metrics.StatsdReporterConfig{
        TagSeparator: ",",
    },
}

// Create provider with both exporters
provider, err := metrics.NewOpenTelemetryProvider(
    logger,
    prometheusConfig,  // Prometheus config
    clientConfig,
    false,
    statsdConfig,      // StatsD config
)
```

## Metric Types Supported

The StatsD exporter supports the following OpenTelemetry metric types:

### Counters (Sum metrics)
- **Monotonic sums**: Exported as StatsD counters using `Inc()`
- **Non-monotonic sums**: Exported as StatsD gauges using `Gauge()`

### Gauges
- Exported as StatsD gauges using `Gauge()`

### Histograms
- Exported as multiple StatsD metrics:
  - `{metric_name}.count`: Total count of observations
  - `{metric_name}.sum`: Sum of all observed values
  - `{metric_name}.bucket_le_{bound}`: Count of observations in each bucket

## Configuration Options

### StatsdConfig Fields

- `HostPort`: The address of the StatsD server (e.g., "localhost:8125")
- `Prefix`: A prefix to add to all metric names
- `Reporter.TagSeparator`: The separator to use for StatsD tags (default: ",")

### Notes

- The StatsD exporter uses the `github.com/cactus/go-statsd-client/v5` library
- Metrics are sent via UDP, so there's no guarantee of delivery
- The exporter automatically converts OpenTelemetry attributes to StatsD tags
- Float64 values are converted to int64 for StatsD compatibility where necessary
- The exporter uses a PeriodicReader to automatically send metrics at regular intervals

## Error Handling

The StatsD exporter logs errors but doesn't fail the application if StatsD is unavailable. This ensures that your application continues to work even if the StatsD server is down.
