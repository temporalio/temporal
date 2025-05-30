package metrics

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ metric.Exporter = (*statsdExporter)(nil)

type statsdExporter struct {
	client       statsd.Statter
	config       *StatsdConfig
	logger       log.Logger
	tagSeparator string
	mu           sync.Mutex
	shutdown     bool
}

// NewStatsdExporter creates a new StatsD exporter that implements the OpenTelemetry metric.Exporter interface
func NewStatsdExporter(config *StatsdConfig, logger log.Logger) (*statsdExporter, error) {
	if config == nil {
		return nil, errors.New("StatsdConfig cannot be nil")
	}

	// Create StatsD client
	clientConfig := &statsd.ClientConfig{
		Address: config.HostPort,
		Prefix:  config.Prefix,
	}

	client, err := statsd.NewClientWithConfig(clientConfig)
	if err != nil {
		return nil, errors.New("failed to create StatsD client")
	}

	return &statsdExporter{
		client:       client,
		config:       config,
		logger:       logger,
		tagSeparator: config.Reporter.TagSeparator,
	}, nil
}

// Temporality implements metric.Exporter
func (e *statsdExporter) Temporality(ik metric.InstrumentKind) metricdata.Temporality {
	// StatsD typically uses cumulative temporality for counters and delta for gauges
	switch ik {
	case metric.InstrumentKindCounter, metric.InstrumentKindObservableCounter:
		return metricdata.CumulativeTemporality
	case metric.InstrumentKindHistogram:
		return metricdata.CumulativeTemporality
	default:
		return metricdata.DeltaTemporality
	}
}

// Aggregation implements metric.Exporter
func (e *statsdExporter) Aggregation(ik metric.InstrumentKind) metric.Aggregation {
	// For StatsD, we use default aggregations since StatsD handles aggregation on the server side
	return metric.DefaultAggregationSelector(ik)
}

// Export implements metric.Exporter
func (e *statsdExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.shutdown {
		return errors.New("exporter is shutdown")
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if err := e.exportMetric(m); err != nil {
				e.logger.Error("Failed to export metric to StatsD", tag.Error(err), tag.NewStringTag("metric_name", m.Name))
			}
		}
	}

	return nil
}

// ForceFlush implements metric.Exporter
func (e *statsdExporter) ForceFlush(ctx context.Context) error {
	// StatsD is UDP-based and doesn't support flushing
	return nil
}

// Shutdown implements metric.Exporter
func (e *statsdExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.shutdown {
		return nil
	}

	e.shutdown = true
	return e.client.Close()
}

func (e *statsdExporter) exportMetric(m metricdata.Metrics) error {
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		return e.exportSumInt64(m.Name, data)
	case metricdata.Sum[float64]:
		return e.exportSumFloat64(m.Name, data)
	case metricdata.Gauge[int64]:
		return e.exportGaugeInt64(m.Name, data)
	case metricdata.Gauge[float64]:
		return e.exportGaugeFloat64(m.Name, data)
	case metricdata.Histogram[int64]:
		return e.exportHistogramInt64(m.Name, data)
	case metricdata.Histogram[float64]:
		return e.exportHistogramFloat64(m.Name, data)
	default:
		e.logger.Warn("Unsupported metric type for StatsD export", tag.NewStringTag("metric_name", m.Name))
		return nil
	}
}

func (e *statsdExporter) exportSumInt64(name string, data metricdata.Sum[int64]) error {
	for _, dp := range data.DataPoints {
		metricName := e.buildMetricName(name, dp.Attributes)
		if data.IsMonotonic {
			// For monotonic sums (counters), use Inc
			if err := e.client.Inc(metricName, dp.Value, 1.0); err != nil {
				return err
			}
		} else {
			// For non-monotonic sums, use Gauge
			if err := e.client.Gauge(metricName, dp.Value, 1.0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *statsdExporter) exportSumFloat64(name string, data metricdata.Sum[float64]) error {
	for _, dp := range data.DataPoints {
		metricName := e.buildMetricName(name, dp.Attributes)
		if data.IsMonotonic {
			// For monotonic sums (counters), use Inc with converted value
			if err := e.client.Inc(metricName, int64(dp.Value), 1.0); err != nil {
				return err
			}
		} else {
			// For non-monotonic sums, convert to int64 and use Gauge
			if err := e.client.Gauge(metricName, int64(dp.Value), 1.0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *statsdExporter) exportGaugeInt64(name string, data metricdata.Gauge[int64]) error {
	for _, dp := range data.DataPoints {
		metricName := e.buildMetricName(name, dp.Attributes)
		if err := e.client.Gauge(metricName, dp.Value, 1.0); err != nil {
			return err
		}
	}
	return nil
}

func (e *statsdExporter) exportGaugeFloat64(name string, data metricdata.Gauge[float64]) error {
	for _, dp := range data.DataPoints {
		metricName := e.buildMetricName(name, dp.Attributes)
		// Convert float64 to int64 for StatsD gauge
		if err := e.client.Gauge(metricName, int64(dp.Value), 1.0); err != nil {
			return err
		}
	}
	return nil
}

func (e *statsdExporter) exportHistogramInt64(name string, data metricdata.Histogram[int64]) error {
	for _, dp := range data.DataPoints {
		metricName := e.buildMetricName(name, dp.Attributes)

		// Export histogram as multiple metrics
		// Count
		if err := e.client.Inc(metricName+".count", int64(dp.Count), 1.0); err != nil {
			return err
		}

		// Sum (dp.Sum is just an int64, not a pointer or optional type)
		if err := e.client.Gauge(metricName+".sum", dp.Sum, 1.0); err != nil {
			return err
		}
	}
	return nil
}

func (e *statsdExporter) exportHistogramFloat64(name string, data metricdata.Histogram[float64]) error {
	for _, dp := range data.DataPoints {
		metricName := e.buildMetricName(name, dp.Attributes)

		// Export histogram as multiple metrics
		// Count
		if err := e.client.Inc(metricName+".count", int64(dp.Count), 1.0); err != nil {
			return err
		}

		// Sum (dp.Sum is just a float64, not a pointer or optional type)
		if err := e.client.Gauge(metricName+".sum", int64(dp.Sum), 1.0); err != nil {
			return err
		}
	}
	return nil
}

func (e *statsdExporter) buildMetricName(name string, attrs attribute.Set) string {
	if attrs.Len() == 0 {
		return name
	}
	tags := e.buildTags(attrs)
	// if a tag separator is provided, we need to emit the tags separately.
	if e.tagSeparator != "" {
		return appendSeparatedTags(name, e.tagSeparator, tags)
	}

	// if no tag separator is provided, we need to embed the tags in the metric name.
	return embedTags(name, tags)
}

// embedTags adds the sorted list of tags directly in the stat name.
// For example, if the stat is `hello.world` and the tags are `{universe: milkyWay, planet: earth}`,
// the stat will be emitted as `hello.world.planet.earth.universe.milkyWay`.
func embedTags(name string, tags []statsd.Tag) string {
	var buffer strings.Builder
	buffer.WriteString(name)
	for _, tg := range tags {
		// adding "." as delimiter so that it will show as different parts in Graphite/Grafana
		buffer.WriteString("." + tg[0] + "." + tg[1])
	}

	return buffer.String()
}

// appendSeparatedTags adds the sorted list of tags using the DogStatsd/InfluxDB supported tagging protocol.
// For example, if the stat is `hello.world` and the tags are `{universe: milkyWay, planet: earth}` and the separator is `,`,
// the stat will be emitted as `hello.world,planet=earth,universe=milkyWay`.
//
// For more details on the protocol see:
// - Datadog: https://docs.datadoghq.com/developers/dogstatsd/datagram_shell
// - InfluxDB: https://github.com/influxdata/telegraf/blob/ce9411343076b56dabd77fc8845cc58872d4b2e6/plugins/inputs/statsd/README.md#influx-statsd
func appendSeparatedTags(name string, separator string, tags []statsd.Tag) string {
	var buffer strings.Builder
	buffer.WriteString(name)
	for _, tg := range tags {
		buffer.WriteString(separator + tg[0] + "=" + tg[1])
	}
	return buffer.String()
}

func (e *statsdExporter) buildTags(attrs attribute.Set) []statsd.Tag {
	if attrs.Len() == 0 {
		return nil
	}

	tags := make([]statsd.Tag, 0, attrs.Len())
	iter := attrs.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		tags = append(tags, statsd.Tag{string(kv.Key), kv.Value.AsString()})
	}

	// Sort tags for consistency
	sort.Slice(tags, func(i, j int) bool {
		return tags[i][0] < tags[j][0]
	})

	return tags
}
