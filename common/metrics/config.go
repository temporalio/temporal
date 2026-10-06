package metrics

import (
	"time"
)

type (
	// Config contains the config items for metrics subsystem
	Config struct {
		ClientConfig `yaml:"clientConfig,inline"`

		// Statsd is the configuration for statsd reporter
		Statsd *StatsdConfig `yaml:"statsd"`
		// Prometheus is the configuration for prometheus reporter
		Prometheus *PrometheusConfig `yaml:"prometheus"`
	}

	ClientConfig struct {
		// Tags is the set of key-value pairs to be reported as part of every metric
		Tags map[string]string `yaml:"tags"`
		// ExcludeTags is a map from tag name string to tag values string list.
		// Each value present in keys will have relevant tag value replaced with "_tag_excluded_"
		// Each value in values list will white-list tag values to be reported as usual.
		ExcludeTags map[string][]string `yaml:"excludeTags"`
		// Prefix sets the prefix to all outgoing metrics
		// When migrating from tally to opentelemetry and to be backward compatible with the existing metric names,
		// if the prefix has a "_" suffix, add an additional "_" at the end.
		// i.e. "temporal" -> "temporal", but "temporal_" -> "temporal__", "temporal__" -> "temporal___".
		// This is because tally implementation blindly adds "_" as the separator between the prefix
		// and the metric name, while opentelemetry implementation only adds it if it's not already there.
		Prefix string `yaml:"prefix"`

		// DefaultHistogramBoundaries defines the default histogram bucket
		// boundaries.
		// Configuration of histogram boundaries for given metric unit.
		//
		// Supported values:
		// - "dimensionless"
		// - "milliseconds"
		// - "bytes"
		PerUnitHistogramBoundaries map[string][]float64 `yaml:"perUnitHistogramBoundaries"`

		// Following configs are added for backwards compatibility when switching from tally to opentelemetry
		// All configs should be set to true when using opentelemetry framework to have the same behavior as tally.

		// WithoutUnitSuffix controls the additional of unit suffixes to metric names.
		// This config only takes effect when using opentelemetry framework.
		// Note: this config only takes effect when using prometheus via opentelemetry framework
		WithoutUnitSuffix bool `yaml:"withoutUnitSuffix"`
		// WithoutCounterSuffix controls the additional of _total suffixes to counter metric names.
		// This config only takes effect when using opentelemetry framework.
		// Note: this config only takes effect when using prometheus via opentelemetry framework
		WithoutCounterSuffix bool `yaml:"withoutCounterSuffix"`
		// RecordTimerInSeconds controls if Timer metric should be emitted as number of seconds
		// (instead of milliseconds).
		// This config only takes effect when using prometheus via opentelemetry framework
		RecordTimerInSeconds bool `yaml:"recordTimerInSeconds"`
		// TagsCacheMaxSize controls the maximum number of entries in the metrics
		// tag cache. When the cache is full, all entries are cleared. Default: 10000.
		TagsCacheMaxSize int `yaml:"tagsCacheMaxSize"`
	}

	// StatsdConfig contains the config items for statsd metrics reporter
	StatsdConfig struct {
		// The host and port of the statsd server
		HostPort string `yaml:"hostPort" validate:"nonzero"`
		// The prefix to use in reporting to statsd
		Prefix string `yaml:"prefix" validate:"nonzero"`
		// FlushInterval is the maximum interval for sending packets.
		// If it is not specified, it defaults to 1 second.
		FlushInterval time.Duration `yaml:"flushInterval"`
		// FlushBytes specifies the maximum udp packet size you wish to send.
		// If FlushBytes is unspecified, it defaults  to 1432 bytes, which is
		// considered safe for local traffic.
		FlushBytes int `yaml:"flushBytes"`
		// Reporter allows additional configuration of the stats reporter, e.g. with custom tagging options.
		Reporter StatsdReporterConfig `yaml:"reporter"`
		// Metric framework: tally/opentelemetry. If not specified, it defaults to tally.
		Framework string `yaml:"framework"`
	}

	StatsdReporterConfig struct {
		// TagSeparator allows tags to be appended with a separator. If not specified tag keys and values
		// are embedded to the stat name directly.
		TagSeparator string `yaml:"tagSeparator"`
	}

	// PrometheusConfig is a new format for config for prometheus metrics.
	PrometheusConfig struct {
		// Metric framework: Tally/OpenTelemetry
		Framework string `yaml:"framework"`
		// Address for prometheus to serve metrics from.
		ListenAddress string `yaml:"listenAddress"`

		// HandlerPath if specified will be used instead of using the default
		// HTTP handler path "/metrics".
		HandlerPath string `yaml:"handlerPath"`

		// LoggerRPS sets the RPS of the logger provided to prometheus. Default of 0 means no limit.
		LoggerRPS float64 `yaml:"loggerRPS"`

		// Configs below are kept for backwards compatibility with previously exposed tally prometheus.Configuration.

		// Deprecated. ListenNetwork if specified will be used instead of using tcp network.
		// Supported networks: tcp, tcp4, tcp6 and unix.
		ListenNetwork string `yaml:"listenNetwork"`

		// Deprecated. TimerType is the default Prometheus type to use for Tally timers.
		// TimerType is always histogram.
		TimerType string `yaml:"timerType"`

		// Deprecated. Please use PerUnitHistogramBoundaries in ClientConfig.
		// DefaultHistogramBoundaries defines the default histogram bucket boundaries for tally timer metrics.
		DefaultHistogramBoundaries []float64 `yaml:"defaultHistogramBoundaries"`

		// Deprecated. Please use PerUnitHistogramBoundaries in ClientConfig.
		// DefaultHistogramBuckets if specified will set the default histogram
		// buckets to be used by the reporter for tally timer metrics.
		// The unit for value specified is Second.
		// If specified, will override DefaultSummaryObjectives and PerUnitHistogramBoundaries["milliseconds"].
		DefaultHistogramBuckets []HistogramObjective `yaml:"defaultHistogramBuckets"`

		// Deprecated. DefaultSummaryObjectives if specified will set the default summary
		// objectives to be used by the reporter.
		// The unit for value specified is Second.
		// If specified, will override PerUnitHistogramBoundaries["milliseconds"].
		DefaultSummaryObjectives []SummaryObjective `yaml:"defaultSummaryObjectives"`

		// Deprecated. OnError specifies what to do when an error either with listening
		// on the specified listen address or registering a metric with the
		// Prometheus. By default the registerer will panic.
		OnError string `yaml:"onError"`

		// Deprecated. SanitizeOptions is an optional field that enables a user to
		// specify which characters are valid and/or should be replaced before metrics
		// are emitted.
		SanitizeOptions *SanitizeOptions `yaml:"sanitizeOptions"`
	}
)

// Deprecated. HistogramObjective is a Prometheus histogram bucket.
// Added for backwards compatibility.
type HistogramObjective struct {
	Upper float64 `yaml:"upper"`
}

// Deprecated. SummaryObjective is a Prometheus summary objective.
// Added for backwards compatibility.
type SummaryObjective struct {
	Percentile   float64 `yaml:"percentile"`
	AllowedError float64 `yaml:"allowedError"`
}

type SanitizeRange struct {
	StartRange string `yaml:"startRange"`
	EndRange   string `yaml:"endRange"`
}

type ValidCharacters struct {
	Ranges         []SanitizeRange `yaml:"ranges"`
	SafeCharacters string          `yaml:"safeChars"`
}

type SanitizeOptions struct {
	NameCharacters       *ValidCharacters `yaml:"nameChars"`
	KeyCharacters        *ValidCharacters `yaml:"keyChars"`
	ValueCharacters      *ValidCharacters `yaml:"valueChars"`
	ReplacementCharacter string           `yaml:"replacementChar"`
}

// Supported framework types
const (
	// FrameworkTally tally framework id
	FrameworkTally = "tally"
	// FrameworkOpentelemetry OpenTelemetry framework id
	FrameworkOpentelemetry = "opentelemetry"
)

// Valid unit name for PerUnitHistogramBoundaries config field
const (
	UnitNameDimensionless = "dimensionless"
	UnitNameMilliseconds  = "milliseconds"
	UnitNameBytes         = "bytes"
)
