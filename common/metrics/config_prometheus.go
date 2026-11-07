//go:build !temporal_minimal || temporal_with_prometheus

package metrics

import (
	"errors"
	"fmt"
	"strings"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
)

func validatePrometheusConfig(*PrometheusConfig) error { return nil }

// tally sanitizer options that satisfy both Prometheus and M3 restrictions.
// This will rename metrics at the tally emission level, so metrics name we
// use maybe different from what gets emitted. In the current implementation
// it will replace - and . with _
// We should still ensure that the base metrics are prometheus compatible,
// but this is necessary as the same prom client initialization is used by
// our system workflows.
var (
	safeCharacters = []rune{'_'}

	defaultTallySanitizeOptions = tally.SanitizeOptions{
		NameCharacters: tally.ValidCharacters{
			Ranges:     tally.AlphanumericRange,
			Characters: safeCharacters,
		},
		KeyCharacters: tally.ValidCharacters{
			Ranges:     tally.AlphanumericRange,
			Characters: safeCharacters,
		},
		ValueCharacters: tally.ValidCharacters{
			Ranges:     tally.AlphanumericRange,
			Characters: safeCharacters,
		},
		ReplacementCharacter: tally.DefaultReplacementCharacter,
	}
)

func convertSanitizeOptionsToTally(config *PrometheusConfig) (tally.SanitizeOptions, error) {
	if config.SanitizeOptions == nil {
		return defaultTallySanitizeOptions, nil
	}

	return config.SanitizeOptions.toTally()
}

func convertPrometheusConfigToTally(
	clientConfig *ClientConfig,
	config *PrometheusConfig,
) *prometheus.Configuration {
	defaultObjectives := make([]prometheus.SummaryObjective, len(config.DefaultSummaryObjectives))
	for i, item := range config.DefaultSummaryObjectives {
		defaultObjectives[i].AllowedError = item.AllowedError
		defaultObjectives[i].Percentile = item.Percentile
	}

	return &prometheus.Configuration{
		HandlerPath:              config.HandlerPath,
		ListenNetwork:            config.ListenNetwork,
		ListenAddress:            config.ListenAddress,
		TimerType:                "histogram",
		DefaultHistogramBuckets:  buildTallyTimerHistogramBuckets(clientConfig, config),
		DefaultSummaryObjectives: defaultObjectives,
		OnError:                  config.OnError,
	}
}

func buildTallyTimerHistogramBuckets(
	clientConfig *ClientConfig,
	config *PrometheusConfig,
) []prometheus.HistogramObjective {
	if len(config.DefaultHistogramBuckets) > 0 {
		result := make([]prometheus.HistogramObjective, len(config.DefaultHistogramBuckets))
		for i, item := range config.DefaultHistogramBuckets {
			result[i].Upper = item.Upper
		}
		return result
	}

	if len(config.DefaultHistogramBoundaries) > 0 {
		result := make([]prometheus.HistogramObjective, 0, len(config.DefaultHistogramBoundaries))
		for _, value := range config.DefaultHistogramBoundaries {
			result = append(result, prometheus.HistogramObjective{
				Upper: value,
			})
		}
		return result
	}

	boundaries := clientConfig.PerUnitHistogramBoundaries[Milliseconds]
	result := make([]prometheus.HistogramObjective, 0, len(boundaries))
	for _, boundary := range boundaries {
		result = append(result, prometheus.HistogramObjective{
			Upper: boundary / float64(time.Second/time.Millisecond), // convert milliseconds to seconds
		})
	}
	return result
}

// newPrometheusScope returns a new prometheus scope with
// a default reporting interval of a second
func newPrometheusScope(logger log.Logger, c *Config) tally.Scope {
	sanitizeOptions, err := convertSanitizeOptionsToTally(c.Prometheus)
	if err != nil {
		logger.Fatal("invalid sanitize options input on prometheus config", tag.Error(err))
		return nil
	}

	if c.Prometheus.LoggerRPS > 0 {
		logger = log.NewThrottledLogger(logger, func() float64 { return c.Prometheus.LoggerRPS })
	}

	return buildPrometheusScope(
		logger,
		convertPrometheusConfigToTally(&c.ClientConfig, c.Prometheus),
		sanitizeOptions,
		&c.ClientConfig,
	)
}

func buildPrometheusScope(
	logger log.Logger,
	config *prometheus.Configuration,
	sanitizeOptions tally.SanitizeOptions,
	clientConfig *ClientConfig,
) tally.Scope {
	var definitionConflicts tally.Counter
	reporter, err := config.NewReporter(
		prometheus.ConfigurationOptions{
			Registry: prom.NewRegistry(),
			OnError: func(err error) {
				// Tally drops recordings if the same metric name is used with different label names or help text.
				// Report a soft assertion so tests using this reporter fail, while production keeps running.
				if strings.Contains(err.Error(), "has different label names or a different help string") {
					definitionConflicts.Inc(1)
					softassert.Fail(logger, "error in prometheus reporter", tag.Error(err))
					return
				}
				logger.Warn("error in prometheus reporter", tag.Error(err))
			},
		},
	)
	if err != nil {
		logger.Fatal("error creating prometheus reporter", tag.Error(err))
	}
	scopeOpts := tally.ScopeOptions{
		Tags:            clientConfig.Tags,
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sanitizeOptions,
		Prefix:          clientConfig.Prefix,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	// Cache the counter because OnError runs while a scope's metric lock is held.
	definitionConflicts = scope.Counter(PrometheusMetricDefinitionConflicts.Name())
	return scope
}

func (s SanitizeRange) toTally() (tally.SanitizeRange, error) {
	startRangeRunes := []rune(s.StartRange)
	if len(startRangeRunes) != 1 {
		return tally.SanitizeRange{}, fmt.Errorf("start range '%+v' must be a single rune", startRangeRunes)
	}

	endRangeRunes := []rune(s.EndRange)
	if len(endRangeRunes) != 1 {
		return tally.SanitizeRange{}, fmt.Errorf("end range '%+v' must be a single rune", endRangeRunes)
	}

	return tally.SanitizeRange([2]rune{startRangeRunes[0], endRangeRunes[0]}), nil
}

func (v ValidCharacters) toTally() (tally.ValidCharacters, error) {
	var ranges []tally.SanitizeRange

	for _, r := range v.Ranges {
		tallyRange, err := r.toTally()
		if err != nil {
			return tally.ValidCharacters{}, err
		}

		ranges = append(ranges, tallyRange)
	}

	return tally.ValidCharacters{
		Ranges:     ranges,
		Characters: []rune(v.SafeCharacters),
	}, nil
}

func (s SanitizeOptions) toTally() (tally.SanitizeOptions, error) {
	tallyNameChars, err := s.NameCharacters.toTally()
	if err != nil {
		return tally.SanitizeOptions{}, fmt.Errorf("invalid nameChars: %v", err)
	}

	tallyKeyChars, err := s.KeyCharacters.toTally()
	if err != nil {
		return tally.SanitizeOptions{}, fmt.Errorf("invalid keyChars: %v", err)
	}

	tallyValueChars, err := s.ValueCharacters.toTally()
	if err != nil {
		return tally.SanitizeOptions{}, fmt.Errorf("invalid valueChars: %v", err)
	}

	replacementChars := []rune(s.ReplacementCharacter)
	if len(replacementChars) != 1 {
		return tally.SanitizeOptions{}, errors.New("can only specify a single replacement character")
	}

	return tally.SanitizeOptions{
		NameCharacters:       tallyNameChars,
		KeyCharacters:        tallyKeyChars,
		ValueCharacters:      tallyValueChars,
		ReplacementCharacter: replacementChars[0],
	}, nil
}
