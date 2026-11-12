//go:build !temporal_minimal || temporal_with_statsd

package metrics

import (
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	statsdreporter "go.temporal.io/server/common/metrics/tally/statsd"
)

func validateStatsdConfig(*StatsdConfig) error { return nil }

// newStatsdScope returns a new statsd scope with
// a default reporting interval of a second
func newStatsdScope(logger log.Logger, c *Config) tally.Scope {
	config := c.Statsd
	if len(config.HostPort) == 0 {
		return tally.NoopScope
	}
	statter, err := statsd.NewClientWithConfig(&statsd.ClientConfig{
		Address:       config.HostPort,
		Prefix:        config.Prefix,
		FlushInterval: config.FlushInterval,
		FlushBytes:    config.FlushBytes,
	})
	if err != nil {
		logger.Fatal("error creating statsd client", tag.Error(err))
	}
	// NOTE: according to (https://github.com/uber-go/tally) Tally's statsd implementation doesn't support tagging.
	// Therefore, we implement Tally interface to have a statsd reporter that can support tagging
	opts := statsdreporter.Options{
		TagSeparator: c.Statsd.Reporter.TagSeparator,
	}
	reporter := statsdreporter.NewReporter(statter, opts)
	scopeOpts := tally.ScopeOptions{
		Tags:     c.Tags,
		Reporter: reporter,
		Prefix:   c.Prefix,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}
