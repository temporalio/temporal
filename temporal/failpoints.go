package temporal

import (
	"os"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/failpoints"
)

// resolveFailpoints reads the static startup switch once and returns the failpoint source that the
// server's services share. It holds a registry only if the failpoints.enabled config setting
// (configEnabled) and the failpoints.EnvVar environment variable, read with lookup, are both set;
// lookup defaults to os.LookupEnv. It logs a warning if failpoints are enabled, and records the
// failpoints_enabled gauge either way.
func resolveFailpoints(
	configEnabled bool,
	lookup func(string) (string, bool),
	logger log.Logger,
	metricsHandler metrics.Handler,
) *failpoints.Source {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	decision := failpoints.Decide(configEnabled, lookup)
	enabled := 0.0
	if decision.Enabled {
		enabled = 1
		logger.Warn("Failpoints are enabled: tests and tools can make this server fail, stall or return errors. "+
			"Without an authorizer or TLS anyone who can reach a server port can change failpoints; use only on isolated test clusters.",
			tag.NewStringTag("failpoints-enabled-by", decision.Reason))
	}
	metrics.FailpointsEnabled.With(metricsHandler.WithTags(metrics.ServiceNameTag(primitives.ServerService))).Record(enabled)
	return failpoints.NewSource(decision)
}
