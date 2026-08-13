package health

import (
	"time"

	"go.temporal.io/server/common/stats"
)

// Settings is the high-level config: the overall/all bucket that applies
// across every key, then there is an optional named groups for more specific health checks
type Settings struct {
	// Overall applies across every key
	Overall Thresholds

	// Groups are subsets of keys with their own thresholds
	Groups []Group
}

// Thresholds defines how one bucket of keys is measured and when it's
// considered unhealthy. Nil/empty sub-fields mean "skip that check", which supports
// partial setups (e.g. latency-only or error-ratio-only)
type Thresholds struct {
	// WindowConfig is the latency (tdigest) recording window. Nil = no latency tracking
	WindowConfig *stats.WindowConfig

	// QuantileThresholds are the latency trip points evaluated against the distribution
	QuantileThresholds []QuantileThreshold

	// ErrorRatioThreshold configures the moving-window error ratio. Nil = no error-ratio check
	ErrorRatioThreshold *ErrorRatioThreshold

	// Enforced controls whether a breach actually marks the node unhealthy. When false
	// the checks are still computed and reported but never change the SERVING state
	Enforced bool
}

// Group is a named subset of keys with its own thresholds
type Group struct {
	Name       string
	Keys       []string
	Thresholds Thresholds
}

type QuantileThreshold struct {
	Quantile  float64
	Threshold time.Duration
}

type ErrorRatioThreshold struct {
	WindowSize time.Duration
	BufferSize int
	Threshold  float64
}
