package health

import "time"

type QuantileThreshold struct {
	Quantile  float64
	Threshold time.Duration
}

type HealthCheckGroup struct {
	Name                string
	Keys                []string
	QuantileThresholds  []QuantileThreshold
	ErrorRatioThreshold *float64
	// Enforced controls whether the group actually marks the node unhealthy. When false
	// the group's checks are still computed and reported but never change the state
	Enforced bool
}
