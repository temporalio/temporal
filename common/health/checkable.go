package health

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	// Checkable structs represent a component with some health status that changes
	// over time. The HostHealthAggregator will periodically call CheckHealth on
	// all registered Checkable structs and aggregate their results. When implementing CheckHealth,
	// be aware that this is called from a different goroutine! Use locks accordingly.
	Checkable interface {
		NameForHealthReporting() string
		CheckHealth() []HealthCheck
	}

	// HealthCheck is an internal struct that should be convertable to the
	// HealthCheck proto from proto/internal/temporal/server/api/health/v1/message.proto.
	// This struct is copy-friendly and avoids protobuf cruft like the internal mutex
	HealthCheck struct {
		// CheckName should be a globally-unique string that describes
		// this check in under three words. Some examples:
		// "host_availability", "
		CheckName string
		State     enumsspb.HealthState
		Value     float64
		Threshold float64
		// Message is a human-readable description of the result.
		// Not all results need a message! If your CheckName is "workflow_error_rate"
		// and the value and threshold are set, you probably don't need to re-explain that
		// the workflow error rate is above 1%.
		Message string
	}
)
