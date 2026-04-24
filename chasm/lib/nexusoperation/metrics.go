package nexusoperation

import (
	"go.temporal.io/server/common/metrics"
)

var OutboundRequestCounter = metrics.NewCounterDef(
	"nexus_outbound_requests",
	metrics.WithDescription("The number of Nexus outbound requests made by the history service."),
)
var OutboundRequestLatency = metrics.NewTimerDef(
	"nexus_outbound_latency",
	metrics.WithDescription("Latency of outbound Nexus requests made by the history service."),
)
var NexusOperationSuccessCount = metrics.NewCounterDef(
	"nexus_operation_success_count",
	metrics.WithDescription("Nexus Operations successfully completed."),
)
var NexusOperationFailedCount = metrics.NewCounterDef(
	"nexus_operation_failed_count",
	metrics.WithDescription("Nexus Operations failures."),
)
var NexusOperationCancelCount = metrics.NewCounterDef(
	"nexus_operation_cancel_count",
	metrics.WithDescription("Nexus Operations cancellations."),
)
var NexusOperationTerminateCount = metrics.NewCounterDef(
	"nexus_operation_terminate_count",
	metrics.WithDescription("Nexus Operations that were terminated before completion."),
)
var NexusOperationTimeoutCount = metrics.NewCounterDef(
	"nexus_operation_timeout_count",
	metrics.WithDescription("Nexus Operations that timed out before completion."),
)

var NexusOperationScheduleToCloseLatency = metrics.NewTimerDef(
	"nexus_operation_schedule_to_close_latency",
	metrics.WithDescription("Duration from Nexus Operation scheduled time to terminal state."),
)
var NexusOperationScheduleToStartLatency = metrics.NewTimerDef(
	"nexus_operation_schedule_to_start_latency",
	metrics.WithDescription("Duration from Nexus Operation scheduled time to started time."),
)
var NexusOperationStartToCloseLatency = metrics.NewTimerDef(
	"nexus_operation_start_to_close_latency",
	metrics.WithDescription("Duration from Nexus Operation scheduled time to completed time. Only emitted for async operations."),
)

// NexusMetricTagConfig controls which optional metric tags are included with Nexus operation
// metrics. Enabling service or operation tags applies to both caller and handler metrics. HeaderTagMappings is
// not used for caller metrics.
type NexusMetricTagConfig struct {
	// Include service name as a metric tag
	IncludeServiceTag bool
	// Include operation name as a metric tag
	IncludeOperationTag bool
	// Configuration for mapping request headers to metric tags
	HeaderTagMappings []NexusHeaderTagMapping
}

type NexusHeaderTagMapping struct {
	// Name of the request header to extract value from
	SourceHeader string
	// Name of the metric tag to set with the header value
	TargetTag string
}
