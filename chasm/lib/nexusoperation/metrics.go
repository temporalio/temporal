package nexusoperation

import (
	failurepb "go.temporal.io/api/failure/v1"
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
	"nexus_operation_success",
	metrics.WithDescription("Nexus Operations successfully completed."),
)
var NexusOperationFailedCount = metrics.NewCounterDef(
	"nexus_operation_fail",
	metrics.WithDescription("Nexus Operations failures."),
)

// FailedReasonHandlerError means the start request failed with a non-retryable Nexus handler error.
// Keep one reason for all handler errors; nexus_outbound_requests records their types.
const FailedReasonHandlerError metrics.ReasonString = "handler_error"

// FailedReasonCallerError means the caller ended the operation after a non-retryable start failure,
// such as an invalid response. It identifies where the failure was resolved, not who caused it.
const FailedReasonCallerError metrics.ReasonString = "caller_error"

// FailedReasonOperationError means the handler reported the operation as failed, at start or completion.
const FailedReasonOperationError metrics.ReasonString = "operation_error"

// OperationFailedReason returns the metric reason when a start attempt permanently fails the operation.
// Pass the unwrapped failure, not the NexusOperationFailure recorded in history.
//
// Only the top-level failure is inspected. Other failures, including nil, use FailedReasonOperationError.
func OperationFailedReason(failure *failurepb.Failure) metrics.ReasonString {
	switch {
	case failure.GetNexusHandlerFailureInfo() != nil:
		return FailedReasonHandlerError
	case failure.GetServerFailureInfo() != nil,
		failure.GetApplicationFailureInfo().GetType() == "CallError": // HSM specific check
		return FailedReasonCallerError
	default:
		return FailedReasonOperationError
	}
}

var NexusOperationCancelCount = metrics.NewCounterDef(
	"nexus_operation_cancel",
	metrics.WithDescription("Nexus Operations cancellations."),
)
var NexusOperationTerminateCount = metrics.NewCounterDef(
	"nexus_operation_terminate",
	metrics.WithDescription("Nexus Operations that were terminated before completion."),
)
var NexusOperationTimeoutCount = metrics.NewCounterDef(
	"nexus_operation_timeout",
	metrics.WithDescription("Nexus Operations that timed out before completion."),
)

var HeaderPrefixUsageCounter = metrics.NewCounterDef(
	"nexus_header_prefix_usage",
	metrics.WithDescription("Nexus Operation requests with header keys that start with a tracked or rejected prefix, tagged by prefix and by whether the request was rejected."),
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
	metrics.WithDescription("Duration from Nexus Operation started time to completed time. Only emitted for async operations."),
)

type NexusMetricTagConfig struct {
	// Include service name as a metric tag. Used for caller and handler metrics.
	IncludeServiceTag bool
	// Include operation name as a metric tag. Used for caller and handler metrics.
	IncludeOperationTag bool
	// Include the operation backend as a metric tag. Only used for caller outcome and latency metrics.
	IncludeBackendTag bool
	// Configuration for mapping request headers to metric tags. Only used for handler metrics.
	HeaderTagMappings []NexusHeaderTagMapping
}

type NexusHeaderTagMapping struct {
	// Name of the request header to extract value from
	SourceHeader string
	// Name of the metric tag to set with the header value
	TargetTag string
}
