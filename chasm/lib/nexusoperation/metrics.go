package nexusoperation

import (
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
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

// FailedReasonOperationFailed is the NexusOperationFailedCount reason for an operation the handler
// reported as failed, synchronously at start or later via a completion.
const FailedReasonOperationFailed metrics.ReasonString = "operation_failed"

// FailedReasonServerError is the NexusOperationFailedCount reason for an operation that the caller
// failed itself after a start attempt, e.g. because the handler's response exceeded a size limit or the
// request failed with a non-retryable server error.
const FailedReasonServerError metrics.ReasonString = "server_error"

// AttemptFailedReason returns the NexusOperationFailedCount reason for an operation that a start
// attempt failed. failure is the failure the attempt resolved the operation with, not the
// NexusOperationFailure wrapper recorded in history.
//
// A start attempt rejected with a non-retryable handler error resolves with the handler failure at
// the top level; that reports "handler_error:<type>", with the type capped to the Nexus spec's
// handler error types plus UNKNOWN so that a handler cannot mint new time series. A non-retryable
// failure the caller raises itself reports FailedReasonServerError; CHASM records these as server
// failures and HSM as "CallError" application failures. Any other failure, including a nil one,
// reports FailedReasonOperationFailed: the handler reporting the operation as failed.
func AttemptFailedReason(failure *failurepb.Failure) metrics.ReasonString {
	if info := failure.GetNexusHandlerFailureInfo(); info != nil {
		return metrics.ReasonString("handler_error:" + commonnexus.BoundHandlerErrorType(info.GetType()))
	}
	if failure.GetServerFailureInfo() != nil || failure.GetApplicationFailureInfo().GetType() == "CallError" {
		return FailedReasonServerError
	}
	return FailedReasonOperationFailed
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
	// Configuration for mapping request headers to metric tags. Only used for handler metrics.
	HeaderTagMappings []NexusHeaderTagMapping
}

type NexusHeaderTagMapping struct {
	// Name of the request header to extract value from
	SourceHeader string
	// Name of the metric tag to set with the header value
	TargetTag string
}
