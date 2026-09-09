package callback

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/grpc/codes"
)

// CHASM callback metrics.
// These are defined independently from HSM callbacks to avoid coupling between the two implementations.
var (
	RequestCounter = metrics.NewCounterDef(
		"callback_outbound_requests",
		metrics.WithDescription("The number of callback outbound requests made by the history service."),
	)
	RequestLatencyHistogram = metrics.NewTimerDef(
		"callback_outbound_latency",
		metrics.WithDescription("Latency histogram of outbound callback requests made by the history service."),
	)

	// Named separately from callback_outbound_* rather than sharing those names with a
	// destination tag, so internal failures don't land in that metric's unscoped error alert.
	InternalRequestCounter = metrics.NewCounterDef(
		"callback_internal_requests",
		metrics.WithDescription("The number of internal (cross-shard) callback deliveries made by the history service."),
	)
	InternalRequestLatencyHistogram = metrics.NewTimerDef(
		"callback_internal_latency",
		metrics.WithDescription("Latency histogram of internal (cross-shard) callback deliveries made by the history service."),
	)

	// NexusHandler-variant callback metrics.
	NexusHandlerRequestCounter = metrics.NewCounterDef(
		"callback_nexushandler_requests",
		metrics.WithDescription("The number of NexusHandler callback deliveries made by the history service."),
	)
	NexusHandlerRequestLatencyHistogram = metrics.NewTimerDef(
		"callback_nexushandler_latency",
		metrics.WithDescription("Latency histogram of NexusHandler callback deliveries made by the history service."),
	)

	// Emitted for all forms of callbacks (internal, outbound, and NexusHandler-variant) once the transition has committed.
	InvocationEventCounter = metrics.NewCounterDef(
		"callback_invocation_events",
		metrics.WithDescription("Committed callback invocation events. Per callback: (retryable-error)* (success | nonretryable-error)."),
	)
	InvocationAttemptsHistogram = metrics.NewDimensionlessHistogramDef(
		"callback_invocation_attempts",
		metrics.WithDescription("Attempts a callback had made on reaching a terminal event, by outcome."),
	)
)

// outcomeTag is a value of the outcome tag carried by the metrics above.
//
// While CHASM Callback outcomes are distinct from the Frontend service's Nexus invocation outcomes,
// the labels should match when applicable. See common/nexus.DispatchResult's metricOutcome.
type outcomeTag string

// The tags specific to InvocationEventCounter, which tracks the summary callback invocation event.
const (
	outcomeEventSuccess           outcomeTag = "success"
	outcomeEventRetryableError    outcomeTag = "retryable-error"
	outcomeEventNonRetryableError outcomeTag = "nonretryable-error"
)

// Outcome tags used for more granular tracing, used by InternalRequestCounter or NexusHandlerRequestLatencyHistogram.
// This list is not exhaustive, handlerErrorOutcome and grpcErrorOutcome are used to generate outcome tags too.
const (
	// outcomeUnknown is the sentinel Invoke starts from, so a path that returns without
	// setting an outcome shows up as unknown rather than as a success.
	outcomeUnknown outcomeTag = "unknown"
	// An outbound failure that is not a Nexus handler error, e.g. a transport failure.
	outcomeUnknownError outcomeTag = "unknown-error"

	outcomeFailure           outcomeTag = "failure"
	outcomeLegacyFailure     outcomeTag = "operation_error" // Emitted for clients sending older, deprecated error formats.
	outcomeInvalidRef        outcomeTag = "invalid-ref"
	outcomeMissingToken      outcomeTag = "missing-token"
	outcomeRequestBuildError outcomeTag = "request-build-error"
	outcomeRequestTimeout    outcomeTag = "request-timeout"
	outcomeSuccess           outcomeTag = "success"
	outcomeTokenDecodeError  outcomeTag = "token-decode-error"
)

// handlerErrorOutcome generates the outcomeTag for a received Nexus HandlerError.
func handlerErrorOutcome(handlerErr *nexus.HandlerError) outcomeTag {
	// Only emit outcome tags for the error types defined in the spec.
	// All others will be labeled as "handler-error:UNKNOWN", to prevent
	// the metric from having unbounded cardinality.
	boundedErrType := commonnexus.BoundHandlerErrorType(string(handlerErr.Type))
	return outcomeTag("handler-error:" + boundedErrType)
}

// grpcErrorOutcome returns an outcomeTag derived from an error implementing
// Statsui()/GRPCStatus(). Otherwise defaults to "error:Unknown".
func grpcErrorOutcome(err error) outcomeTag {
	tagSuffix := codes.Unknown.String()
	if st, ok := common.GetRPCStatus(err); ok {
		tagSuffix = st.Code().String()
	}
	return outcomeTag("error:" + tagSuffix)
}
