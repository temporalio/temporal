package callback

import "go.temporal.io/server/common/metrics"

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

	// Emitted for both the internal and outbound paths, once the transition has committed.
	InvocationResultCounter = metrics.NewCounterDef(
		"callback_invocation_results",
		metrics.WithDescription("Committed callback invocation results, by disposition: succeeded, retrying, or failed. A failed result is terminal."),
	)
	InvocationAttemptsHistogram = metrics.NewDimensionlessHistogramDef(
		"callback_invocation_attempts",
		metrics.WithDescription("Attempts a callback had made on reaching a terminal disposition, by disposition."),
	)
)

// Disposition tag values for InvocationResultCounter and InvocationAttemptsHistogram.
const (
	dispositionSucceeded = "succeeded"
	dispositionRetrying  = "retrying"
	dispositionFailed    = "failed"
)

// Internal-path outcomes decided before the RPC is issued. Failures after it are tagged by
// gRPC status code.
const (
	outcomeSuccess           = "success"
	outcomeMissingToken      = "missing-token"
	outcomeTokenDecodeError  = "token-decode-error"
	outcomeInvalidRef        = "invalid-ref"
	outcomeRequestBuildError = "request-build-error"
	outcomeRequestTimeout    = "request-timeout"
)
