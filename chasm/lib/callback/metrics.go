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
type outcomeTag string

// Invocation events; the terminal success event is outcomeSuccess below.
const (
	outcomeRetryableError    outcomeTag = "retryable-error"
	outcomeNonretryableError outcomeTag = "nonretryable-error"
)

// Internal-path delivery outcomes decided before the RPC is issued. Failures after it are tagged by
// gRPC status code, under errorOutcomePrefix.
const (
	// outcomeUnknown is the sentinel Invoke starts from, so a path that returns without
	// setting an outcome shows up as unknown rather than as a success.
	outcomeUnknown           outcomeTag = "unknown"
	outcomeSuccess           outcomeTag = "success"
	outcomeMissingToken      outcomeTag = "missing-token"
	outcomeTokenDecodeError  outcomeTag = "token-decode-error"
	outcomeInvalidRef        outcomeTag = "invalid-ref"
	outcomeRequestBuildError outcomeTag = "request-build-error"
	outcomeRequestTimeout    outcomeTag = "request-timeout"
	// An outbound failure that is not a Nexus handler error, e.g. a transport failure.
	outcomeUnknownError outcomeTag = "unknown-error"
)

const (
	handlerErrorOutcomePrefix = "handler-error:"
	errorOutcomePrefix        = "error:"
)
