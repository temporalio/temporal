package nexusoperation

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
)

// wrappedUnavailable mirrors how net/http.Client.Do wraps a RoundTripper error:
// the underlying serviceerror.Unavailable (e.g. returned by a membership-resolver
// RoundTripper when no frontend host is available) is only reachable via Unwrap(),
// not via a direct type assertion or a gRPC status.
func wrappedUnavailable() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://internal",
		Err: serviceerror.NewUnavailable("no frontend host to route request to"),
	}
}

// TestCallErrorToFailure_RetriesTransientServiceErrorEvenWhenWrapped guards against a
// regression where the retryability decision was made on the wrapped call error
// (which IsRetryableRPCError cannot classify) instead of the unwrapped serviceerror
// that errors.AsType already extracted. A transient Unavailable must stay retryable
// regardless of how many layers wrap it, or a benign frontend blip is escalated into a
// terminal, non-retryable operation failure.
func TestCallErrorToFailure_RetriesTransientServiceErrorEvenWhenWrapped(t *testing.T) {
	failure, retryable, err := callErrorToFailure(wrappedUnavailable())
	require.NoError(t, err)
	require.True(t, retryable, "wrapped Unavailable must be classified as retryable")
	require.False(t, failure.GetServerFailureInfo().GetNonRetryable())
}

func TestNewInvocationResult_RetriesTransientServiceErrorEvenWhenWrapped(t *testing.T) {
	result, err := newInvocationResult(nil, wrappedUnavailable())
	require.NoError(t, err)
	require.IsType(t, invocationResultRetry{}, result,
		"wrapped Unavailable must produce a retry result, not a terminal failure")
}

// TestIsRetryableRPCError_RequiresUnwrappedServiceError documents the exact mechanism:
// IsRetryableRPCError only recognizes a serviceerror via a direct (non-unwrapping) type
// assertion or a gRPC status; it cannot see through wrapping on its own. Callers that
// already have the unwrapped serviceerror (e.g. from errors.AsType) must pass that value,
// not the original wrapped error.
func TestIsRetryableRPCError_RequiresUnwrappedServiceError(t *testing.T) {
	wrapped := wrappedUnavailable()
	unwrapped := serviceerror.NewUnavailable("no frontend host to route request to")

	require.False(t, common.IsRetryableRPCError(wrapped),
		"sanity check: the wrapped error alone can't be classified")
	require.True(t, common.IsRetryableRPCError(unwrapped),
		"the unwrapped serviceerror must be classified as retryable")
}
