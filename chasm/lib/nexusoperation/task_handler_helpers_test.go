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
// not via a direct type assertion or a gRPC status. IsRetryableRPCError can't see
// through that wrapping, so callers must pass the already-unwrapped serviceerror
// (e.g. from errors.AsType), not the original wrapped error.
func wrappedUnavailable() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://internal",
		Err: serviceerror.NewUnavailable("no frontend host to route request to"),
	}
}

// Regression: a wrapped transient serviceerror must still be retried.
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

func TestIsRetryableRPCError_RequiresUnwrappedServiceError(t *testing.T) {
	wrapped := wrappedUnavailable()
	unwrapped := serviceerror.NewUnavailable("no frontend host to route request to")

	require.False(t, common.IsRetryableRPCError(wrapped),
		"sanity check: the wrapped error alone can't be classified")
	require.True(t, common.IsRetryableRPCError(unwrapped),
		"the unwrapped serviceerror must be classified as retryable")
}
