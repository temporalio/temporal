package nexusoperation

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

// wrappedUnavailable returns a transient serviceerror reachable only via Unwrap,
// as an HTTP client wraps transport errors.
func wrappedUnavailable() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://internal",
		Err: serviceerror.NewUnavailable("no frontend host to route request to"),
	}
}

func TestCallErrorToFailure_RetriesTransientServiceErrorEvenWhenWrapped(t *testing.T) {
	t.Parallel()

	failure, retryable, err := callErrorToFailure(wrappedUnavailable())
	require.NoError(t, err)
	require.True(t, retryable, "wrapped Unavailable must be classified as retryable")
	require.False(t, failure.GetServerFailureInfo().GetNonRetryable())
}

func TestNewInvocationResult_RetriesTransientServiceErrorEvenWhenWrapped(t *testing.T) {
	t.Parallel()

	result, err := newInvocationResult(nil, wrappedUnavailable())
	require.NoError(t, err)
	require.IsType(t, invocationResultRetry{}, result,
		"wrapped Unavailable must produce a retry result, not a terminal failure")
}
