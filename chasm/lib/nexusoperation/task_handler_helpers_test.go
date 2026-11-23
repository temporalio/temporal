package nexusoperation

import (
	"net/url"
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
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

	result, err := newInvocationResult(nil, nil, wrappedUnavailable())
	require.NoError(t, err)
	require.IsType(t, invocationResultRetry{}, result,
		"wrapped Unavailable must produce a retry result, not a terminal failure")
}

// wrappedRedirectRejected returns the error http.Client.Do produces when CheckRedirect refuses a redirect.
func wrappedRedirectRejected() error {
	return &url.Error{
		Op:  "Post",
		URL: "http://169.254.169.254/latest/meta-data/",
		Err: commonnexus.ErrRedirectNotAllowed,
	}
}

func TestCallErrorToFailure_RedirectIsNonRetryable(t *testing.T) {
	t.Parallel()

	failure, retryable, err := callErrorToFailure(wrappedRedirectRejected())
	require.NoError(t, err)
	require.False(t, retryable)
	require.True(t, failure.GetServerFailureInfo().GetNonRetryable())
	require.False(t, isDestinationDown(wrappedRedirectRejected()),
		"a redirecting destination is reachable and must not trip the circuit breaker")
}

func TestNewInvocationResult_RedirectFailsOperation(t *testing.T) {
	t.Parallel()

	result, err := newInvocationResult(nil, nil, wrappedRedirectRejected())
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, result)
}

func TestNewCancellationResult_RedirectFailsCancellation(t *testing.T) {
	t.Parallel()

	result, err := newCancellationResult(wrappedRedirectRejected())
	require.NoError(t, err)
	require.IsType(t, cancellationResultFail{}, result)
}

func TestBuildCallbackURL(t *testing.T) {
	t.Parallel()

	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "ns-name", Id: "ns-id"},
		&persistencespb.NamespaceConfig{},
		"active-cluster",
	)
	callbackTemplate, err := template.New("callback").Parse("http://example/callback/{{.NamespaceName}}-{{.NamespaceID}}")
	require.NoError(t, err)

	workerEndpoint := &persistencespb.NexusEndpointEntry{
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_Worker_{},
				},
			},
		},
	}
	externalEndpoint := &persistencespb.NexusEndpointEntry{
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_External_{},
				},
			},
		},
	}

	got, err := buildCallbackURL(callbackTemplate, ns, workerEndpoint)
	require.NoError(t, err)
	require.Equal(t, commonnexus.SystemCallbackURL, got)

	got, err = buildCallbackURL(callbackTemplate, ns, externalEndpoint)
	require.NoError(t, err)
	require.Equal(t, "http://example/callback/ns-name-ns-id", got)
}
