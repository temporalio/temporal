package callback

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	queuescommon "go.temporal.io/server/service/history/queues/common"
)

func TestInvocableOutbound_Invoke(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()
	nsDetail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   "namespace-id",
			Name: "namespace-name",
		},
		Config: &persistencespb.NamespaceConfig{},
	}
	ns, err := namespace.FromPersistentState(nsDetail, factory(nsDetail))
	require.NoError(t, err)

	tests := []struct {
		name         string
		callbackURL  string
		caller       HTTPCaller
		assertResult func(*testing.T, invocationResult)
	}{
		{
			name:        "happy-path",
			callbackURL: "http://localhost/callback",
			caller: func(_ *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			assertResult: func(t *testing.T, result invocationResult) {
				require.IsType(t, invocationResultOK{}, result)
			},
		},
		{
			name:        "non-retryable-http-error",
			callbackURL: "http://localhost/callback",
			caller: func(_ *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			assertResult: func(t *testing.T, result invocationResult) {
				require.IsType(t, invocationResultFail{}, result)
				require.ErrorContains(t, result.error(), "handler error (BAD_REQUEST)")
			},
		},
		{
			name:        "retryable-http-error",
			callbackURL: "http://localhost/callback",
			caller: func(_ *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			assertResult: func(t *testing.T, result invocationResult) {
				require.IsType(t, invocationResultRetry{}, result)
				require.ErrorContains(t, result.error(), "handler error (INTERNAL)")
			},
		},
		{
			name:        "operation-not-started-for-system-callback",
			callbackURL: commonnexus.SystemCallbackURL,
			caller: func(_ *http.Request) (*http.Response, error) {
				return nil, serviceerror.NewNexusOperationNotStarted("nexus operation not started", "request-id")
			},
			assertResult: func(t *testing.T, result invocationResult) {
				// The completion must remain retryable so it can be redelivered after the
				// start handler returns, and the original error type must be preserved so
				// WrapError can avoid tripping the circuit breaker.
				require.IsType(t, invocationResultRetry{}, result)
				var opNotStarted *serviceerror.NexusOperationNotStarted
				require.ErrorAs(t, result.error(), &opNotStarted)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := &invocationTaskHandler{
				metricsHandler: metrics.NoopMetricsHandler,
				logger:         log.NewTestLogger(),
				httpCallerProvider: func(_ queuescommon.NamespaceIDAndDestination) HTTPCaller {
					return tc.caller
				},
			}

			invokable := invocableOutbound{
				callback: &callbackspb.Callback_Nexus{Url: tc.callbackURL},
			}

			result := invokable.Invoke(
				context.Background(),
				ns,
				handler,
				nil,
				chasm.TaskAttributes{Destination: tc.callbackURL},
			)
			tc.assertResult(t, result)
		})
	}
}
