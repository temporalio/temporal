package callback

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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
			name:        "non-retryable-status-code",
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
			name:        "retryable-status-code",
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
			name:        "retryable-error-dns",
			callbackURL: "http://localhost/callback",
			caller: func(_ *http.Request) (*http.Response, error) {
				return nil, &net.DNSError{
					Err:         "no such host",
					Name:        "example.invalid",
					Server:      "1.1.1.1:11",
					IsNotFound:  true,
					IsTimeout:   false,
					IsTemporary: false,
				}
			},
			assertResult: func(t *testing.T, result invocationResult) {
				require.IsType(t, invocationResultRetry{}, result)
				require.ErrorContains(t, result.error(), "lookup example.invalid on 1.1.1.1:11: no such host")
			},
		},
		{
			name:        "retryable-error-conntimeout",
			callbackURL: "http://localhost/callback",
			caller: func(_ *http.Request) (*http.Response, error) {
				return nil, &net.OpError{
					Op:     "dial",
					Net:    "tcp",
					Source: nil,
					Addr:   &net.TCPAddr{IP: net.ParseIP("192.168.0.1"), Port: 80},
					Err:    os.ErrDeadlineExceeded,
				}
			},
			assertResult: func(t *testing.T, result invocationResult) {
				require.IsType(t, invocationResultRetry{}, result)
				require.ErrorContains(t, result.error(), "dial tcp 192.168.0.1:80: i/o timeout")
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
