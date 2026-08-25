package frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
)

func TestNexusForwardingInterceptorInterceptNexus(t *testing.T) {
	metadata := clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))
	currentCluster := cluster.TestCurrentClusterName
	remoteCluster := cluster.TestAlternativeClusterName

	type requestDisposition int
	const (
		requestFailed requestDisposition = iota
		requestHandledLocally
		requestForwarded
	)

	// dummy server to simulate fowarded req
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"token":"operation-token","state":"running"}`)
	}))
	defer server.Close()
	forwardingClient := testFrontendHTTPClientCache{clients: map[string]*common.FrontendHTTPClient{
		remoteCluster: {
			Client:  *server.Client(),
			Address: server.Listener.Addr().String(),
			Scheme:  "http",
		},
	}}
	options := nexus.StartOperationOptions{
		Header: nexus.Header{"X-Request": "request"},
	}
	requestInput := nexus.NewLazyValue(nexus.DefaultSerializer(), &nexus.Reader{
		ReadCloser: io.NopCloser(bytes.NewBufferString(`"input"`)),
		Header:     nexus.Header{"type": "json"},
	})
	forwardingInfo := interceptornexus.ForwardingInfo{
		OriginalRequestHeaders: http.Header{"X-Original": {"original"}},
		TaskQueue:              "task-queue",
	}

	for _, tc := range []struct {
		name            string
		namespace       *namespace.Namespace
		forwardingOn    bool
		expectedOutcome string
		disposition     requestDisposition
	}{
		{
			name: "local namespace should resolve",
			namespace: namespace.NewLocalNamespaceForTest(
				&persistencespb.NamespaceInfo{Name: testNamespace},
				nil,
				currentCluster,
			),
			disposition: requestHandledLocally,
		},
		{
			name: "global namespace with forwarding enabled should redirect",
			namespace: namespace.NewNamespaceForTest(
				&persistencespb.NamespaceInfo{Name: testNamespace},
				nil,
				true,
				&persistencespb.NamespaceReplicationConfig{ActiveClusterName: remoteCluster, Clusters: []string{currentCluster, remoteCluster}},
				0,
			),
			forwardingOn: true,
			disposition:  requestForwarded,
		},
		{
			name: "global namespace with forwarding disabled should fail",
			namespace: namespace.NewNamespaceForTest(
				&persistencespb.NamespaceInfo{Name: testNamespace},
				nil,
				true,
				&persistencespb.NamespaceReplicationConfig{ActiveClusterName: remoteCluster, Clusters: []string{currentCluster, remoteCluster}},
				0,
			),
			forwardingOn:    false,
			expectedOutcome: "namespace_inactive_forwarding_disabled",
			disposition:     requestFailed,
		},
		{
			name: "global namespace with forwarding enabled to unknown cluster fails",
			namespace: namespace.NewNamespaceForTest(
				&persistencespb.NamespaceInfo{Name: testNamespace},
				nil,
				true,
				&persistencespb.NamespaceReplicationConfig{ActiveClusterName: "unknown-cluster", Clusters: []string{currentCluster}},
				0,
			),
			forwardingOn:    true,
			expectedOutcome: "request_forwarding_failed",
			disposition:     requestFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forwarder := &nexusForwardingInterceptor{
				logger:            log.NewNoopLogger(),
				clusterMetadata:   metadata,
				forwardingClients: forwardingClient,
				redirectionInterceptor: interceptor.NewRedirection(
					dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
					dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
					nil,
					config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding},
					log.NewNoopLogger(),
					nil,
					metrics.NoopMetricsHandler,
					clock.NewRealTimeSource(),
					metadata,
				),
				serviceConfig: &Config{
					EnableNamespaceNotActiveAutoForwarding: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(tc.forwardingOn),
					NexusForwardRequestUseEndpoint:         dynamicconfig.GetBoolPropertyFn(false),
				},
			}
			in := interceptornexus.NewStartOpInput(
				"s", "o", testNamespace, options, requestInput,
				forwardingInfo,
				interceptornexus.RequestMetadata{NamespaceEntry: tc.namespace},
			)
			ctx := context.Background()
			nextCalled := false
			result, err := forwarder.InterceptNexus(
				ctx,
				in,
				func(context.Context, interceptornexus.InterceptorInput) (any, error) {
					nextCalled = true
					return requestHandledLocally, nil
				},
			)
			if tc.expectedOutcome != "" {
				var interceptorErr *interceptornexus.InterceptorError
				require.ErrorAs(t, err, &interceptorErr)
				require.Equal(t, tc.expectedOutcome, interceptorErr.Outcome)
			} else {
				require.NoError(t, err)
			}
			expectedNextCalled := tc.disposition == requestHandledLocally
			require.Equal(t, expectedNextCalled, nextCalled)
			switch tc.disposition {
			case requestHandledLocally:
				require.Equal(t, requestHandledLocally, result)
			case requestForwarded:
				require.IsType(t, &nexus.HandlerStartOperationResultAsync{}, result)
			case requestFailed:
				require.Nil(t, result)
			default:
				t.Fatal("unexpected disposition")
			}
		})
	}
}

type testFrontendHTTPClientCache struct {
	clients map[string]*common.FrontendHTTPClient
}

func (c testFrontendHTTPClientCache) Get(clusterName string) (*common.FrontendHTTPClient, error) {
	client, ok := c.clients[clusterName]
	if !ok {
		return nil, errors.New("unknown cluster")
	}
	return client, nil
}
