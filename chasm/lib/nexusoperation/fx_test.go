package nexusoperation

import (
	"context"
	"net/http"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.uber.org/mock/gomock"
)

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientProviderFactoryExternalEndpointInjectsTraceContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"current": {ClusterID: "cluster-id"},
	})
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("current")
	rpcFactory := common.NewMockRPCFactory(ctrl)
	rpcFactory.EXPECT().CreateLocalFrontendHTTPClient().Return(&common.FrontendHTTPClient{}, nil)
	tp := trace.NewTracerProvider()

	var traceparent string
	transportProvider := func(_, _ string) http.RoundTripper {
		return testRoundTripper(func(r *http.Request) (*http.Response, error) {
			traceparent = r.Header.Get("traceparent")
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Body:       http.NoBody,
				Header:     http.Header{},
				Request:    r,
			}, nil
		})
	}
	provider, err := clientProviderFactory(transportProvider, clusterMetadata, rpcFactory, tp, nil)
	require.NoError(t, err)

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	client, err := provider(ctx, "namespace-id", endpointEntry, "service")
	require.NoError(t, err)
	handle, err := client.NewOperationHandle("operation", "token")
	require.NoError(t, err)
	require.NoError(t, handle.Cancel(ctx, nexus.CancelOperationOptions{}))
	require.NotEmpty(t, traceparent)
}
