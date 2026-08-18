package nexusoperation

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.uber.org/mock/gomock"
)

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestClientProviderFactoryUsesSelectedHTTPClient(t *testing.T) {
	errHTTPClientCalled := errors.New("HTTP client called")

	tests := []struct {
		name      string
		clusterID string
		target    *persistencespb.NexusEndpointTarget
	}{
		{
			name: "external without cluster ID",
			target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_External_{
					External: &persistencespb.NexusEndpointTarget_External{
						Url: "http://external.invalid",
					},
				},
			},
		},
		{
			name:      "external with cluster ID",
			clusterID: "cluster-id",
			target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_External_{
					External: &persistencespb.NexusEndpointTarget_External{
						Url: "http://external.invalid",
					},
				},
			},
		},
		{
			name: "worker without cluster ID",
			target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_Worker_{
					Worker: &persistencespb.NexusEndpointTarget_Worker{},
				},
			},
		},
		{
			name:      "worker with cluster ID",
			clusterID: "cluster-id",
			target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_Worker_{
					Worker: &persistencespb.NexusEndpointTarget_Worker{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			transport := testRoundTripper(func(*http.Request) (*http.Response, error) {
				return nil, errHTTPClientCalled
			})

			rpcFactory := common.NewMockRPCFactory(ctrl)
			rpcFactory.EXPECT().CreateLocalFrontendHTTPClient().Return(
				&common.FrontendHTTPClient{
					Client:  http.Client{Transport: transport},
					Address: "frontend.invalid",
					Scheme:  "http",
				},
				nil,
			)

			clusterMetadata := cluster.NewMockMetadata(ctrl)
			clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
				"current": {ClusterID: test.clusterID},
			})
			clusterMetadata.EXPECT().GetCurrentClusterName().Return("current")

			provider, err := clientProviderFactory(
				func(string, string) http.RoundTripper {
					return transport
				},
				clusterMetadata,
				rpcFactory,
			)
			require.NoError(t, err)

			client, err := provider(
				context.Background(),
				"namespace-id",
				&persistencespb.NexusEndpointEntry{
					Id: "endpoint-id",
					Endpoint: &persistencespb.NexusEndpoint{
						Spec: &persistencespb.NexusEndpointSpec{
							Target: test.target,
						},
					},
				},
				"service",
			)
			require.NoError(t, err)

			_, err = client.StartOperation(
				context.Background(),
				"operation",
				nil,
				nexus.StartOperationOptions{},
			)
			require.ErrorIs(t, err, errHTTPClientCalled)
		})
	}
}
