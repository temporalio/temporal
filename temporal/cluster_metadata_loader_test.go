package temporal_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/temporal"
)

func TestNewClusterMetadataLoader(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockClusterMetadataManager(ctrl)
	logger := log.NewNoopLogger()
	loader := temporal.NewClusterMetadataLoader(metadataManager, logger)
	ctx := context.Background()
	svc := &config.Config{
		ClusterMetadata: &cluster.Config{
			CurrentClusterName: "current_cluster",
			ClusterInformation: map[string]cluster.ClusterInformation{
				"current_cluster": {
					RPCAddress:  "rpc_static",
					HTTPAddress: "http_static",
				},
				"remote_cluster": {
					RPCAddress: "rpc_static",
				},
			},
		},
	}

	metadataManager.EXPECT().ListClusterMetadata(gomock.Any(), gomock.Any()).Return(&persistence.ListClusterMetadataResponse{
		ClusterMetadata: []*persistence.GetClusterMetadataResponse{
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "cluster1",
					ClusterAddress: "rpc_dynamic",
					HttpAddress:    "http_dynamic",
				},
			},
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "cluster2",
					ClusterAddress: "rpc_dynamic",
					HttpAddress:    "http_dynamic",
				},
			},
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "cluster3",
					ClusterAddress: "rpc_dynamic",
					HttpAddress:    "http_dynamic",
				},
			},
		},
	}, nil)

	err := loader.LoadAndMergeWithStaticConfig(ctx, svc)
	require.NoError(t, err)

	assert.Equal(t, map[string]cluster.ClusterInformation{
		"cluster1": {
			RPCAddress:  "rpc_static",
			HTTPAddress: "http_dynamic",
		},
		"cluster2": {
			RPCAddress:  "rpc2",
			HTTPAddress: "http2",
		},
		"cluster3": {
			RPCAddress:  "rpc3",
			HTTPAddress: "http3",
		},
	}, svc.ClusterMetadata.ClusterInformation)
}
