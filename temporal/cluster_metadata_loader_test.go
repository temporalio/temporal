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
			CurrentClusterName: "cluster1",
			ClusterInformation: map[string]cluster.ClusterInformation{
				"cluster1": {
					RPCAddress: "rpc1",
				},
			},
		},
	}

	metadataManager.EXPECT().ListClusterMetadata(gomock.Any(), gomock.Any()).Return(&persistence.ListClusterMetadataResponse{
		ClusterMetadata: []*persistence.GetClusterMetadataResponse{
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "cluster1",
					ClusterAddress: "rpc1",
					HttpAddress:    "http1",
				},
			},
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "cluster2",
					ClusterAddress: "rpc2",
					HttpAddress:    "http2",
				},
			},
		},
	}, nil)

	err := loader.LoadAndMergeWithStaticConfig(ctx, svc)
	require.NoError(t, err)

	assert.Equal(t, map[string]cluster.ClusterInformation{
		"cluster1": {
			RPCAddress:  "rpc1",
			HTTPAddress: "http1",
		},
		"cluster2": {
			RPCAddress:  "rpc2",
			HTTPAddress: "http2",
		},
	}, svc.ClusterMetadata.ClusterInformation)
}
