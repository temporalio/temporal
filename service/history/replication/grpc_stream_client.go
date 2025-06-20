package replication

import (
	"context"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/cluster"
	"google.golang.org/grpc/metadata"
)

type (
	StreamBiDirectionStreamClientProvider struct {
		clusterMetadata cluster.Metadata
		clientBean      client.Bean
	}
)

func NewStreamBiDirectionStreamClientProvider(
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
) *StreamBiDirectionStreamClientProvider {
	return &StreamBiDirectionStreamClientProvider{
		clusterMetadata: clusterMetadata,
		clientBean:      clientBean,
	}
}

func (p *StreamBiDirectionStreamClientProvider) Get(
	ctx context.Context,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) (BiDirectionStreamClient[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	allClusterInfo := p.clusterMetadata.GetAllClusterInfo()
	clusterName, _, err := ClusterIDToClusterNameShardCount(allClusterInfo, serverShardKey.ClusterID)
	if err != nil {
		return nil, err
	}
	adminClient, err := p.clientBean.GetRemoteAdminClient(clusterName)
	if err != nil {
		return nil, err
	}
	ctx = metadata.NewOutgoingContext(ctx, history.EncodeClusterShardMD(
		history.ClusterShardID{
			ClusterID: clientShardKey.ClusterID,
			ShardID:   clientShardKey.ShardID,
		},
		history.ClusterShardID{
			ClusterID: serverShardKey.ClusterID,
			ShardID:   serverShardKey.ShardID,
		},
	))
	return adminClient.StreamWorkflowReplicationMessages(ctx)
}
