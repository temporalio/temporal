package replication

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func GetStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	shard historyi.ShardContext,
	replicationAckMgr replication.AckManager,
) (_ *historyservice.ShardReplicationStatus, retError error) {
	resp := historyservice.ShardReplicationStatus_builder{
		ShardId:        shard.GetShardID(),
		ShardLocalTime: timestamppb.New(shard.GetTimeSource().Now()),
	}.Build()

	maxReplicationTaskId, maxTaskVisibilityTimeStamp := replicationAckMgr.GetMaxTaskInfo()
	resp.SetMaxReplicationTaskId(maxReplicationTaskId)
	resp.SetMaxReplicationTaskVisibilityTime(timestamppb.New(maxTaskVisibilityTimeStamp))

	remoteClusters, handoverNamespaces, err := shard.GetReplicationStatus(request.GetRemoteClusters())
	if err != nil {
		return nil, err
	}
	resp.SetRemoteClusters(remoteClusters)
	resp.SetHandoverNamespaces(handoverNamespaces)
	return resp, nil
}
