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
	resp := &historyservice.ShardReplicationStatus{
		ShardId:        shard.GetShardID(),
		ShardLocalTime: timestamppb.New(shard.GetTimeSource().Now()),
		// RangeId increments on every shard (re)acquire; a change between polls signals
		// ownership churn (which resets the per-remote ack watermarks). WaitReplication uses it
		// to refuse a shard whose ownership is bouncing rather than accept its unreported ack.
		RangeId: shard.GetRangeID(),
	}

	maxReplicationTaskId, maxTaskVisibilityTimeStamp := replicationAckMgr.GetMaxTaskInfo()
	resp.MaxReplicationTaskId = maxReplicationTaskId
	resp.MaxReplicationTaskVisibilityTime = timestamppb.New(maxTaskVisibilityTimeStamp)

	remoteClusters, handoverNamespaces, err := shard.GetReplicationStatus(request.RemoteClusters)
	if err != nil {
		return nil, err
	}
	resp.RemoteClusters = remoteClusters
	resp.HandoverNamespaces = handoverNamespaces
	return resp, nil
}
