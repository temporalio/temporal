package historydlq

import (
	commonspb "go.temporal.io/server/api/common/v1"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
)

func queueKeyToProto(queue QueueKey) *commonspb.HistoryDLQKey {
	return &commonspb.HistoryDLQKey{
		TaskCategory:  queue.CategoryID,
		SourceCluster: queue.SourceCluster,
		TargetCluster: queue.TargetCluster,
	}
}

func queueProtoToKey(queue *commonspb.HistoryDLQKey) QueueKey {
	return QueueKey{
		CategoryID:    queue.GetTaskCategory(),
		SourceCluster: queue.GetSourceCluster(),
		TargetCluster: queue.GetTargetCluster(),
	}
}

func taskIdentityToEntryProto(identity TaskIdentity) *historydlqpb.EntryTaskIdentity {
	return &historydlqpb.EntryTaskIdentity{
		TaskId: identity.TaskID,
	}
}

func entryTaskIdentityProtoToIdentity(identity *historydlqpb.EntryTaskIdentity, sourceShardID int32) TaskIdentity {
	return TaskIdentity{
		SourceShardID: sourceShardID,
		TaskID:        identity.GetTaskId(),
	}
}
