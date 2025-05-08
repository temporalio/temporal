package interfaces

import (
	"context"
	"time"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/service/history/tasks"
)

type (
	ReplicationStream interface {
		SubscribeReplicationNotification(string) (<-chan struct{}, string)
		UnsubscribeReplicationNotification(string)
		ConvertReplicationTask(
			ctx context.Context,
			task tasks.Task,
			clusterID int32,
		) (*replicationspb.ReplicationTask, error)

		GetReplicationTasksIter(
			ctx context.Context,
			pollingCluster string,
			minInclusiveTaskID int64,
			maxExclusiveTaskID int64,
		) (collection.Iterator[tasks.Task], error)

		GetMaxReplicationTaskInfo() (int64, time.Time)
	}
)
