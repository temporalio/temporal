package replication

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

type (
	// DLQWriter is an interface that can be implemented easily by the two different queue solutions that we have.
	// - Queue V1 implements this interface via [persistence.ExecutionManager].
	// - Queue V2 will implement this interface via [go.temporal.io/server/service/history/queues.DLQWriter].
	//
	// We want this interface to make the migration referenced by [persistence.QueueV2] easier.
	DLQWriter interface {
		WriteTaskToDLQ(ctx context.Context, request DLQWriteRequest) error
	}
	// DLQWriteRequest is a request to write a task to the DLQ.
	DLQWriteRequest struct {
		SourceShardID       int32
		SourceCluster       string
		TargetShardID       int32
		ReplicationTaskInfo *persistencespb.ReplicationTaskInfo
	}
	// ExecutionManager is a trimmed version of [go.temporal.io/server/common/persistence.ExecutionManager] that only
	// provides the methods we need.
	ExecutionManager interface {
		PutReplicationTaskToDLQ(
			ctx context.Context,
			request *persistence.PutReplicationTaskToDLQRequest,
		) error
	}
	// executionManagerDLQWriter is a [DLQWriter] that uses the shard's [persistence.ExecutionManager].
	executionManagerDLQWriter struct {
		executionManager ExecutionManager
	}
	// TaskParser is a trimmed version of [go.temporal.io/server/common/persistence/serialization.Serializer]
	// that only provides the methods we need.
	TaskParser interface {
		ParseReplicationTask(replicationTask *persistencespb.ReplicationTaskInfo) (tasks.Task, error)
	}
	// DLQWriterAdapter is a [DLQWriter] that uses the QueueV2 [queues.DLQWriter] object.
	DLQWriterAdapter struct {
		dlqWriter          *queues.DLQWriter
		taskParser         TaskParser
		currentClusterName string
	}
	dlqWriterToggleParams struct {
		fx.In
		Config                    *configs.Config
		ExecutionManagerDLQWriter *executionManagerDLQWriter
		DLQWriterAdapter          *DLQWriterAdapter
	}
	dlqWriterToggle struct {
		*dlqWriterToggleParams
	}
)

// NewExecutionManagerDLQWriter creates a new DLQWriter that uses the [ExecutionManager].
func NewExecutionManagerDLQWriter(executionManager ExecutionManager) *executionManagerDLQWriter {
	return &executionManagerDLQWriter{
		executionManager: executionManager,
	}
}

// NewDLQWriterAdapter creates a new DLQWriter from a QueueV2 [queues.DLQWriter].
func NewDLQWriterAdapter(
	dlqWriter *queues.DLQWriter,
	taskParser TaskParser,
	currentClusterName string,
) *DLQWriterAdapter {
	return &DLQWriterAdapter{
		dlqWriter:          dlqWriter,
		taskParser:         taskParser,
		currentClusterName: currentClusterName,
	}
}

// This creates a new [DLQWriter] that can be toggled between the two implementations.
func newDLQWriterToggle(
	params dlqWriterToggleParams,
) DLQWriter {
	return &dlqWriterToggle{
		dlqWriterToggleParams: &params,
	}
}

// WriteTaskToDLQ implements [DLQWriter.WriteTaskToDLQ] by calling either
// - QueueV1: [ExecutionManagerDLQWriter.WriteTaskToDLQ]
// - QueueV2: [DLQWriterAdapter.WriteTaskToDLQ]
func (d *dlqWriterToggle) WriteTaskToDLQ(ctx context.Context, request DLQWriteRequest) error {
	if d.Config.HistoryReplicationDLQV2() {
		return d.DLQWriterAdapter.WriteTaskToDLQ(ctx, request)
	}
	return d.ExecutionManagerDLQWriter.WriteTaskToDLQ(ctx, request)
}

// WriteTaskToDLQ implements [DLQWriter.WriteTaskToDLQ] by calling [persistence.ExecutionManager.PutReplicationTaskToDLQ].
func (e *executionManagerDLQWriter) WriteTaskToDLQ(
	ctx context.Context,
	request DLQWriteRequest,
) error {
	return e.executionManager.PutReplicationTaskToDLQ(
		ctx, &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: request.SourceCluster,
			// For dlq v1, this is target cluster shardID
			ShardID:  request.TargetShardID,
			TaskInfo: request.ReplicationTaskInfo,
		},
	)
}

// WriteTaskToDLQ implements [DLQWriter.WriteTaskToDLQ] by calling [queues.DLQWriter.Write].
func (d *DLQWriterAdapter) WriteTaskToDLQ(
	ctx context.Context,
	request DLQWriteRequest,
) error {
	task, err := d.taskParser.ParseReplicationTask(request.ReplicationTaskInfo)
	if err != nil {
		return err
	}
	return d.dlqWriter.WriteTaskToDLQ(ctx, request.SourceCluster, d.currentClusterName, int(request.SourceShardID), task, false)
}

// This is a helper function to make it easier to change the DLQWriteRequest format in the future.
func writeTaskToDLQ(
	ctx context.Context,
	dlqWriter DLQWriter,
	sourceShardID int32,
	sourceClusterName string,
	targetShardID int32,
	replicationTaskInfo *persistencespb.ReplicationTaskInfo,
) error {
	return dlqWriter.WriteTaskToDLQ(ctx, DLQWriteRequest{
		SourceShardID:       sourceShardID,
		SourceCluster:       sourceClusterName,
		TargetShardID:       targetShardID,
		ReplicationTaskInfo: replicationTaskInfo,
	})
}
