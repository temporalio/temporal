// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package replication

import (
	"context"

	"go.uber.org/fx"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
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
		ShardID             int32
		SourceCluster       string
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
			ShardID:           request.ShardID,
			TaskInfo:          request.ReplicationTaskInfo,
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
	return d.dlqWriter.WriteTaskToDLQ(ctx, request.SourceCluster, d.currentClusterName, task)
}

// This is a helper function to make it easier to change the DLQWriteRequest format in the future.
func writeTaskToDLQ(
	ctx context.Context,
	dlqWriter DLQWriter,
	shardContext shard.Context,
	sourceClusterName string,
	replicationTaskInfo *persistencespb.ReplicationTaskInfo,
) error {
	return dlqWriter.WriteTaskToDLQ(ctx, DLQWriteRequest{
		ShardID:             shardContext.GetShardID(),
		SourceCluster:       sourceClusterName,
		ReplicationTaskInfo: replicationTaskInfo,
	})
}
