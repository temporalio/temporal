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

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
)

type (
	// DLQWriter is an interface that can be implemented easily by the two different queue solutions that we have.
	// - Queue V1 implements this interface via [persistence.ExecutionManager].
	// - Queue V2 will implement this interface via [go.temporal.io/server/service/history/queues.DLQWriter].
	//
	// We want this interface to make the migration referenced by [persistence.QueueV2] easier.
	DLQWriter interface {
		WriteTaskToDLQ(ctx context.Context, request WriteRequest) error
	}
	// WriteRequest is a request to write a task to the DLQ.
	WriteRequest struct {
		// ShardContext is an argument that we can remove once we migrate to queue V2.
		ShardContext        shard.Context
		SourceCluster       string
		ReplicationTaskInfo *persistencespb.ReplicationTaskInfo
	}
	// ExecutionManagerDLQWriter is a [DLQWriter] that uses the shard's [persistence.ExecutionManager].
	// The zero-value is a valid instance.
	ExecutionManagerDLQWriter struct{}
)

// NewExecutionManagerDLQWriter creates a new DLQWriter.
func NewExecutionManagerDLQWriter() DLQWriter {
	return &ExecutionManagerDLQWriter{}
}

// WriteTaskToDLQ implements [DLQWriter.WriteTaskToDLQ] by calling [persistence.ExecutionManager.PutReplicationTaskToDLQ].
func (e *ExecutionManagerDLQWriter) WriteTaskToDLQ(
	ctx context.Context,
	request WriteRequest,
) error {
	return request.ShardContext.GetExecutionManager().PutReplicationTaskToDLQ(
		ctx, &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: request.SourceCluster,
			ShardID:           request.ShardContext.GetShardID(),
			TaskInfo:          request.ReplicationTaskInfo,
		},
	)
}
