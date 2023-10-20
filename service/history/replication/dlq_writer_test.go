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

package replication_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
)

type (
	fakeShardContext struct {
		shard.Context
		requests []*persistence.PutReplicationTaskToDLQRequest
		shardID  int
	}
	fakeExecutionManager struct {
		persistence.ExecutionManager
		requests *[]*persistence.PutReplicationTaskToDLQRequest
	}
)

func TestNewExecutionManagerDLQWriter_ReplicationTask(t *testing.T) {
	t.Parallel()

	writer := replication.NewExecutionManagerDLQWriter()
	shardContext := &fakeShardContext{
		shardID: 13,
	}
	replicationTaskInfo := &persistencespb.ReplicationTaskInfo{
		TaskId: 21,
	}
	err := writer.WriteTaskToDLQ(context.Background(), replication.WriteRequest{
		ShardContext:        shardContext,
		SourceCluster:       "test-source-cluster",
		ReplicationTaskInfo: replicationTaskInfo,
	})
	require.NoError(t, err)
	if assert.Len(t, shardContext.requests, 1) {
		assert.Equal(t, 13, int(shardContext.requests[0].ShardID))
		assert.Equal(t, "test-source-cluster", shardContext.requests[0].SourceClusterName)
		assert.Equal(t, replicationTaskInfo, shardContext.requests[0].TaskInfo)
	}
}

func (f *fakeShardContext) GetShardID() int32 {
	return int32(f.shardID)
}

func (f *fakeShardContext) GetExecutionManager() persistence.ExecutionManager {
	return &fakeExecutionManager{requests: &f.requests}
}

func (f *fakeExecutionManager) PutReplicationTaskToDLQ(
	_ context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	*f.requests = append(*f.requests, request)
	return nil
}
