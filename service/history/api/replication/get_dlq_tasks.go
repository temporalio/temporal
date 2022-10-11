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

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
)

func GetDLQTasks(
	ctx context.Context,
	shard shard.Context,
	replicationAckMgr replication.AckManager,
	taskInfos []*replicationspb.ReplicationTaskInfo,
) ([]*replicationspb.ReplicationTask, error) {
	tasks := make([]*replicationspb.ReplicationTask, 0, len(taskInfos))
	for _, taskInfo := range taskInfos {
		task, err := replicationAckMgr.GetTask(ctx, taskInfo)
		if err != nil {
			shard.GetLogger().Error("Failed to fetch DLQ replication messages.", tag.Error(err))
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}
