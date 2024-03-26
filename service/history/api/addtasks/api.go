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

package addtasks

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// TaskDeserializer is a trimmed version of [go.temporal.io/server/common/persistence/serialization.Serializer] that
	// requires only the DeserializeTask method.
	TaskDeserializer interface {
		DeserializeTask(category tasks.Category, blob *commonpb.DataBlob) (tasks.Task, error)
	}
)

const (
	// maxTasksPerRequest is the maximum number of tasks that can be added in a single AddTasks API call. We set this to
	// prevent the history service from OOMing when a client sends a request with a large number of tasks because we
	// will deserialize all tasks in memory before adding them to the queue.
	maxTasksPerRequest = 1000
)

// Invoke is the implementation of the history service's AddTasks API. This exposes the [shard.Context.AddTasks] API via
// the history service. This method works by batching tasks by workflow run, and then invoking the relevant shard's
// AddTasks API for each task batch. See [historyservice.HistoryServiceClient.AddTasks] for more details. We don't do
// any validation on the shard ID because that must have been done by whoever provided the shard.Context to this method.
func Invoke(
	ctx context.Context,
	shardContext shard.Context,
	deserializer TaskDeserializer,
	numShards int,
	req *historyservice.AddTasksRequest,
	taskRegistry tasks.TaskCategoryRegistry,
) (*historyservice.AddTasksResponse, error) {
	if len(req.Tasks) > maxTasksPerRequest {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"Too many tasks in request: %d > %d",
			len(req.Tasks),
			maxTasksPerRequest,
		))
	}

	if len(req.Tasks) == 0 {
		return nil, serviceerror.NewInvalidArgument("No tasks in request")
	}

	taskBatches := make(map[definition.WorkflowKey]map[tasks.Category][]tasks.Task)

	for i, task := range req.Tasks {
		if task == nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Nil task at index: %d", i))
		}

		category, err := api.GetTaskCategory(int(task.CategoryId), taskRegistry)
		if err != nil {
			return nil, err
		}

		if task.Blob == nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"Task blob is nil at index: %d",
				i,
			))
		}

		deserializedTask, err := deserializer.DeserializeTask(category, task.Blob)
		if err != nil {
			return nil, err
		}

		shardID := tasks.GetShardIDForTask(deserializedTask, numShards)
		if shardID != int(req.ShardId) {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"Task is for wrong shard: index = %d, task shard = %d, request shard = %d",
				i, shardID, req.ShardId,
			))
		}

		// group by namespaceID + workflowID
		workflowKey := definition.NewWorkflowKey(
			deserializedTask.GetNamespaceID(),
			deserializedTask.GetWorkflowID(),
			"",
		)
		if _, ok := taskBatches[workflowKey]; !ok {
			taskBatches[workflowKey] = make(map[tasks.Category][]tasks.Task, 1)
		}

		taskBatches[workflowKey][category] = append(taskBatches[workflowKey][category], deserializedTask)
	}

	for workflowKey, taskBatch := range taskBatches {
		err := shardContext.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
			ShardID:     shardContext.GetShardID(),
			RangeID:     shardContext.GetRangeID(),
			NamespaceID: workflowKey.NamespaceID,
			WorkflowID:  workflowKey.WorkflowID,
			Tasks:       taskBatch,
		})
		if err != nil {
			return nil, err
		}
	}

	return &historyservice.AddTasksResponse{}, nil
}
