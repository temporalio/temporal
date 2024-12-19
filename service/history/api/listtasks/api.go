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

package listtasks

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Invoke(
	ctx context.Context,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	executionManager persistence.ExecutionManager,
	request *historyservice.ListTasksRequest,
) (*historyservice.ListTasksResponse, error) {
	adminRequest := request.Request
	taskCategory, ok := taskCategoryRegistry.GetCategoryByID(int(adminRequest.Category))
	if !ok {
		return nil, &serviceerror.InvalidArgument{
			Message: fmt.Sprintf("unknown task category: %v", adminRequest.Category),
		}
	}

	taskRange := adminRequest.GetTaskRange()
	var minTaskKey, maxTaskKey tasks.Key
	if taskRange.InclusiveMinTaskKey != nil {
		minTaskKey = tasks.NewKey(
			timestamp.TimeValue(taskRange.InclusiveMinTaskKey.FireTime),
			taskRange.InclusiveMinTaskKey.TaskId,
		)
		if err := tasks.ValidateKey(minTaskKey); err != nil {
			return nil, &serviceerror.InvalidArgument{
				Message: fmt.Sprintf("invalid minTaskKey: %v", err.Error()),
			}
		}
	}
	if taskRange.ExclusiveMaxTaskKey != nil {
		maxTaskKey = tasks.NewKey(
			timestamp.TimeValue(taskRange.ExclusiveMaxTaskKey.FireTime),
			taskRange.ExclusiveMaxTaskKey.TaskId,
		)
		if err := tasks.ValidateKey(maxTaskKey); err != nil {
			return nil, &serviceerror.InvalidArgument{
				Message: fmt.Sprintf("invalid maxTaskKey: %v", err.Error()),
			}
		}
	}

	resp, err := executionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             adminRequest.ShardId,
		TaskCategory:        taskCategory,
		InclusiveMinTaskKey: minTaskKey,
		ExclusiveMaxTaskKey: maxTaskKey,
		BatchSize:           int(adminRequest.BatchSize),
		NextPageToken:       adminRequest.NextPageToken,
	})
	if err != nil {
		return nil, err
	}

	return &historyservice.ListTasksResponse{
		Response: &adminservice.ListHistoryTasksResponse{
			Tasks:         toAdminTask(resp.Tasks),
			NextPageToken: resp.NextPageToken,
		},
	}, nil
}

func toAdminTask(historyTasks []tasks.Task) []*adminservice.Task {
	var adminTasks []*adminservice.Task
	for _, historyTask := range historyTasks {
		historyTaskVersion := common.EmptyVersion
		if taskWithVersion, ok := historyTask.(tasks.HasVersion); ok {
			historyTaskVersion = taskWithVersion.GetVersion()
		}

		adminTasks = append(adminTasks, &adminservice.Task{
			NamespaceId: historyTask.GetNamespaceID(),
			WorkflowId:  historyTask.GetWorkflowID(),
			RunId:       historyTask.GetRunID(),
			TaskId:      historyTask.GetTaskID(),
			TaskType:    historyTask.GetType(),
			FireTime:    timestamppb.New(historyTask.GetKey().FireTime),
			Version:     historyTaskVersion,
		})
	}
	return adminTasks
}
