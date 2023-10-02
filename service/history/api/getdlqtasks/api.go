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

// Package getdlqtasks contains the logic to implement the [historyservice.HistoryServiceServer.GetDLQTasks] API.
package getdlqtasks

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/consts"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// Invoke the GetDLQTasks API. All errors returned from this function are already translated into the appropriate type
// from the [serviceerror] package.
func Invoke(
	ctx context.Context,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	req *historyservice.GetDLQTasksRequest,
) (*historyservice.GetDLQTasksResponse, error) {
	category, ok := tasks.GetCategoryByID(int32(req.DlqKey.Category))
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid queue category %v", req.DlqKey.Category))
	}

	response, err := historyTaskQueueManager.ReadRawTasks(ctx, &persistence.ReadTasksRequest{
		QueueKey: persistence.QueueKey{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			Category:      category,
			SourceCluster: req.DlqKey.SourceCluster,
			TargetCluster: req.DlqKey.TargetCluster,
		},
		PageSize:      int(req.PageSize),
		NextPageToken: req.NextPageToken,
	})
	if err != nil {
		if errors.Is(err, persistence.ErrReadTasksNonPositivePageSize) {
			return nil, consts.ErrInvalidPageSize
		}

		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetDLQTasks failed. Error: %v", err))
	}

	dlqTasks := make([]*historyservice.HistoryDLQTask, len(response.Tasks))
	for i, task := range response.Tasks {
		dlqTasks[i] = &historyservice.HistoryDLQTask{
			Metadata: &historyservice.HistoryDLQTaskMetadata{
				MessageId: task.MessageMetadata.ID,
			},
			Task: &historyservice.HistoryTask{
				ShardId: task.Task.ShardId,
				Task:    task.Task.Blob,
			},
		}
	}

	return &historyservice.GetDLQTasksResponse{
		DlqTasks:      dlqTasks,
		NextPageToken: response.NextPageToken,
	}, nil
}
