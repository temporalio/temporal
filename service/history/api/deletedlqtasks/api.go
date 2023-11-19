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

package deletedlqtasks

import (
	"context"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/tasks"
)

func Invoke(
	ctx context.Context,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	req *historyservice.DeleteDLQTasksRequest,
	registry tasks.TaskCategoryRegistry,
) (*historyservice.DeleteDLQTasksResponse, error) {
	category, err := api.GetTaskCategory(int(req.DlqKey.TaskCategory), registry)
	if err != nil {
		return nil, err
	}

	if req.InclusiveMaxTaskMetadata == nil {
		return nil, serviceerror.NewInvalidArgument("must supply inclusive_max_task_metadata")
	}

	resp, err := historyTaskQueueManager.DeleteTasks(ctx, &persistence.DeleteTasksRequest{
		QueueKey: persistence.QueueKey{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			Category:      category,
			SourceCluster: req.DlqKey.SourceCluster,
			TargetCluster: req.DlqKey.TargetCluster,
		},
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: req.InclusiveMaxTaskMetadata.MessageId,
		},
	})
	if err != nil {
		return nil, err
	}

	return &historyservice.DeleteDLQTasksResponse{MessagesDeleted: resp.MessagesDeleted}, nil
}
