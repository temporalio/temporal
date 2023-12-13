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

package listqueues

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
)

func Invoke(
	ctx context.Context,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	req *historyservice.ListQueuesRequest,
) (*historyservice.ListQueuesResponse, error) {
	resp, err := historyTaskQueueManager.ListQueues(ctx, &persistence.ListQueuesRequest{
		QueueType:     persistence.QueueV2Type(req.QueueType),
		PageSize:      int(req.PageSize),
		NextPageToken: req.NextPageToken,
	})
	if err != nil {
		if errors.Is(err, persistence.ErrNonPositiveListQueuesPageSize) {
			return nil, consts.ErrInvalidPageSize
		}
		if errors.Is(err, persistence.ErrNegativeListQueuesOffset) || errors.Is(err, persistence.ErrInvalidListQueuesNextPageToken) {
			return nil, consts.ErrInvalidPaginationToken
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListQueues failed. Error: %v", err))
	}
	var queues []*historyservice.ListQueuesResponse_QueueInfo
	for _, queue := range resp.Queues {
		queues = append(queues, &historyservice.ListQueuesResponse_QueueInfo{
			QueueName:    queue.QueueName,
			MessageCount: queue.MessageCount,
		})
	}
	return &historyservice.ListQueuesResponse{
		Queues:        queues,
		NextPageToken: resp.NextPageToken,
	}, nil
}
