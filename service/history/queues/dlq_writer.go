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

package queues

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// DLQWriter can be used to write tasks to the DLQ.
	DLQWriter struct {
		dlqWriter QueueWriter
	}
	// QueueWriter is a subset of persistence.HistoryTaskQueueManager.
	QueueWriter interface {
		CreateQueue(
			ctx context.Context,
			request *persistence.CreateQueueRequest,
		) (*persistence.CreateQueueResponse, error)
		EnqueueTask(
			ctx context.Context,
			request *persistence.EnqueueTaskRequest,
		) (*persistence.EnqueueTaskResponse, error)
	}
)

var (
	ErrSendTaskToDLQ = errors.New("failed to send task to DLQ")
	ErrCreateDLQ     = errors.New("failed to create DLQ")
)

// NewDLQWriter returns a DLQ which will write to the given QueueWriter.
func NewDLQWriter(w QueueWriter) *DLQWriter {
	return &DLQWriter{
		dlqWriter: w,
	}
}

// WriteTaskToDLQ writes a task to the DLQ, creating the underlying queue if it doesn't already exist.
func (q *DLQWriter) WriteTaskToDLQ(ctx context.Context, sourceCluster, targetCluster string, task tasks.Task) error {
	queueKey := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      task.GetCategory(),
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
	}
	_, err := q.dlqWriter.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	if err != nil {
		if !errors.Is(err, persistence.ErrQueueAlreadyExists) {
			return fmt.Errorf("%w: %v", ErrCreateDLQ, err)
		}
	}
	_, err = q.dlqWriter.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     queueKey.QueueType,
		SourceCluster: queueKey.SourceCluster,
		TargetCluster: queueKey.TargetCluster,
		Task:          task,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	return nil
}
