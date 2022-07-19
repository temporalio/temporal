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

package taskqueue

import (
	"context"
	"math"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/backoff"
	p "go.temporal.io/server/common/persistence"
)

var retryForeverPolicy = newRetryForeverPolicy()

func (s *Scavenger) completeTasks(key *p.TaskQueueKey, exclusiveMaxTaskID int64, limit int) (int, error) {
	var n int
	var err error
	err = s.retryForever(func() error {
		n, err = s.db.CompleteTasksLessThan(context.TODO(), &p.CompleteTasksLessThanRequest{
			NamespaceID:        key.NamespaceID,
			TaskQueueName:      key.TaskQueueName,
			TaskType:           key.TaskQueueType,
			ExclusiveMaxTaskID: exclusiveMaxTaskID,
			Limit:              limit,
		})
		return err
	})
	return n, err
}

func (s *Scavenger) getTasks(key *p.TaskQueueKey, batchSize int) (*p.GetTasksResponse, error) {
	var err error
	var resp *p.GetTasksResponse
	err = s.retryForever(func() error {
		resp, err = s.db.GetTasks(context.TODO(), &p.GetTasksRequest{
			NamespaceID:        key.NamespaceID,
			TaskQueue:          key.TaskQueueName,
			TaskType:           key.TaskQueueType,
			InclusiveMinTaskID: 0, // get the first N tasks sorted by taskID
			ExclusiveMaxTaskID: math.MaxInt64,
			PageSize:           batchSize,
		})
		return err
	})
	return resp, err
}

func (s *Scavenger) listTaskQueue(pageSize int, pageToken []byte) (*p.ListTaskQueueResponse, error) {
	var err error
	var resp *p.ListTaskQueueResponse
	err = s.retryForever(func() error {
		resp, err = s.db.ListTaskQueue(context.TODO(), &p.ListTaskQueueRequest{
			PageSize:  pageSize,
			PageToken: pageToken,
		})
		return err
	})
	return resp, err
}

func (s *Scavenger) deleteTaskQueue(key *p.TaskQueueKey, rangeID int64) error {
	// retry only on service busy errors
	return backoff.ThrottleRetry(func() error {
		return s.db.DeleteTaskQueue(context.TODO(), &p.DeleteTaskQueueRequest{
			TaskQueue: &p.TaskQueueKey{
				NamespaceID:   key.NamespaceID,
				TaskQueueName: key.TaskQueueName,
				TaskQueueType: key.TaskQueueType,
			},
			RangeID: rangeID,
		})
	}, retryForeverPolicy, func(err error) bool {
		_, ok := err.(*serviceerror.ResourceExhausted)
		return ok
	})
}

func (s *Scavenger) retryForever(op func() error) error {
	return backoff.ThrottleRetry(op, retryForeverPolicy, s.isRetryable)
}

func newRetryForeverPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(250 * time.Millisecond)
	policy.SetExpirationInterval(backoff.NoInterval)
	policy.SetMaximumInterval(30 * time.Second)
	return policy
}

func (s *Scavenger) isRetryable(err error) bool {
	return s.Alive()
}
