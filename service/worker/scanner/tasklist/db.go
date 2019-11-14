// Copyright (c) 2017 Uber Technologies, Inc.
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

package tasklist

import (
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	p "github.com/uber/cadence/common/persistence"
)

var retryForeverPolicy = newRetryForeverPolicy()

func (s *Scavenger) completeTasks(key *taskListKey, taskID int64, limit int) (int, error) {
	var n int
	var err error
	err = s.retryForever(func() error {
		n, err = s.db.CompleteTasksLessThan(&p.CompleteTasksLessThanRequest{
			DomainID:     key.DomainID,
			TaskListName: key.Name,
			TaskType:     key.TaskType,
			TaskID:       taskID,
			Limit:        limit,
		})
		return err
	})
	return n, err
}

func (s *Scavenger) getTasks(key *taskListKey, batchSize int) (*p.GetTasksResponse, error) {
	var err error
	var resp *p.GetTasksResponse
	err = s.retryForever(func() error {
		resp, err = s.db.GetTasks(&p.GetTasksRequest{
			DomainID:  key.DomainID,
			TaskList:  key.Name,
			TaskType:  key.TaskType,
			ReadLevel: -1, // get the first N tasks sorted by taskID
			BatchSize: batchSize,
		})
		return err
	})
	return resp, err
}

func (s *Scavenger) listTaskList(pageSize int, pageToken []byte) (*p.ListTaskListResponse, error) {
	var err error
	var resp *p.ListTaskListResponse
	err = s.retryForever(func() error {
		resp, err = s.db.ListTaskList(&p.ListTaskListRequest{
			PageSize:  pageSize,
			PageToken: pageToken,
		})
		return err
	})
	return resp, err
}

func (s *Scavenger) deleteTaskList(key *taskListKey, rangeID int64) error {
	// retry only on service busy errors
	return backoff.Retry(func() error {
		return s.db.DeleteTaskList(&p.DeleteTaskListRequest{
			DomainID:     key.DomainID,
			TaskListName: key.Name,
			TaskListType: key.TaskType,
			RangeID:      rangeID,
		})
	}, retryForeverPolicy, func(err error) bool {
		_, ok := err.(*shared.ServiceBusyError)
		return ok
	})
}

func (s *Scavenger) retryForever(op func() error) error {
	return backoff.Retry(op, retryForeverPolicy, s.isRetryable)
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
