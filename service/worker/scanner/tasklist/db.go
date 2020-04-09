package tasklist

import (
	"time"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/backoff"
	p "github.com/temporalio/temporal/common/persistence"
)

var retryForeverPolicy = newRetryForeverPolicy()

func (s *Scavenger) completeTasks(key *p.TaskListKey, taskID int64, limit int) (int, error) {
	var n int
	var err error
	err = s.retryForever(func() error {
		n, err = s.db.CompleteTasksLessThan(&p.CompleteTasksLessThanRequest{
			NamespaceID:  key.NamespaceID,
			TaskListName: key.Name,
			TaskType:     key.TaskType,
			TaskID:       taskID,
			Limit:        limit,
		})
		return err
	})
	return n, err
}

func (s *Scavenger) getTasks(key *p.TaskListKey, batchSize int) (*p.GetTasksResponse, error) {
	var err error
	var resp *p.GetTasksResponse
	err = s.retryForever(func() error {
		resp, err = s.db.GetTasks(&p.GetTasksRequest{
			NamespaceID: key.NamespaceID,
			TaskList:    key.Name,
			TaskType:    key.TaskType,
			ReadLevel:   -1, // get the first N tasks sorted by taskID
			BatchSize:   batchSize,
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

func (s *Scavenger) deleteTaskList(key *p.TaskListKey, rangeID int64) error {
	// retry only on service busy errors
	return backoff.Retry(func() error {
		return s.db.DeleteTaskList(&p.DeleteTaskListRequest{
			TaskList: &p.TaskListKey{
				NamespaceID: key.NamespaceID,
				Name:        key.Name,
				TaskType:    key.TaskType,
			},
			RangeID: rangeID,
		})
	}, retryForeverPolicy, func(err error) bool {
		_, ok := err.(*serviceerror.ResourceExhausted)
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
