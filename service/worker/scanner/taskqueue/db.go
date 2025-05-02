package taskqueue

import (
	"context"
	"math"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	p "go.temporal.io/server/common/persistence"
)

var (
	retryForeverPolicy = backoff.NewExponentialRetryPolicy(250 * time.Millisecond).
		WithExpirationInterval(backoff.NoInterval).
		WithMaximumInterval(30 * time.Second)
)

func (s *Scavenger) completeTasks(
	ctx context.Context,
	key *p.TaskQueueKey,
	exclusiveMaxTaskID int64,
	limit int,
) (int, error) {
	var n int
	var err error
	err = s.retryForever(func() error {
		n, err = s.db.CompleteTasksLessThan(ctx, &p.CompleteTasksLessThanRequest{
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

func (s *Scavenger) getTasks(
	ctx context.Context,
	key *p.TaskQueueKey,
	batchSize int,
) (*p.GetTasksResponse, error) {
	var err error
	var resp *p.GetTasksResponse
	err = s.retryForever(func() error {
		resp, err = s.db.GetTasks(ctx, &p.GetTasksRequest{
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

func (s *Scavenger) listTaskQueue(
	ctx context.Context,
	pageSize int,
	pageToken []byte,
) (*p.ListTaskQueueResponse, error) {
	var err error
	var resp *p.ListTaskQueueResponse
	err = s.retryForever(func() error {
		resp, err = s.db.ListTaskQueue(ctx, &p.ListTaskQueueRequest{
			PageSize:  pageSize,
			PageToken: pageToken,
		})
		return err
	})
	return resp, err
}

func (s *Scavenger) deleteTaskQueue(
	ctx context.Context,
	key *p.TaskQueueKey,
	rangeID int64,
) error {
	// retry only on service busy errors
	return backoff.ThrottleRetry(func() error {
		return s.db.DeleteTaskQueue(ctx, &p.DeleteTaskQueueRequest{
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

func (s *Scavenger) isRetryable(err error) bool {
	return s.Alive()
}
