package taskqueue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// subqueueTaskFixture models the persistence behavior that matters to the
// scavenger: tasks are partitioned by subqueue, so a read scoped to one
// subqueue must not observe tasks stored in another one.
type subqueueTaskFixture struct {
	tasks map[int][]*persistencespb.AllocatedTaskInfo
}

func (f *subqueueTaskFixture) add(subqueue int, taskID int64, expired bool) {
	expiry := time.Now().UTC().Add(time.Hour)
	if expired {
		expiry = time.Now().UTC().Add(-time.Hour)
	}
	f.tasks[subqueue] = append(f.tasks[subqueue], &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.NewString(),
			WorkflowId:       uuid.NewString(),
			RunId:            uuid.NewString(),
			CreateTime:       timestamp.TimePtr(time.Now().UTC()),
			ScheduledEventId: 3,
			ExpiryTime:       timestamppb.New(expiry),
		},
		TaskId: taskID,
	})
}

// TestScavengerDeletesExpiredTasksInAllSubqueues verifies that the scavenger
// scans every subqueue listed in the task queue metadata, deletes expired
// tasks stored outside the zero subqueue, and only deletes the queue metadata
// after all subqueues are empty.
func TestScavengerDeletesExpiredTasksInAllSubqueues(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	taskMgr := p.NewMockTaskManager(controller)

	queueName := "multi-subqueue-tq"
	queueInfo := &p.PersistedTaskQueueInfo{
		Data: &persistencespb.TaskQueueInfo{
			NamespaceId:    uuid.NewString(),
			Name:           queueName,
			LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-72 * time.Hour)),
			// metadata lists two subqueues; expired tasks live in subqueue 1
			Subqueues: []*persistencespb.SubqueueInfo{{}, {}},
		},
		RangeID: 22,
	}
	fixture := &subqueueTaskFixture{tasks: make(map[int][]*persistencespb.AllocatedTaskInfo)}
	fixture.add(1, 1, true)
	fixture.add(1, 2, true)
	fixture.add(1, 3, true)

	queriedSubqueues := make(map[int]int)
	deletedQueues := make(map[string]bool)

	taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.ListTaskQueueRequest) (*p.ListTaskQueueResponse, error) {
			if req.PageToken == nil {
				return &p.ListTaskQueueResponse{Items: []*p.PersistedTaskQueueInfo{queueInfo}}, nil
			}
			return &p.ListTaskQueueResponse{}, nil
		}).AnyTimes()
	taskMgr.EXPECT().GetTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.GetTasksRequest) (*p.GetTasksResponse, error) {
			queriedSubqueues[req.Subqueue]++
			tasks := fixture.tasks[req.Subqueue]
			if len(tasks) > req.PageSize {
				tasks = tasks[:req.PageSize]
			}
			return &p.GetTasksResponse{Tasks: tasks}, nil
		}).AnyTimes()
	taskMgr.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.CompleteTasksLessThanRequest) (int, error) {
			remaining := make([]*persistencespb.AllocatedTaskInfo, 0, len(fixture.tasks[req.Subqueue]))
			deleted := 0
			for _, task := range fixture.tasks[req.Subqueue] {
				if task.GetTaskId() < req.ExclusiveMaxTaskID && deleted < req.Limit {
					deleted++
					continue
				}
				remaining = append(remaining, task)
			}
			fixture.tasks[req.Subqueue] = remaining
			return deleted, nil
		}).AnyTimes()
	taskMgr.EXPECT().DeleteTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.DeleteTaskQueueRequest) error {
			deletedQueues[req.TaskQueue.TaskQueueName] = true
			return nil
		}).AnyTimes()

	scvg := NewScavenger(taskMgr, metrics.NoopMetricsHandler, log.NewTestLogger())
	runScavengerAndWait(t, scvg)

	require.NotZero(t, queriedSubqueues[1],
		"scavenger never scanned subqueue 1 of a queue with two subqueues")
	require.Empty(t, fixture.tasks[1],
		"scavenger failed to delete expired tasks stored in subqueue 1")
	require.True(t, deletedQueues[queueName],
		"scavenger failed to delete an idle queue whose subqueues are all empty")
}

// TestScavengerKeepsQueueWithLiveTaskInNonZeroSubqueue verifies that a live
// task in any subqueue prevents the queue metadata from being deleted.
func TestScavengerKeepsQueueWithLiveTaskInNonZeroSubqueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	taskMgr := p.NewMockTaskManager(controller)

	queueName := "multi-subqueue-live-tq"
	queueInfo := &p.PersistedTaskQueueInfo{
		Data: &persistencespb.TaskQueueInfo{
			NamespaceId:    uuid.NewString(),
			Name:           queueName,
			LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-72 * time.Hour)),
			Subqueues:      []*persistencespb.SubqueueInfo{{}, {}},
		},
		RangeID: 22,
	}
	fixture := &subqueueTaskFixture{tasks: make(map[int][]*persistencespb.AllocatedTaskInfo)}
	fixture.add(1, 1, true)
	fixture.add(1, 2, false) // live task keeps the queue alive

	deletedQueues := make(map[string]bool)

	taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.ListTaskQueueRequest) (*p.ListTaskQueueResponse, error) {
			if req.PageToken == nil {
				return &p.ListTaskQueueResponse{Items: []*p.PersistedTaskQueueInfo{queueInfo}}, nil
			}
			return &p.ListTaskQueueResponse{}, nil
		}).AnyTimes()
	taskMgr.EXPECT().GetTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.GetTasksRequest) (*p.GetTasksResponse, error) {
			tasks := fixture.tasks[req.Subqueue]
			if len(tasks) > req.PageSize {
				tasks = tasks[:req.PageSize]
			}
			return &p.GetTasksResponse{Tasks: tasks}, nil
		}).AnyTimes()
	taskMgr.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.CompleteTasksLessThanRequest) (int, error) {
			remaining := make([]*persistencespb.AllocatedTaskInfo, 0, len(fixture.tasks[req.Subqueue]))
			deleted := 0
			for _, task := range fixture.tasks[req.Subqueue] {
				if task.GetTaskId() < req.ExclusiveMaxTaskID && deleted < req.Limit {
					deleted++
					continue
				}
				remaining = append(remaining, task)
			}
			fixture.tasks[req.Subqueue] = remaining
			return deleted, nil
		}).AnyTimes()
	taskMgr.EXPECT().DeleteTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.DeleteTaskQueueRequest) error {
			deletedQueues[req.TaskQueue.TaskQueueName] = true
			return nil
		}).AnyTimes()

	scvg := NewScavenger(taskMgr, metrics.NoopMetricsHandler, log.NewTestLogger())
	runScavengerAndWait(t, scvg)

	require.False(t, deletedQueues[queueName],
		"scavenger deleted queue metadata while a live task was still present in subqueue 1")
	require.Len(t, fixture.tasks[1], 2,
		"scavenger must not complete past a live task in the batch")
}

func runScavengerAndWait(t *testing.T, scvg *Scavenger) {
	t.Helper()
	// speed up the executor drain wait, same as ScavengerTestSuite
	origPollInterval := executorPollInterval
	executorPollInterval = 10 * time.Millisecond
	defer func() { executorPollInterval = origPollInterval }()

	scvg.Start()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-scvg.stopC:
		return
	case <-timer.C:
		t.Fatal("timed out waiting for scavenger to finish")
	}
}
