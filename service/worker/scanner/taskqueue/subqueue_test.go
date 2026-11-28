package taskqueue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
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

// reviewerScenarioFixture builds the backlog shape from the review of #12875:
// with 17 subqueues, each subqueue from 0 to 15 holds 15 expired tasks followed
// by one live task, so a scan starting at subqueue 0 has to inspect 16 tasks per
// subqueue before it can move on, and subqueue 16 (which holds one expired task)
// is only reachable once 256 tasks have been inspected.
func reviewerScenarioFixture() *subqueueTaskFixture {
	fixture := &subqueueTaskFixture{tasks: make(map[int][]*persistencespb.AllocatedTaskInfo)}
	for subqueue := 0; subqueue < 16; subqueue++ {
		for taskID := int64(1); taskID <= 15; taskID++ {
			fixture.add(subqueue, taskID, true)
		}
		fixture.add(subqueue, 16, false)
	}
	fixture.add(16, 1, true)
	return fixture
}

// wireSubqueueMocks wires a subqueue aware task manager: reads and deletes are
// scoped to the requested subqueue, tasks are returned in task ID order, and
// deletions are recorded per task queue.
func wireSubqueueMocks(
	taskMgr *p.MockTaskManager,
	fixture *subqueueTaskFixture,
	queriedSubqueues map[int]int,
	deletedQueues map[string]bool,
) {
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
}

// TestScavengerMakesProgressWithLiveTaskInEverySubqueue covers the no-progress
// case reported on #12875: when every subqueue starts with a batch that contains
// a live task, a scan that charges the maxTasksPerJob budget for inspections
// spends the whole budget without deleting anything, defers, and then starts
// over from subqueue 0 on the next run, so the expired tasks behind those live
// tasks (here: subqueue 16) are never deleted. The handler is called repeatedly
// with the same state, which is what the executor does when a task is deferred.
func TestScavengerMakesProgressWithLiveTaskInEverySubqueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	origMaxTasksPerJob := maxTasksPerJob
	maxTasksPerJob = 256 // the reviewer scenario depends on the production value
	defer func() { maxTasksPerJob = origMaxTasksPerJob }()

	taskMgr := p.NewMockTaskManager(controller)
	fixture := reviewerScenarioFixture()
	queriedSubqueues := make(map[int]int)
	deletedQueues := make(map[string]bool)
	wireSubqueueMocks(taskMgr, fixture, queriedSubqueues, deletedQueues)

	scvg := NewScavenger(taskMgr, metrics.NoopMetricsHandler, log.NewTestLogger())
	key := &p.TaskQueueKey{
		NamespaceID:   uuid.NewString(),
		TaskQueueName: "multi-subqueue-live-tq",
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}
	state := &taskQueueState{
		rangeID:     22,
		lastUpdated: time.Now().UTC().Add(-72 * time.Hour),
		subqueues:   17,
	}

	status := handlerStatusDefer
	for i := 0; i < 4 && status == handlerStatusDefer; i++ {
		status = scvg.deleteHandler(key, state)
	}

	require.Equal(t, handlerStatusDone, status,
		"scan kept deferring instead of reaching the last subqueue")
	require.NotZero(t, queriedSubqueues[16], "scavenger never scanned the last subqueue")
	require.Empty(t, fixture.tasks[16], "scavenger failed to delete the expired task in the last subqueue")
	require.Len(t, fixture.tasks[0], 16, "scavenger completed past a live task")
	require.False(t, deletedQueues[key.TaskQueueName],
		"scavenger deleted queue metadata while a live task was still present")
}

// TestScavengerPassCompletesWithLiveTaskInEverySubqueue runs the whole scavenger
// against the same backlog: a deferred scan that makes no progress keeps the
// executor's task outstanding, so the pass never completes.
func TestScavengerPassCompletesWithLiveTaskInEverySubqueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	origMaxTasksPerJob := maxTasksPerJob
	maxTasksPerJob = 256 // the reviewer scenario depends on the production value
	defer func() { maxTasksPerJob = origMaxTasksPerJob }()

	taskMgr := p.NewMockTaskManager(controller)
	fixture := reviewerScenarioFixture()
	queriedSubqueues := make(map[int]int)
	deletedQueues := make(map[string]bool)
	wireSubqueueMocks(taskMgr, fixture, queriedSubqueues, deletedQueues)

	queueName := "multi-subqueue-live-tq"
	queueInfo := &p.PersistedTaskQueueInfo{
		Data: &persistencespb.TaskQueueInfo{
			NamespaceId:    uuid.NewString(),
			Name:           queueName,
			TaskType:       enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastUpdateTime: timestamppb.New(time.Now().UTC().Add(-72 * time.Hour)),
			Subqueues:      make([]*persistencespb.SubqueueInfo, 17),
		},
		RangeID: 22,
	}
	taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.ListTaskQueueRequest) (*p.ListTaskQueueResponse, error) {
			if req.PageToken == nil {
				return &p.ListTaskQueueResponse{Items: []*p.PersistedTaskQueueInfo{queueInfo}}, nil
			}
			return &p.ListTaskQueueResponse{}, nil
		}).AnyTimes()

	scvg := NewScavenger(taskMgr, metrics.NoopMetricsHandler, log.NewTestLogger())
	runScavengerAndWait(t, scvg)

	require.NotZero(t, queriedSubqueues[16], "scavenger pass ended before scanning the last subqueue")
	require.Empty(t, fixture.tasks[16], "scavenger pass ended without deleting the expired task in the last subqueue")
	require.False(t, deletedQueues[queueName],
		"scavenger deleted queue metadata while a live task was still present")
}
