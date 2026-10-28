package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	ScavengerTestSuite struct {
		suite.Suite

		controller *gomock.Controller
		taskMgr    *p.MockTaskManager

		taskQueueTable *mockTaskQueueTable
		taskTables     map[string]*mockTaskTable
		scvgr          *Scavenger
	}
)

var errTest = errors.New("transient error")

func TestScavengerTestSuite(t *testing.T) {
	suite.Run(t, new(ScavengerTestSuite))
}

func (s *ScavengerTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.taskMgr = p.NewMockTaskManager(s.controller)
	s.taskQueueTable = &mockTaskQueueTable{}
	s.taskTables = make(map[string]*mockTaskTable)
	logger := log.NewTestLogger()
	s.scvgr = NewScavenger(s.taskMgr, metrics.NoopMetricsHandler, logger)
	maxTasksPerJob = 4
	executorPollInterval = time.Millisecond * 50
}

func (s *ScavengerTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ScavengerTestSuite) TestAllExpiredTasks() {
	nTasks := 32
	nTaskQueues := 3
	for i := range nTaskQueues {
		name := fmt.Sprintf("test-expired-tq-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Empty(tasks, "failed to delete all expired tasks")
		s.Nil(s.taskQueueTable.get(tl), "failed to delete expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestAllAliveTasks() {
	nTasks := 32
	nTaskQueues := 3
	for i := range nTaskQueues {
		name := fmt.Sprintf("test-Alive-tq-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, false)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Len(tasks, nTasks, "scavenger deleted a non-expired executorTask")
		s.NotNil(s.taskQueueTable.get(tl), "scavenger deleted a non-expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestExpiredTasksFollowedByAlive() {
	nTasks := 32
	nTaskQueues := 3
	for i := range nTaskQueues {
		name := fmt.Sprintf("test-Alive-tq-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks/2, true)
		tt.generate(nTasks/2, false)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Len(tasks, nTasks/2, "scavenger deleted non-expired tasks")
		s.Equal(int64(nTasks/2), tasks[0].GetTaskId(), "scavenger deleted wrong set of tasks")
		s.NotNil(s.taskQueueTable.get(tl), "scavenger deleted a non-expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestAliveTasksFollowedByExpired() {
	nTasks := 32
	nTaskQueues := 3
	for i := range nTaskQueues {
		name := fmt.Sprintf("test-Alive-tl-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks/2, false)
		tt.generate(nTasks/2, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Len(tasks, nTasks, "scavenger deleted non-expired tasks")
		s.NotNil(s.taskQueueTable.get(tl), "scavenger deleted a non-expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestAllExpiredTasksWithErrors() {
	nTasks := 32
	nTaskQueues := 3
	for i := range nTaskQueues {
		name := fmt.Sprintf("test-expired-tl-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocksWithErrors()
	s.runScavenger()
	for _, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Empty(tasks, "failed to delete all expired tasks")
	}
	result, _ := s.taskQueueTable.list(nil, 10)
	s.Len(result, 1, "expected partial deletion due to transient errors")
}

func (s *ScavengerTestSuite) runScavenger() {
	s.scvgr.Start()
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-s.scvgr.stopC:
		timer.Stop()
		return
	case <-timer.C:
		s.Fail("timed out waiting for scavenger to finish")
	}
}

func (s *ScavengerTestSuite) setupTaskMgrMocks() {
	s.taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.ListTaskQueueRequest) (*p.ListTaskQueueResponse, error) {
			items, next := s.taskQueueTable.list(req.PageToken, req.PageSize)
			return &p.ListTaskQueueResponse{Items: items, NextPageToken: next}, nil
		}).AnyTimes()
	s.taskMgr.EXPECT().DeleteTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.DeleteTaskQueueRequest) error {
			s.taskQueueTable.delete(req.TaskQueue.TaskQueueName)
			return nil
		}).AnyTimes()
	s.taskMgr.EXPECT().GetTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.GetTasksRequest) (*p.GetTasksResponse, error) {
			result := s.taskTables[req.TaskQueue].get(req.PageSize)
			return &p.GetTasksResponse{Tasks: result}, nil
		}).AnyTimes()
	s.taskMgr.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.CompleteTasksLessThanRequest) (int, error) {
			return s.taskTables[req.TaskQueueName].deleteLessThan(req.ExclusiveMaxTaskID, req.Limit), nil
		}).AnyTimes()
}

func (s *ScavengerTestSuite) setupTaskMgrMocksWithErrors() {
	s.taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).Return(nil, errTest)
	s.taskMgr.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(nil, errTest)
	s.taskMgr.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).Return(0, errTest)
	s.taskMgr.EXPECT().DeleteTaskQueue(gomock.Any(), gomock.Any()).Return(errTest)
	s.setupTaskMgrMocks()
}

// The existing scanner suite mutates package-level job limits.
//
//parallelize:ignore
func TestSingleSessionMetadataCleanup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		assigned bool
		kind     enumspb.TaskQueueKind
		taskType enumspb.TaskQueueType
		retained bool
	}{
		{name: "assigned workflow sticky", assigned: true, kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, retained: true},
		{name: "unassigned workflow sticky", kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		{name: "activity sticky", assigned: true, kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		{name: "normal workflow", assigned: true, kind: enumspb.TASK_QUEUE_KIND_NORMAL, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, expiredTask := range []bool{false, true} {
				name := "empty queue"
				if expiredTask {
					name = "expired task"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					tv := testvars.New(t)
					manager := p.NewMockTaskManager(gomock.NewController(t))
					scavenger := NewScavenger(manager, metrics.NoopMetricsHandler, log.NewTestLogger())
					t.Cleanup(scavenger.lifecycleCancel)
					info := &p.PersistedTaskQueueInfo{
						RangeID: 7,
						Data: &persistencespb.TaskQueueInfo{
							NamespaceId: tv.NamespaceID().String(), Name: tv.StickyTaskQueue().GetName(),
							TaskType: tc.taskType, Kind: tc.kind, SingleSessionAssigned: tc.assigned,
							LastUpdateTime: timestamppb.New(time.Now().Add(-72 * time.Hour)),
						},
					}
					response := &p.GetTasksResponse{}
					if expiredTask {
						response.Tasks = []*persistencespb.AllocatedTaskInfo{{TaskId: 1, Data: &persistencespb.TaskInfo{ExpiryTime: timestamppb.New(time.Now().Add(-time.Hour))}}}
						manager.EXPECT().CompleteTasksLessThan(gomock.Any(), &p.CompleteTasksLessThanRequest{
							NamespaceID: info.Data.GetNamespaceId(), TaskQueueName: info.Data.GetName(), TaskType: tc.taskType,
							ExclusiveMaxTaskID: 2, Limit: 1,
						}).Return(1, nil)
					}
					manager.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(response, nil)
					if !tc.retained {
						manager.EXPECT().DeleteTaskQueue(gomock.Any(), &p.DeleteTaskQueueRequest{
							TaskQueue: &p.TaskQueueKey{NamespaceID: info.Data.GetNamespaceId(), TaskQueueName: info.Data.GetName(), TaskQueueType: tc.taskType},
							RangeID:   7,
						}).Return(nil)
					}
					require.Equal(t, handlerStatusDone, scavenger.newTask(info).Run())
					var expectedDeleted int64
					if !tc.retained {
						expectedDeleted = 1
					}
					require.Equal(t, expectedDeleted, scavenger.stats.taskqueue.nDeleted)
					require.Equal(t, int64(len(response.Tasks)), scavenger.stats.task.nDeleted)
				})
			}
		})
	}
}
