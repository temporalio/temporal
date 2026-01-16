package mongodb_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb"
)

type TaskStoreSuite struct {
	suite.Suite
	factory *mongodb.Factory
	store   persistence.TaskStore
	ctx     context.Context
	cancel  context.CancelFunc
	dbName  string
}

func TestTaskStoreSuite(t *testing.T) {
	suite.Run(t, new(TaskStoreSuite))
}

func (s *TaskStoreSuite) SetupSuite() {
	s.dbName = fmt.Sprintf("temporal_test_tasks_%d", time.Now().UnixNano())
	cfg := newMongoTestConfig(s.dbName)
	cfg.ConnectTimeout = 10 * time.Second

	logger := log.NewTestLogger()
	var err error
	s.factory, err = mongodb.NewFactory(cfg, "test-cluster", logger, metrics.NoopMetricsHandler)
	if err != nil {
		s.T().Skipf("Skipping TaskStoreSuite: %v", err)
	}
	s.Require().NoError(err)

	s.store, err = s.factory.NewTaskStore()
	s.Require().NoError(err)
}

func (s *TaskStoreSuite) TearDownSuite() {
	if s.factory != nil {
		s.factory.Close()
	}
}

func (s *TaskStoreSuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second)
}

func (s *TaskStoreSuite) TearDownTest() {
	s.cancel()
}

func (s *TaskStoreSuite) TestTaskQueueLifecycle() {
	info := persistence.NewDataBlob([]byte("queue-info"), enumspb.ENCODING_TYPE_PROTO3.String())
	err := s.store.CreateTaskQueue(s.ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "namespace",
		TaskQueue:     "queue",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: info,
	})
	s.Require().NoError(err)

	resp, err := s.store.GetTaskQueue(s.ctx, &persistence.InternalGetTaskQueueRequest{
		NamespaceID: "namespace",
		TaskQueue:   "queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	s.Require().NoError(err)
	s.Require().Equal(int64(1), resp.RangeID)

	info2 := persistence.NewDataBlob([]byte("queue-info-2"), enumspb.ENCODING_TYPE_PROTO3.String())
	_, err = s.store.UpdateTaskQueue(s.ctx, &persistence.InternalUpdateTaskQueueRequest{
		NamespaceID:   "namespace",
		TaskQueue:     "queue",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       2,
		PrevRangeID:   1,
		TaskQueueInfo: info2,
	})
	s.Require().NoError(err)

	listResp, err := s.store.ListTaskQueue(s.ctx, &persistence.ListTaskQueueRequest{PageSize: 10})
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(len(listResp.Items), 1)

	err = s.store.DeleteTaskQueue(s.ctx, &persistence.DeleteTaskQueueRequest{
		TaskQueue: &persistence.TaskQueueKey{
			NamespaceID:   "namespace",
			TaskQueueName: "queue",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
		RangeID: 2,
	})
	s.Require().NoError(err)
}

func (s *TaskStoreSuite) TestCreateAndGetTasks() {
	info := persistence.NewDataBlob([]byte("queue-info"), enumspb.ENCODING_TYPE_PROTO3.String())
	err := s.store.CreateTaskQueue(s.ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "ns-tasks",
		TaskQueue:     "queue-tasks",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       10,
		TaskQueueInfo: info,
	})
	s.Require().NoError(err)

	tasks := []*persistence.InternalCreateTask{
		{
			TaskId:   1,
			Task:     persistence.NewDataBlob([]byte("task-1"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
		{
			TaskId:   2,
			Task:     persistence.NewDataBlob([]byte("task-2"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
	}

	_, err = s.store.CreateTasks(s.ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID:   "ns-tasks",
		TaskQueue:     "queue-tasks",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       10,
		TaskQueueInfo: info,
		Tasks:         tasks,
	})
	s.Require().NoError(err)

	resp, err := s.store.GetTasks(s.ctx, &persistence.GetTasksRequest{
		NamespaceID:        "ns-tasks",
		TaskQueue:          "queue-tasks",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: 100,
		Subqueue:           persistence.SubqueueZero,
		PageSize:           10,
		UseLimit:           true,
	})
	s.Require().NoError(err)
	s.Require().Len(resp.Tasks, 2)

	count, err := s.store.CompleteTasksLessThan(s.ctx, &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        "ns-tasks",
		TaskQueueName:      "queue-tasks",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ExclusiveMaxTaskID: 2,
		Subqueue:           persistence.SubqueueZero,
	})
	s.Require().NoError(err)
	s.Require().Positive(count)
}

func (s *TaskStoreSuite) TestCreateTasksWriteFencing() {
	info := persistence.NewDataBlob([]byte("queue-info"), enumspb.ENCODING_TYPE_PROTO3.String())
	queueName := "queue-tasks-fence"
	namespace := "ns-tasks-fence"
	rangeID := int64(50)

	err := s.store.CreateTaskQueue(s.ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   namespace,
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       rangeID,
		TaskQueueInfo: info,
	})
	s.Require().NoError(err)

	tasks := []*persistence.InternalCreateTask{
		{
			TaskId:   1,
			Task:     persistence.NewDataBlob([]byte("task-1"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
		{
			TaskId:   2,
			Task:     persistence.NewDataBlob([]byte("task-2"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
	}

	_, err = s.store.CreateTasks(s.ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID:   namespace,
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       rangeID,
		TaskQueueInfo: info,
		Tasks:         tasks,
	})
	s.Require().NoError(err)

	// RangeID mismatch should fail atomically and not write tasks
	_, err = s.store.CreateTasks(s.ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID:   namespace,
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       rangeID + 1,
		TaskQueueInfo: info,
		Tasks: []*persistence.InternalCreateTask{
			{
				TaskId:   3,
				Task:     persistence.NewDataBlob([]byte("task-3"), enumspb.ENCODING_TYPE_PROTO3.String()),
				Subqueue: persistence.SubqueueZero,
			},
		},
	})
	s.Require().Error(err)
	var cfe *persistence.ConditionFailedError
	s.Require().ErrorAs(err, &cfe)

	resp, err := s.store.GetTasks(s.ctx, &persistence.GetTasksRequest{
		NamespaceID:        namespace,
		TaskQueue:          queueName,
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: 100,
		Subqueue:           persistence.SubqueueZero,
		PageSize:           10,
		UseLimit:           true,
	})
	s.Require().NoError(err)
	s.Require().Len(resp.Tasks, 2)
}

func TestCreateTasks(t *testing.T) {
	dbName := fmt.Sprintf("temporal_test_tasks_%d", time.Now().UnixNano())
	cfg := newMongoTestConfig(dbName)
	cfg.ConnectTimeout = 10 * time.Second

	logger := log.NewTestLogger()
	factory, err := mongodb.NewFactory(cfg, "test-cluster", logger, metrics.NoopMetricsHandler)
	if err != nil {
		t.Skipf("Skipping TestCreateTasks: %v", err)
	}
	t.Cleanup(factory.Close)

	store, err := factory.NewTaskStore()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	info := persistence.NewDataBlob([]byte("queue-info"), enumspb.ENCODING_TYPE_PROTO3.String())
	queueName := "queue-tasks-fence-standalone"
	namespace := "ns-tasks-fence-standalone"
	rangeID := int64(60)

	err = store.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   namespace,
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       rangeID,
		TaskQueueInfo: info,
	})
	require.NoError(t, err)

	tasks := []*persistence.InternalCreateTask{
		{
			TaskId:   1,
			Task:     persistence.NewDataBlob([]byte("task-1"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
		{
			TaskId:   2,
			Task:     persistence.NewDataBlob([]byte("task-2"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
	}

	_, err = store.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID:   namespace,
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       rangeID,
		TaskQueueInfo: info,
		Tasks:         tasks,
	})
	require.NoError(t, err)

	// RangeID mismatch should fail atomically and not write tasks
	_, err = store.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID:   namespace,
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       rangeID + 1,
		TaskQueueInfo: info,
		Tasks: []*persistence.InternalCreateTask{
			{
				TaskId:   3,
				Task:     persistence.NewDataBlob([]byte("task-3"), enumspb.ENCODING_TYPE_PROTO3.String()),
				Subqueue: persistence.SubqueueZero,
			},
		},
	})
	require.Error(t, err)
	var cfe *persistence.ConditionFailedError
	require.ErrorAs(t, err, &cfe)

	resp, err := store.GetTasks(ctx, &persistence.GetTasksRequest{
		NamespaceID:        namespace,
		TaskQueue:          queueName,
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: 100,
		Subqueue:           persistence.SubqueueZero,
		PageSize:           10,
		UseLimit:           true,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 2)
}

func (s *TaskStoreSuite) TestUserDataUpdates() {
	queueName := "user-data-queue"
	// initial create ensures namespace exists
	info := persistence.NewDataBlob([]byte("qd"), enumspb.ENCODING_TYPE_PROTO3.String())
	err := s.store.CreateTaskQueue(s.ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "ns-user",
		TaskQueue:     queueName,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       5,
		TaskQueueInfo: info,
	})
	s.Require().NoError(err)

	applied := false
	conflicting := false
	update := &persistence.InternalSingleTaskQueueUserDataUpdate{
		Version:     0,
		UserData:    persistence.NewDataBlob([]byte("user-data"), enumspb.ENCODING_TYPE_PROTO3.String()),
		Applied:     &applied,
		Conflicting: &conflicting,
	}
	err = s.store.UpdateTaskQueueUserData(s.ctx, &persistence.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "ns-user",
		Updates: map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
			queueName: update,
		},
	})
	s.Require().NoError(err)
	s.Require().True(*update.Applied)
	s.Require().False(*update.Conflicting)

	resp, err := s.store.GetTaskQueueUserData(s.ctx, &persistence.GetTaskQueueUserDataRequest{
		NamespaceID: "ns-user",
		TaskQueue:   queueName,
	})
	s.Require().NoError(err)
	s.Require().Equal(int64(1), resp.Version)

	entries, err := s.store.ListTaskQueueUserDataEntries(s.ctx, &persistence.ListTaskQueueUserDataEntriesRequest{
		NamespaceID: "ns-user",
		PageSize:    10,
	})
	s.Require().NoError(err)
	s.Require().Len(entries.Entries, 1)
}

func (s *TaskStoreSuite) TestBuildIDMapping() {
	const (
		namespace = "ns-build"
		buildID   = "build-1"
	)

	queueOne := "build-queue-1"
	queueTwo := "build-queue-2"

	info := persistence.NewDataBlob([]byte("qd"), enumspb.ENCODING_TYPE_PROTO3.String())
	for _, queue := range []string{queueOne, queueTwo} {
		err := s.store.CreateTaskQueue(s.ctx, &persistence.InternalCreateTaskQueueRequest{
			NamespaceID:   namespace,
			TaskQueue:     queue,
			TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			RangeID:       10,
			TaskQueueInfo: info,
		})
		s.Require().NoError(err)

		err = s.store.UpdateTaskQueueUserData(s.ctx, &persistence.InternalUpdateTaskQueueUserDataRequest{
			NamespaceID: namespace,
			Updates: map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
				queue: {
					Version:       0,
					UserData:      persistence.NewDataBlob([]byte("data"), enumspb.ENCODING_TYPE_PROTO3.String()),
					BuildIdsAdded: []string{buildID},
				},
			},
		})
		s.Require().NoError(err)
	}

	queues, err := s.store.GetTaskQueuesByBuildId(s.ctx, &persistence.GetTaskQueuesByBuildIdRequest{
		NamespaceID: namespace,
		BuildID:     buildID,
	})
	s.Require().NoError(err)
	s.Require().ElementsMatch([]string{queueOne, queueTwo}, queues)

	count, err := s.store.CountTaskQueuesByBuildId(s.ctx, &persistence.CountTaskQueuesByBuildIdRequest{
		NamespaceID: namespace,
		BuildID:     buildID,
	})
	s.Require().NoError(err)
	s.Require().Equal(2, count)

	resp, err := s.store.GetTaskQueueUserData(s.ctx, &persistence.GetTaskQueueUserDataRequest{
		NamespaceID: namespace,
		TaskQueue:   queueOne,
	})
	s.Require().NoError(err)

	err = s.store.UpdateTaskQueueUserData(s.ctx, &persistence.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: namespace,
		Updates: map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
			queueOne: {
				Version:         resp.Version,
				UserData:        persistence.NewDataBlob([]byte("data"), enumspb.ENCODING_TYPE_PROTO3.String()),
				BuildIdsRemoved: []string{buildID},
			},
		},
	})
	s.Require().NoError(err)

	queues, err = s.store.GetTaskQueuesByBuildId(s.ctx, &persistence.GetTaskQueuesByBuildIdRequest{
		NamespaceID: namespace,
		BuildID:     buildID,
	})
	s.Require().NoError(err)
	s.Require().Equal([]string{queueTwo}, queues)

	count, err = s.store.CountTaskQueuesByBuildId(s.ctx, &persistence.CountTaskQueuesByBuildIdRequest{
		NamespaceID: namespace,
		BuildID:     buildID,
	})
	s.Require().NoError(err)
	s.Require().Equal(1, count)
}

func (s *TaskStoreSuite) TestFairTaskQueues() {
	fairStore, err := s.factory.NewFairTaskStore()
	s.Require().NoError(err)

	info := persistence.NewDataBlob([]byte("queue-info"), enumspb.ENCODING_TYPE_PROTO3.String())
	err = fairStore.CreateTaskQueue(s.ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "ns-fair",
		TaskQueue:     "queue-fair",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       20,
		TaskQueueInfo: info,
	})
	s.Require().NoError(err)

	tasks := []*persistence.InternalCreateTask{
		{
			TaskPass: 1,
			TaskId:   1,
			Task:     persistence.NewDataBlob([]byte("fair-task-1"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
		{
			TaskPass: 1,
			TaskId:   2,
			Task:     persistence.NewDataBlob([]byte("fair-task-2"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
		{
			TaskPass: 2,
			TaskId:   1,
			Task:     persistence.NewDataBlob([]byte("fair-task-3"), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		},
	}

	_, err = fairStore.CreateTasks(s.ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID:   "ns-fair",
		TaskQueue:     "queue-fair",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       20,
		TaskQueueInfo: info,
		Tasks:         tasks,
	})
	s.Require().NoError(err)

	resp, err := fairStore.GetTasks(s.ctx, &persistence.GetTasksRequest{
		NamespaceID:        "ns-fair",
		TaskQueue:          "queue-fair",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: math.MaxInt64,
		Subqueue:           persistence.SubqueueZero,
		PageSize:           2,
		UseLimit:           true,
	})
	s.Require().NoError(err)
	s.Require().Len(resp.Tasks, 2)
	s.Require().NotEmpty(resp.NextPageToken)

	resp2, err := fairStore.GetTasks(s.ctx, &persistence.GetTasksRequest{
		NamespaceID:        "ns-fair",
		TaskQueue:          "queue-fair",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: math.MaxInt64,
		Subqueue:           persistence.SubqueueZero,
		PageSize:           2,
		NextPageToken:      resp.NextPageToken,
	})
	s.Require().NoError(err)
	s.Require().Len(resp2.Tasks, 1)

	count, err := fairStore.CompleteTasksLessThan(s.ctx, &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        "ns-fair",
		TaskQueueName:      "queue-fair",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ExclusiveMaxPass:   2,
		ExclusiveMaxTaskID: math.MaxInt64,
		Subqueue:           persistence.SubqueueZero,
	})
	s.Require().NoError(err)
	s.Require().Positive(count)
}
