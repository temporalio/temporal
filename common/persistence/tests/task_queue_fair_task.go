package tests

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	TaskQueueFairTaskSuite struct {
		suite.Suite
		*require.Assertions

		stickyTTL time.Duration
		taskTTL   time.Duration

		namespaceID   string
		taskQueueName string
		taskQueueType enumspb.TaskQueueType

		taskManager p.TaskManager
		logger      log.Logger

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func NewTaskQueueFairTaskSuite(
	t *testing.T,
	taskStore p.TaskStore,
	logger log.Logger,
) *TaskQueueFairTaskSuite {
	return &TaskQueueFairTaskSuite{
		Assertions: require.New(t),
		taskManager: p.NewTaskManager(
			taskStore,
			serialization.NewSerializer(),
		),
		logger: logger,
	}
}

func (s *TaskQueueFairTaskSuite) SetupSuite() {
}

func (s *TaskQueueFairTaskSuite) TearDownSuite() {
}

func (s *TaskQueueFairTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.stickyTTL = time.Second * 10
	s.taskTTL = time.Second * 16
	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TaskQueueType(rand.Int31n(
		int32(len(enumspb.TaskQueueType_name)) + 1),
	)
}

func (s *TaskQueueFairTaskSuite) TearDownTest() {
	s.cancel()
}

func (s *TaskQueueFairTaskSuite) TestCreateGet_Order() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	tasks := []*persistencespb.AllocatedTaskInfo{
		s.randomTask(10, 2), // higher pass
		s.randomTask(20, 1), // lower pass, higher id
		s.randomTask(5, 1),  // same pass, lower id
		s.randomTask(15, 2), // same pass as first but lower id
	}

	_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
		TaskQueueInfo: &p.PersistedTaskQueueInfo{RangeID: rangeID, Data: taskQueue},
		Tasks:         tasks,
	})
	s.NoError(err)

	resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           10,
	})
	s.NoError(err)

	expected := []*persistencespb.AllocatedTaskInfo{tasks[2], tasks[1], tasks[0], tasks[3]}
	protorequire.ProtoSliceEqual(s.T(), expected, resp.Tasks)
	s.Nil(resp.NextPageToken)
}

func (s *TaskQueueFairTaskSuite) TestCreateDelete_Range() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	var tasks []*persistencespb.AllocatedTaskInfo
	for pass := int64(1); pass <= 5; pass++ {
		tasks = append(tasks, s.randomTask(pass, pass))
	}
	_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
		TaskQueueInfo: &p.PersistedTaskQueueInfo{RangeID: rangeID, Data: taskQueue},
		Tasks:         tasks,
	})
	s.NoError(err)

	_, err = s.taskManager.CompleteTasksLessThan(s.ctx, &p.CompleteTasksLessThanRequest{
		NamespaceID:        s.namespaceID,
		TaskQueueName:      s.taskQueueName,
		TaskType:           s.taskQueueType,
		ExclusiveMaxPass:   3,
		ExclusiveMaxTaskID: 0,
		Limit:              10,
	})
	s.NoError(err)

	resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           10,
	})
	s.NoError(err)

	expected := []*persistencespb.AllocatedTaskInfo{tasks[2], tasks[3], tasks[4]}
	protorequire.ProtoSliceEqual(s.T(), expected, resp.Tasks)
	s.Nil(resp.NextPageToken)
}

func (s *TaskQueueFairTaskSuite) createTaskQueue(rangeID int64) *persistencespb.TaskQueueInfo {
	taskQueueKind := enumspb.TaskQueueKind(rand.Int31n(int32(len(enumspb.TaskQueueKind_name)) + 1))
	taskQueue := s.randomTaskQueueInfo(taskQueueKind)
	_, err := s.taskManager.CreateTaskQueue(s.ctx, &p.CreateTaskQueueRequest{
		RangeID:       rangeID,
		TaskQueueInfo: taskQueue,
	})
	s.NoError(err)
	return taskQueue
}

func (s *TaskQueueFairTaskSuite) randomTaskQueueInfo(taskQueueKind enumspb.TaskQueueKind) *persistencespb.TaskQueueInfo {
	now := time.Now().UTC()
	var expiryTime *timestamppb.Timestamp
	if taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		expiryTime = timestamppb.New(now.Add(s.stickyTTL))
	}
	return &persistencespb.TaskQueueInfo{
		NamespaceId:    s.namespaceID,
		Name:           s.taskQueueName,
		TaskType:       s.taskQueueType,
		Kind:           taskQueueKind,
		AckLevel:       rand.Int63(),
		ExpiryTime:     expiryTime,
		LastUpdateTime: timestamppb.New(now),
	}
}

func (s *TaskQueueFairTaskSuite) randomTask(taskID, pass int64) *persistencespb.AllocatedTaskInfo {
	now := time.Now().UTC()
	return &persistencespb.AllocatedTaskInfo{
		TaskId:   taskID,
		TaskPass: pass,
		Data: &persistencespb.TaskInfo{
			NamespaceId:      s.namespaceID,
			WorkflowId:       uuid.New().String(),
			RunId:            uuid.New().String(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamppb.New(now),
			ExpiryTime:       timestamppb.New(now.Add(s.taskTTL)),
			Clock: &clockspb.VectorClock{
				ClusterId: rand.Int63(),
				ShardId:   rand.Int31(),
				Clock:     rand.Int63(),
			},
		},
	}
}
