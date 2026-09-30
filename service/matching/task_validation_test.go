package matching

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	taskValidatorSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata
		historyClient   *historyservicemock.MockHistoryServiceClient
		namespaceCache  *namespace.MockRegistry

		namespaceID     string
		workflowID      string
		runID           string
		scheduleEventID int64
		task            *persistencespb.AllocatedTaskInfo

		taskValidator *taskValidatorImpl
	}
)

func TestTaskValidatorSuite(t *testing.T) {
	s := new(taskValidatorSuite)
	suite.Run(t, s)
}

func (s *taskValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)

	s.namespaceID = uuid.New().String()
	s.workflowID = uuid.New().String()
	s.runID = uuid.New().String()
	s.scheduleEventID = rand.Int63()
	s.task = &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			NamespaceId:      s.namespaceID,
			WorkflowId:       s.workflowID,
			RunId:            s.runID,
			ScheduledEventId: s.scheduleEventID,
			CreateTime:       timestamp.TimeNowPtrUtc(),
			Stamp:            rand.Int31(),
		},
	}

	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily(s.namespaceID, "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		NewConfig(dynamicconfig.NewNoopCollection()), "nsname",
	)
	s.taskValidator = newTaskValidator(context.Background(), cfg, s.clusterMetadata, s.namespaceCache, s.historyClient)
}

func (s *taskValidatorSuite) putCache(info taskValidationInfo) {
	s.taskValidator.mu.Lock()
	defer s.taskValidator.mu.Unlock()
	s.taskValidator.putLocked(info)
}

func (s *taskValidatorSuite) cacheInfo(taskID int64) (taskValidationInfo, bool) {
	s.taskValidator.mu.Lock()
	defer s.taskValidator.mu.Unlock()
	info, ok := s.taskValidator.cache[taskID]
	return info, ok
}

func (s *taskValidatorSuite) TestPreValidateActive_NewTask_Skip_WithCreationTime() {
	s.task.Data.CreateTime = timestamppb.New(time.Unix(0, rand.Int63()))

	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(s.task.TaskId, info.taskID)
	s.Equal(s.task.Data.CreateTime.AsTime(), info.validationTime)
}

func (s *taskValidatorSuite) TestPreValidateActive_NewTask_Skip_WithoutCreationTime() {
	s.task.Data.CreateTime = nil

	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(s.task.TaskId, info.taskID)
	s.Less(time.Since(info.validationTime), time.Second)
}

func (s *taskValidatorSuite) TestPreValidateActive_ExistingTask_Validate() {
	s.putCache(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(-s.taskValidator.config.ValidatorValidationThreshold() * 2),
	})
	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidateActive_ExistingTask_Skip() {
	s.putCache(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(s.taskValidator.config.ValidatorValidationThreshold() * 2),
	})
	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Skip_WithCreationTime() {
	s.task.Data.CreateTime = timestamppb.New(time.Now().Add(-s.taskValidator.config.ValidatorValidationThreshold() / 2))

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(s.task.TaskId, info.taskID)
	s.Equal(s.task.Data.CreateTime.AsTime(), info.validationTime)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Validate_WithCreationTime() {
	s.task.Data.CreateTime = timestamppb.New(time.Now().Add(-s.taskValidator.config.ValidatorValidationThreshold() * 2))

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.True(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(s.task.TaskId, info.taskID)
	s.Equal(s.task.Data.CreateTime.AsTime(), info.validationTime)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Skip_WithoutCreationTime() {
	s.task.Data.CreateTime = nil

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(s.task.TaskId, info.taskID)
	s.Less(time.Since(info.validationTime), time.Second)
}

func (s *taskValidatorSuite) TestPreValidatePassive_ExistingTask_Validate() {
	s.putCache(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(-s.taskValidator.config.ValidatorValidationThreshold() * 2),
	})
	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidatePassive_ExistingTask_Skip() {
	s.putCache(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(s.taskValidator.config.ValidatorValidationThreshold() * 2),
	})
	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestCache_TwoTaskIDsIndependent() {
	other := s.task.TaskId + 1
	s.putCache(taskValidationInfo{
		taskID:         other,
		validationTime: time.Now().Add(-s.taskValidator.config.ValidatorValidationThreshold() * 2),
	})
	s.task.Data.CreateTime = timestamppb.Now()

	s.False(s.taskValidator.preValidateActive(s.task), "first sight of this id must skip")
	s.True(s.taskValidator.preValidateActive(&persistencespb.AllocatedTaskInfo{
		TaskId: other,
		Data:   s.task.Data,
	}), "other id must still be past threshold")
}

func (s *taskValidatorSuite) TestCache_ConcurrentFirstSeen() {
	const n = 20
	shouldValidate := make([]bool, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			task := &persistencespb.AllocatedTaskInfo{
				TaskId: int64(i + 1),
				Data: &persistencespb.TaskInfo{
					CreateTime: timestamppb.Now(),
				},
			}
			shouldValidate[i] = s.taskValidator.preValidateActive(task)
		}(i)
	}
	wg.Wait()

	for i, got := range shouldValidate {
		s.False(got, "first sight of task %d must skip", i+1)
	}
	s.taskValidator.mu.Lock()
	defer s.taskValidator.mu.Unlock()
	s.Len(s.taskValidator.cache, n)
}

func (s *taskValidatorSuite) TestCache_EvictsLeastRecentlyAccessedWhenFull() {
	maxSize := s.taskValidator.config.ValidatorCacheMaxSize()
	now := time.Now()
	for i := range maxSize {
		s.putCache(taskValidationInfo{
			taskID:         int64(i + 1),
			validationTime: now.Add(-time.Duration(i) * time.Second),
		})
	}
	s.False(s.taskValidator.preValidateActive(&persistencespb.AllocatedTaskInfo{TaskId: 1}))
	newTask := &persistencespb.AllocatedTaskInfo{
		TaskId: int64(maxSize + 1),
		Data:   &persistencespb.TaskInfo{CreateTime: timestamppb.Now()},
	}
	s.False(s.taskValidator.preValidateActive(newTask))

	_, recentlyAccessedKept := s.cacheInfo(1)
	s.True(recentlyAccessedKept)
	_, leastRecentlyAccessedKept := s.cacheInfo(2)
	s.False(leastRecentlyAccessedKept, "least recently accessed entry must be evicted")
	_, newestKept := s.cacheInfo(int64(maxSize))
	s.True(newestKept)
	_, inserted := s.cacheInfo(newTask.TaskId)
	s.True(inserted)
	s.taskValidator.mu.Lock()
	defer s.taskValidator.mu.Unlock()
	s.Len(s.taskValidator.cache, maxSize)
}

func (s *taskValidatorSuite) TestCache_OldTasksValidateWhenFull() {
	maxSize := s.taskValidator.config.ValidatorCacheMaxSize()
	for id := int64(1); id <= int64(maxSize); id++ {
		s.taskValidator.postValidate(&persistencespb.AllocatedTaskInfo{TaskId: id})
	}
	tasks := []*persistencespb.AllocatedTaskInfo{
		{TaskId: int64(maxSize + 1), Data: &persistencespb.TaskInfo{CreateTime: timestamppb.New(time.Now().Add(-time.Hour))}},
		{TaskId: int64(maxSize + 2), Data: &persistencespb.TaskInfo{CreateTime: timestamppb.New(time.Now().Add(-time.Hour))}},
	}
	for _, task := range tasks {
		s.False(s.taskValidator.preValidateActive(task))
	}
	for _, task := range tasks {
		s.True(s.taskValidator.preValidateActive(task), "old task %d must validate on its second pass", task.TaskId)
	}
}

func TestTaskValidatorValidationInterval(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active bool
		age    time.Duration
		want   bool
	}{
		{name: "active within interval", active: true, age: 5 * time.Minute},
		{name: "active past interval", active: true, age: 11 * time.Minute, want: true},
		{name: "passive within interval", age: 5 * time.Minute},
		{name: "passive past interval", age: 11 * time.Minute, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newTaskQueueConfig(
				tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
				NewConfig(dynamicconfig.NewNoopCollection()), "nsname",
			)
			v := newTaskValidator(context.Background(), cfg, nil, nil, nil)
			task := &persistencespb.AllocatedTaskInfo{
				TaskId: 1,
				Data:   &persistencespb.TaskInfo{CreateTime: timestamppb.New(time.Now().Add(-tc.age))},
			}
			if tc.active {
				require.False(t, v.preValidateActive(task))
				require.Equal(t, tc.want, v.preValidateActive(task))
			} else {
				require.Equal(t, tc.want, v.preValidatePassive(task))
			}
		})
	}
}

func TestTaskValidatorDynamicValidationThreshold(t *testing.T) {
	for _, active := range []bool{true, false} {
		name := "passive"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := dynamicconfig.NewMemoryClient()
			cfg := newTaskQueueConfig(
				tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
				NewConfig(dynamicconfig.NewCollection(client, log.NewNoopLogger())), "nsname",
			)
			v := newTaskValidator(context.Background(), cfg, nil, nil, nil)
			task := &persistencespb.AllocatedTaskInfo{
				TaskId: 1,
				Data:   &persistencespb.TaskInfo{CreateTime: timestamppb.New(time.Now().Add(-5 * time.Minute))},
			}
			preValidate := v.preValidatePassive
			if active {
				preValidate = v.preValidateActive
			}
			require.False(t, preValidate(task))
			require.False(t, preValidate(task))
			for _, tc := range []struct {
				threshold time.Duration
				want      bool
			}{
				{threshold: time.Minute, want: true},
				{threshold: 20 * time.Minute},
				{threshold: 0, want: true},
			} {
				t.Cleanup(client.OverrideSetting(dynamicconfig.MatchingValidatorValidationThreshold, []dynamicconfig.ConstrainedValue{{
					Constraints: dynamicconfig.Constraints{Namespace: "nsname", TaskQueueName: "tq", TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
					Value:       tc.threshold,
				}}))
				require.Equal(t, tc.want, preValidate(task))
			}
		})
	}
}

func TestTaskValidatorDynamicCacheCapacity(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		NewConfig(dynamicconfig.NewCollection(client, log.NewNoopLogger())), "nsname",
	)
	v := newTaskValidator(context.Background(), cfg, nil, nil, nil)
	for id := int64(1); id <= 128; id++ {
		v.postValidate(&persistencespb.AllocatedTaskInfo{TaskId: id})
	}
	for _, tc := range []struct {
		capacity int
		wantIDs  []int64
	}{
		{capacity: 2, wantIDs: []int64{127, 128}},
		{capacity: 0, wantIDs: []int64{128}},
		{capacity: -1, wantIDs: []int64{128}},
	} {
		t.Cleanup(client.OverrideSetting(dynamicconfig.MatchingValidatorCacheMaxSize, []dynamicconfig.ConstrainedValue{{
			Constraints: dynamicconfig.Constraints{Namespace: "nsname", TaskQueueName: "tq", TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			Value:       tc.capacity,
		}}))
		require.False(t, v.preValidateActive(&persistencespb.AllocatedTaskInfo{TaskId: 128}))
		var ids []int64
		for id := range v.cache {
			ids = append(ids, id)
		}
		require.ElementsMatch(t, tc.wantIDs, ids)
	}
	t.Cleanup(client.OverrideSetting(dynamicconfig.MatchingValidatorCacheMaxSize, 3))
	v.postValidate(&persistencespb.AllocatedTaskInfo{TaskId: 129})
	v.postValidate(&persistencespb.AllocatedTaskInfo{TaskId: 130})
	require.Len(t, v.cache, 3)
}

func (s *taskValidatorSuite) TestIsTaskValid_ActivityTask_Valid() {
	taskType := enumspb.TASK_QUEUE_TYPE_ACTIVITY

	s.historyClient.EXPECT().IsActivityTaskValid(gomock.Any(), &historyservice.IsActivityTaskValidRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		Clock:            s.task.Data.Clock,
		ScheduledEventId: s.task.Data.ScheduledEventId,
		Stamp:            s.task.Data.GetStamp(),
	}).Return(&historyservice.IsActivityTaskValidResponse{IsValid: true}, nil)

	valid, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.NoError(err)
	s.True(valid)
}

func (s *taskValidatorSuite) TestIsTaskValid_StandaloneActivityTask_Valid() {
	const componentRef = "standalone-activity-component-ref"

	s.task.Data.WorkflowId = ""
	s.task.Data.RunId = ""
	s.task.Data.ScheduledEventId = 0
	s.task.Data.ComponentRef = []byte(componentRef)

	s.historyClient.EXPECT().IsActivityTaskValid(gomock.Any(), &historyservice.IsActivityTaskValidRequest{
		NamespaceId:  s.namespaceID,
		Execution:    &commonpb.WorkflowExecution{},
		Clock:        s.task.Data.Clock,
		Stamp:        s.task.Data.GetStamp(),
		ComponentRef: []byte(componentRef),
	}).Return(&historyservice.IsActivityTaskValidResponse{IsValid: true}, nil)

	valid, err := s.taskValidator.isTaskValid(s.task, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	s.Require().NoError(err)
	s.True(valid)
}

func (s *taskValidatorSuite) TestIsTaskValid_ActivityTask_NotFound() {
	taskType := enumspb.TASK_QUEUE_TYPE_ACTIVITY

	s.historyClient.EXPECT().IsActivityTaskValid(gomock.Any(), &historyservice.IsActivityTaskValidRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		Clock:            s.task.Data.Clock,
		ScheduledEventId: s.task.Data.ScheduledEventId,
		Stamp:            s.task.Data.GetStamp(),
	}).Return(nil, &serviceerror.NotFound{})

	valid, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.NoError(err)
	s.False(valid)
}

func (s *taskValidatorSuite) TestIsTaskValid_ActivityTask_Error() {
	taskType := enumspb.TASK_QUEUE_TYPE_ACTIVITY

	s.historyClient.EXPECT().IsActivityTaskValid(gomock.Any(), &historyservice.IsActivityTaskValidRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		Clock:            s.task.Data.Clock,
		ScheduledEventId: s.task.Data.ScheduledEventId,
		Stamp:            s.task.Data.GetStamp(),
	}).Return(nil, &serviceerror.Unavailable{})

	_, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.Error(err)
}

func (s *taskValidatorSuite) TestIsTaskValid_WorkflowTask_Valid() {
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	s.historyClient.EXPECT().IsWorkflowTaskValid(gomock.Any(), &historyservice.IsWorkflowTaskValidRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		Clock:            s.task.Data.Clock,
		ScheduledEventId: s.task.Data.ScheduledEventId,
		Stamp:            s.task.Data.GetStamp(),
	}).Return(&historyservice.IsWorkflowTaskValidResponse{IsValid: true}, nil)

	valid, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.NoError(err)
	s.True(valid)
}

func (s *taskValidatorSuite) TestIsTaskValid_WorkflowTask_NotFound() {
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	s.historyClient.EXPECT().IsWorkflowTaskValid(gomock.Any(), &historyservice.IsWorkflowTaskValidRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		Clock:            s.task.Data.Clock,
		ScheduledEventId: s.task.Data.ScheduledEventId,
		Stamp:            s.task.Data.GetStamp(),
	}).Return(nil, &serviceerror.NotFound{})

	valid, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.NoError(err)
	s.False(valid)
}

func (s *taskValidatorSuite) TestIsTaskValid_WorkflowTask_Error() {
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	s.historyClient.EXPECT().IsWorkflowTaskValid(gomock.Any(), &historyservice.IsWorkflowTaskValidRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		Clock:            s.task.Data.Clock,
		ScheduledEventId: s.task.Data.ScheduledEventId,
		Stamp:            s.task.Data.GetStamp(),
	}).Return(nil, &serviceerror.Unavailable{})

	_, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.Error(err)
}
