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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
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

	s.taskValidator = newTaskValidator(context.Background(), s.clusterMetadata, s.namespaceCache, s.historyClient)
}

func (s *taskValidatorSuite) putCache(info taskValidationInfo) {
	s.taskValidator.mu.Lock()
	defer s.taskValidator.mu.Unlock()
	s.taskValidator.cache[info.taskID] = info
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
	s.Equal(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: s.task.Data.CreateTime.AsTime(),
	}, info)
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
		validationTime: time.Now().Add(-taskReaderValidationThreshold * 2),
	})
	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidateActive_ExistingTask_Skip() {
	s.putCache(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(taskReaderValidationThreshold * 2),
	})
	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Skip_WithCreationTime() {
	s.task.Data.CreateTime = timestamppb.New(time.Now().Add(-taskReaderValidationThreshold / 2))

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: s.task.Data.CreateTime.AsTime(),
	}, info)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Validate_WithCreationTime() {
	s.task.Data.CreateTime = timestamppb.New(time.Now().Add(-taskReaderValidationThreshold * 2))

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.True(shouldValidate)
	info, ok := s.cacheInfo(s.task.TaskId)
	s.True(ok)
	s.Equal(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: s.task.Data.CreateTime.AsTime(),
	}, info)
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
		validationTime: time.Now().Add(-taskReaderValidationThreshold * 2),
	})
	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidatePassive_ExistingTask_Skip() {
	s.putCache(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(taskReaderValidationThreshold * 2),
	})
	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestCache_TwoTaskIDsIndependent() {
	other := s.task.TaskId + 1
	s.putCache(taskValidationInfo{
		taskID:         other,
		validationTime: time.Now().Add(-taskReaderValidationThreshold * 2),
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
	for i := 0; i < n; i++ {
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

func (s *taskValidatorSuite) TestCache_EvictsOldestWhenFull() {
	now := time.Now()
	for i := 0; i < taskValidatorCacheMaxSize; i++ {
		s.putCache(taskValidationInfo{
			taskID:         int64(i + 1),
			validationTime: now.Add(time.Duration(i) * time.Second),
		})
	}
	newTask := &persistencespb.AllocatedTaskInfo{
		TaskId: int64(taskValidatorCacheMaxSize + 1),
		Data:   &persistencespb.TaskInfo{CreateTime: timestamppb.Now()},
	}
	s.False(s.taskValidator.preValidateActive(newTask))

	_, oldestStillThere := s.cacheInfo(1)
	s.False(oldestStillThere, "oldest validationTime must be evicted")
	_, newestKept := s.cacheInfo(int64(taskValidatorCacheMaxSize))
	s.True(newestKept)
	_, inserted := s.cacheInfo(newTask.TaskId)
	s.True(inserted)
	s.taskValidator.mu.Lock()
	defer s.taskValidator.mu.Unlock()
	s.Len(s.taskValidator.cache, taskValidatorCacheMaxSize)
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
