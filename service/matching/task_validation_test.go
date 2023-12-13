// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package matching

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
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
		},
	}

	s.taskValidator = newTaskValidator(func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 4*time.Second)
	}, s.clusterMetadata, s.namespaceCache, s.historyClient)
}

func (s *taskValidatorSuite) TeardownTest() {
	s.controller.Finish()
}

func (s *taskValidatorSuite) TestPreValidateActive_NewTask_Skip_WithCreationTime() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId - 1,
		validationTime: time.Unix(0, rand.Int63()).UTC(),
	}
	s.task.Data.CreateTime = timestamppb.New(time.Unix(0, rand.Int63()))

	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
	s.Equal(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: s.task.Data.CreateTime.AsTime(),
	}, s.taskValidator.lastValidatedTaskInfo)
}

func (s *taskValidatorSuite) TestPreValidateActive_NewTask_Skip_WithoutCreationTime() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId - 1,
		validationTime: time.Unix(0, rand.Int63()).UTC(),
	}
	s.task.Data.CreateTime = nil

	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
	s.Equal(s.task.TaskId, s.taskValidator.lastValidatedTaskInfo.taskID)
	s.True(time.Now().Sub(s.taskValidator.lastValidatedTaskInfo.validationTime) < time.Second)
}

func (s *taskValidatorSuite) TestPreValidateActive_ExistingTask_Validate() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(-taskReaderValidationThreshold * 2),
	}

	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidateActive_ExistingTask_Skip() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(taskReaderValidationThreshold * 2),
	}

	shouldValidate := s.taskValidator.preValidateActive(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Skip_WithCreationTime() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId - 1,
		validationTime: time.Unix(0, rand.Int63()).UTC(),
	}
	s.task.Data.CreateTime = timestamppb.New(time.Now().Add(-taskReaderValidationThreshold / 2))

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
	s.Equal(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: s.task.Data.CreateTime.AsTime(),
	}, s.taskValidator.lastValidatedTaskInfo)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Validate_WithCreationTime() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId - 1,
		validationTime: time.Unix(0, rand.Int63()).UTC(),
	}
	s.task.Data.CreateTime = timestamppb.New(time.Now().Add(-taskReaderValidationThreshold * 2))

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.True(shouldValidate)
	s.Equal(taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: s.task.Data.CreateTime.AsTime(),
	}, s.taskValidator.lastValidatedTaskInfo)
}

func (s *taskValidatorSuite) TestPreValidatePassive_NewTask_Skip_WithoutCreationTime() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId - 1,
		validationTime: time.Unix(0, rand.Int63()).UTC(),
	}
	s.task.Data.CreateTime = nil

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
	s.Equal(s.task.TaskId, s.taskValidator.lastValidatedTaskInfo.taskID)
	s.True(time.Now().Sub(s.taskValidator.lastValidatedTaskInfo.validationTime) < time.Second)
}

func (s *taskValidatorSuite) TestPreValidatePassive_ExistingTask_Validate() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(-taskReaderValidationThreshold * 2),
	}

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestPreValidatePassive_ExistingTask_Skip() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.TaskId,
		validationTime: time.Now().Add(taskReaderValidationThreshold * 2),
	}

	shouldValidate := s.taskValidator.preValidatePassive(s.task)
	s.False(shouldValidate)
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
	}).Return(&historyservice.IsActivityTaskValidResponse{IsValid: true}, nil)

	valid, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.NoError(err)
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
	}).Return(nil, &serviceerror.Unavailable{})

	_, err := s.taskValidator.isTaskValid(s.task, taskType)
	s.Error(err)
}
