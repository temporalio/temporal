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

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	taskValidatorSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		historyClient *historyservicemock.MockHistoryServiceClient

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
	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)

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

	s.taskValidator = newTaskValidator(s.historyClient)
}

func (s *taskValidatorSuite) TeardownTest() {
	s.controller.Finish()
}

func (s *taskValidatorSuite) TestShouldValidate_NewTask_Validate() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.Data.ScheduledEventId - 1,
		validationTime: time.Unix(0, rand.Int63()),
	}
	s.task.Data.CreateTime = timestamp.TimePtr(time.Now().Add(-2 * taskReaderValidationThreshold))

	shouldValidate := s.taskValidator.shouldValidate(s.task)
	s.True(shouldValidate)
}

func (s *taskValidatorSuite) TestShouldValidate_NewTask_Skip() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.Data.ScheduledEventId - 1,
		validationTime: time.Unix(0, rand.Int63()),
	}
	s.task.Data.CreateTime = timestamp.TimePtr(time.Now().Add(2 * taskReaderValidationThreshold))

	shouldValidate := s.taskValidator.shouldValidate(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestShouldValidate_ExistingTask_Validate() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.Data.ScheduledEventId,
		validationTime: time.Now().Add(-2 * taskReaderValidationThreshold),
	}

	shouldValidate := s.taskValidator.shouldValidate(s.task)
	s.False(shouldValidate)
}

func (s *taskValidatorSuite) TestShouldValidate_ExistingTask_Skip() {
	s.taskValidator.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         s.task.Data.ScheduledEventId,
		validationTime: time.Now().Add(2 * taskReaderValidationThreshold),
	}

	shouldValidate := s.taskValidator.shouldValidate(s.task)
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
