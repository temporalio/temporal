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

package serialization

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/tasks"
)

type (
	taskSerializerSuite struct {
		suite.Suite
		*require.Assertions

		namespaceID    string
		workflowID     string
		runID          string
		taskSerializer *TaskSerializer
	}
)

func TestTaskSerializerSuite(t *testing.T) {
	suite.Run(t, new(taskSerializerSuite))
}

func (s *taskSerializerSuite) SetupSuite() {

}

func (s *taskSerializerSuite) TearDownSuite() {

}

func (s *taskSerializerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespaceID = "random namespace ID"
	s.workflowID = "random workflow ID"
	s.runID = "random run ID"
	s.taskSerializer = NewTaskSerializer(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
}

func (s *taskSerializerSuite) TearDownTest() {

}

func (s *taskSerializerSuite) TestTransferWorkflowTask() {
	workflowTask := &tasks.WorkflowTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		NamespaceID:         uuid.New().String(),
		TaskQueue:           shuffle.String("random task queue name"),
		ScheduleID:          rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{workflowTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{workflowTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTransferActivityTask() {
	activityTask := &tasks.ActivityTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		NamespaceID:         uuid.New().String(),
		TaskQueue:           shuffle.String("random task queue name"),
		ScheduleID:          rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{activityTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{activityTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTransferRequestCancelTask() {
	requestCancelTask := &tasks.CancelExecutionTask{
		VisibilityTimestamp:     time.Unix(0, rand.Int63()).UTC(),
		TaskID:                  rand.Int63(),
		TargetNamespaceID:       uuid.New().String(),
		TargetWorkflowID:        uuid.New().String(),
		TargetRunID:             uuid.New().String(),
		TargetChildWorkflowOnly: rand.Int63()%2 == 0,
		InitiatedID:             rand.Int63(),
		Version:                 rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{requestCancelTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{requestCancelTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTransferSignalTask() {
	signalTask := &tasks.SignalExecutionTask{
		VisibilityTimestamp:     time.Unix(0, rand.Int63()).UTC(),
		TaskID:                  rand.Int63(),
		TargetNamespaceID:       uuid.New().String(),
		TargetWorkflowID:        uuid.New().String(),
		TargetRunID:             uuid.New().String(),
		TargetChildWorkflowOnly: rand.Int63()%2 == 0,
		InitiatedID:             rand.Int63(),
		Version:                 rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{signalTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{signalTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTransferChildWorkflowTask() {
	childWorkflowTask := &tasks.StartChildExecutionTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		TargetNamespaceID:   uuid.New().String(),
		TargetWorkflowID:    uuid.New().String(),
		InitiatedID:         rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{childWorkflowTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{childWorkflowTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTransferCloseTask() {
	closeTask := &tasks.CloseExecutionTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{closeTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{closeTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTransferResetTask() {
	resetTask := &tasks.ResetWorkflowTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{resetTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{resetTask}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerWorkflowTask() {
	workflowTaskTimer := &tasks.WorkflowTaskTimeoutTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		ScheduleAttempt:     rand.Int31(),
		TimeoutType:         enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name)))),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{workflowTaskTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{workflowTaskTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerWorkflowDelayTask() {
	workflowDelayTimer := &tasks.WorkflowBackoffTimerTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		WorkflowBackoffType: enumsspb.WorkflowBackoffType(rand.Int31n(int32(len(enumsspb.WorkflowBackoffType_name)))),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{workflowDelayTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{workflowDelayTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerActivityTask() {
	activityTaskTimer := &tasks.ActivityTimeoutTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Attempt:             rand.Int31(),
		TimeoutType:         enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name)))),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{activityTaskTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{activityTaskTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerActivityRetryTask() {
	activityRetryTimer := &tasks.ActivityRetryTimerTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Attempt:             rand.Int31(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{activityRetryTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{activityRetryTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerUserTask() {
	userTimer := &tasks.UserTimerTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{userTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{userTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerWorkflowRun() {
	workflowTimer := &tasks.WorkflowTimeoutTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{workflowTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{workflowTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestTimerWorkflowCleanupTask() {
	workflowCleanupTimer := &tasks.DeleteHistoryEventTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{workflowCleanupTimer})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{workflowCleanupTimer}, taskSlice)
}

func (s *taskSerializerSuite) TestVisibilityStartTask() {
	visibilityStart := &tasks.StartExecutionVisibilityTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeVisibilityTasks([]tasks.Task{visibilityStart})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeVisibilityTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{visibilityStart}, taskSlice)
}

func (s *taskSerializerSuite) TestVisibilityUpsertTask() {
	visibilityUpsert := &tasks.UpsertExecutionVisibilityTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeVisibilityTasks([]tasks.Task{visibilityUpsert})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeVisibilityTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{visibilityUpsert}, taskSlice)
}

func (s *taskSerializerSuite) TestVisibilityCloseTask() {
	visibilityClose := &tasks.CloseExecutionVisibilityTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeVisibilityTasks([]tasks.Task{visibilityClose})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeVisibilityTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{visibilityClose}, taskSlice)
}

func (s *taskSerializerSuite) TestVisibilityDeleteTask() {
	visibilityDelete := &tasks.CloseExecutionVisibilityTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeVisibilityTasks([]tasks.Task{visibilityDelete})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeVisibilityTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{visibilityDelete}, taskSlice)
}

func (s *taskSerializerSuite) TestReplicateActivityTask() {
	replicateActivityTask := &tasks.SyncActivityTask{
		VisibilityTimestamp: time.Time{},
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		ScheduledID:         rand.Int63(),
	}

	blobSlice, err := s.taskSerializer.SerializeReplicationTasks([]tasks.Task{replicateActivityTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeReplicationTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{replicateActivityTask}, taskSlice)
}

func (s *taskSerializerSuite) TestReplicateHistoryTask() {
	replicateHistoryTask := &tasks.HistoryReplicationTask{
		VisibilityTimestamp: time.Time{},
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		FirstEventID:        rand.Int63(),
		NextEventID:         rand.Int63(),
		BranchToken:         shuffle.Bytes([]byte("random branch token")),
		NewRunBranchToken:   shuffle.Bytes([]byte("random new branch token")),
	}

	blobSlice, err := s.taskSerializer.SerializeReplicationTasks([]tasks.Task{replicateHistoryTask})
	s.NoError(err)
	taskSlice, err := s.taskSerializer.DeserializeReplicationTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{replicateHistoryTask}, taskSlice)
}
