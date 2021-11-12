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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/service/history/tasks"
)

type (
	taskSerializerSuite struct {
		suite.Suite
		*require.Assertions

		workflowKey    definition.WorkflowKey
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

	s.workflowKey = definition.NewWorkflowKey(
		"random namespace ID",
		"random workflow ID",
		"random run ID",
	)
	s.taskSerializer = NewTaskSerializer()
}

func (s *taskSerializerSuite) TearDownTest() {

}

func (s *taskSerializerSuite) TestTransferWorkflowTask() {
	workflowTask := &tasks.WorkflowTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		TaskQueue:           shuffle.String("random task queue name"),
		ScheduleID:          rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTransferTasks(workflowTask)
}

func (s *taskSerializerSuite) TestTransferActivityTask() {
	activityTask := &tasks.ActivityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		TargetNamespaceID:   uuid.New().String(),
		TaskQueue:           shuffle.String("random task queue name"),
		ScheduleID:          rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTransferTasks(activityTask)
}

func (s *taskSerializerSuite) TestTransferRequestCancelTask() {
	requestCancelTask := &tasks.CancelExecutionTask{
		WorkflowKey:             s.workflowKey,
		VisibilityTimestamp:     time.Unix(0, rand.Int63()).UTC(),
		TaskID:                  rand.Int63(),
		TargetNamespaceID:       uuid.New().String(),
		TargetWorkflowID:        uuid.New().String(),
		TargetRunID:             uuid.New().String(),
		TargetChildWorkflowOnly: rand.Int63()%2 == 0,
		InitiatedID:             rand.Int63(),
		Version:                 rand.Int63(),
	}

	s.assertEqualTransferTasks(requestCancelTask)
}

func (s *taskSerializerSuite) TestTransferSignalTask() {
	signalTask := &tasks.SignalExecutionTask{
		WorkflowKey:             s.workflowKey,
		VisibilityTimestamp:     time.Unix(0, rand.Int63()).UTC(),
		TaskID:                  rand.Int63(),
		TargetNamespaceID:       uuid.New().String(),
		TargetWorkflowID:        uuid.New().String(),
		TargetRunID:             uuid.New().String(),
		TargetChildWorkflowOnly: rand.Int63()%2 == 0,
		InitiatedID:             rand.Int63(),
		Version:                 rand.Int63(),
	}

	s.assertEqualTransferTasks(signalTask)
}

func (s *taskSerializerSuite) TestTransferChildWorkflowTask() {
	childWorkflowTask := &tasks.StartChildExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		TargetNamespaceID:   uuid.New().String(),
		TargetWorkflowID:    uuid.New().String(),
		InitiatedID:         rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTransferTasks(childWorkflowTask)
}

func (s *taskSerializerSuite) TestTransferCloseTask() {
	closeTask := &tasks.CloseExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTransferTasks(closeTask)
}

func (s *taskSerializerSuite) TestTransferResetTask() {
	resetTask := &tasks.ResetWorkflowTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTransferTasks(resetTask)
}

func (s *taskSerializerSuite) TestTimerWorkflowTask() {
	workflowTaskTimer := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		ScheduleAttempt:     rand.Int31(),
		TimeoutType:         enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name)))),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(workflowTaskTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowDelayTask() {
	workflowDelayTimer := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		WorkflowBackoffType: enumsspb.WorkflowBackoffType(rand.Int31n(int32(len(enumsspb.WorkflowBackoffType_name)))),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(workflowDelayTimer)
}

func (s *taskSerializerSuite) TestTimerActivityTask() {
	activityTaskTimer := &tasks.ActivityTimeoutTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Attempt:             rand.Int31(),
		TimeoutType:         enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name)))),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(activityTaskTimer)
}

func (s *taskSerializerSuite) TestTimerActivityRetryTask() {
	activityRetryTimer := &tasks.ActivityRetryTimerTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Attempt:             rand.Int31(),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(activityRetryTimer)
}

func (s *taskSerializerSuite) TestTimerUserTask() {
	userTimer := &tasks.UserTimerTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(userTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowRun() {
	workflowTimer := &tasks.WorkflowTimeoutTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(workflowTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowCleanupTask() {
	workflowCleanupTimer := &tasks.DeleteHistoryEventTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTimerTasks(workflowCleanupTimer)
}

func (s *taskSerializerSuite) TestVisibilityStartTask() {
	visibilityStart := &tasks.StartExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualVisibilityTasks(visibilityStart)
}

func (s *taskSerializerSuite) TestVisibilityUpsertTask() {
	visibilityUpsert := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualVisibilityTasks(visibilityUpsert)
}

func (s *taskSerializerSuite) TestVisibilityCloseTask() {
	visibilityClose := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualVisibilityTasks(visibilityClose)
}

func (s *taskSerializerSuite) TestVisibilityDeleteTask() {
	visibilityDelete := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualVisibilityTasks(visibilityDelete)
}

func (s *taskSerializerSuite) TestReplicateActivityTask() {
	replicateActivityTask := &tasks.SyncActivityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		ScheduledID:         rand.Int63(),
	}

	s.assertEqualReplicationTasks(replicateActivityTask)
}

func (s *taskSerializerSuite) TestReplicateHistoryTask() {
	replicateHistoryTask := &tasks.HistoryReplicationTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		FirstEventID:        rand.Int63(),
		NextEventID:         rand.Int63(),
		BranchToken:         shuffle.Bytes([]byte("random branch token")),
		NewRunBranchToken:   shuffle.Bytes([]byte("random new branch token")),
	}

	s.assertEqualReplicationTasks(replicateHistoryTask)
}

func (s *taskSerializerSuite) assertEqualTransferTasks(
	task tasks.Task,
) {
	blobMap, err := s.taskSerializer.SerializeTransferTasks([]tasks.Task{task})
	s.NoError(err)
	blobSlice := []commonpb.DataBlob{blobMap[task.GetKey()]}
	taskSlice, err := s.taskSerializer.DeserializeTransferTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{task}, taskSlice)
}

func (s *taskSerializerSuite) assertEqualTimerTasks(
	task tasks.Task,
) {
	blobMap, err := s.taskSerializer.SerializeTimerTasks([]tasks.Task{task})
	s.NoError(err)
	blobSlice := []commonpb.DataBlob{blobMap[task.GetKey()]}
	taskSlice, err := s.taskSerializer.DeserializeTimerTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{task}, taskSlice)
}

func (s *taskSerializerSuite) assertEqualVisibilityTasks(
	task tasks.Task,
) {
	blobMap, err := s.taskSerializer.SerializeVisibilityTasks([]tasks.Task{task})
	s.NoError(err)
	blobSlice := []commonpb.DataBlob{blobMap[task.GetKey()]}
	taskSlice, err := s.taskSerializer.DeserializeVisibilityTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{task}, taskSlice)
}

func (s *taskSerializerSuite) assertEqualReplicationTasks(
	task tasks.Task,
) {
	blobMap, err := s.taskSerializer.SerializeReplicationTasks([]tasks.Task{task})
	s.NoError(err)
	blobSlice := []commonpb.DataBlob{blobMap[task.GetKey()]}
	taskSlice, err := s.taskSerializer.DeserializeReplicationTasks(blobSlice)
	s.NoError(err)
	s.Equal([]tasks.Task{task}, taskSlice)
}
