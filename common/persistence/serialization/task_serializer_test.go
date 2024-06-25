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
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/testing/protorequire"
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
		ScheduledEventID:    rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(workflowTask)
}

func (s *taskSerializerSuite) TestTransferActivityTask() {
	activityTask := &tasks.ActivityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		TaskQueue:           shuffle.String("random task queue name"),
		ScheduledEventID:    rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(activityTask)
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
		InitiatedEventID:        rand.Int63(),
		Version:                 rand.Int63(),
	}

	s.assertEqualTasks(requestCancelTask)
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
		InitiatedEventID:        rand.Int63(),
		Version:                 rand.Int63(),
	}

	s.assertEqualTasks(signalTask)
}

func (s *taskSerializerSuite) TestTransferChildWorkflowTask() {
	childWorkflowTask := &tasks.StartChildExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		TargetNamespaceID:   uuid.New().String(),
		TargetWorkflowID:    uuid.New().String(),
		InitiatedEventID:    rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(childWorkflowTask)
}

func (s *taskSerializerSuite) TestTransferCloseTask() {
	closeTask := &tasks.CloseExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}
	s.assertEqualTasks(closeTask)
}

func (s *taskSerializerSuite) TestTransferResetTask() {
	resetTask := &tasks.ResetWorkflowTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(resetTask)
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

	s.assertEqualTasks(workflowTaskTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowDelayTask() {
	workflowDelayTimer := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		WorkflowBackoffType: enumsspb.WorkflowBackoffType(rand.Int31n(int32(len(enumsspb.WorkflowBackoffType_name)))),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(workflowDelayTimer)
}

func (s *taskSerializerSuite) TestTimerActivityTask() {
	activityTaskTimer := &tasks.ActivityTimeoutTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
		Attempt:             rand.Int31(),
		TimeoutType:         enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name)))),
	}

	s.assertEqualTasks(activityTaskTimer)
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

	s.assertEqualTasks(activityRetryTimer)
}

func (s *taskSerializerSuite) TestTimerUserTask() {
	userTimer := &tasks.UserTimerTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		EventID:             rand.Int63(),
	}

	s.assertEqualTasks(userTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowRun() {
	workflowRunTimer := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(workflowRunTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowExecution() {
	workflowExecutionTimer := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         s.workflowKey.NamespaceID,
		WorkflowID:          s.workflowKey.WorkflowID,
		FirstRunID:          s.workflowKey.RunID,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
	}

	s.assertEqualTasks(workflowExecutionTimer)
}

func (s *taskSerializerSuite) TestTimerWorkflowCleanupTask() {
	workflowCleanupTimer := &tasks.DeleteHistoryEventTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		BranchToken:         []byte{123},
	}
	s.assertEqualTasks(workflowCleanupTimer)
}

func (s *taskSerializerSuite) TestVisibilityStartTask() {
	visibilityStart := &tasks.StartExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(visibilityStart)
}

func (s *taskSerializerSuite) TestVisibilityUpsertTask() {
	visibilityUpsert := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
	}

	s.assertEqualTasks(visibilityUpsert)
}

func (s *taskSerializerSuite) TestVisibilityCloseTask() {
	visibilityClose := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(visibilityClose)
}

func (s *taskSerializerSuite) TestVisibilityDeleteTask() {
	visibilityDelete := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.assertEqualTasks(visibilityDelete)
}

func (s *taskSerializerSuite) TestSyncActivityTask() {
	syncActivityTask := &tasks.SyncActivityTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		ScheduledEventID:    rand.Int63(),
	}

	s.assertEqualTasks(syncActivityTask)
}

func (s *taskSerializerSuite) TestHistoryReplicationTask() {
	historyReplicationTask := &tasks.HistoryReplicationTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		FirstEventID:        rand.Int63(),
		NextEventID:         rand.Int63(),
		BranchToken:         shuffle.Bytes([]byte("random branch token")),
		NewRunBranchToken:   shuffle.Bytes([]byte("random new branch token")),
	}

	s.assertEqualTasks(historyReplicationTask)
}

func (s *taskSerializerSuite) TestSyncHSMTask() {
	syncHSMTask := &tasks.SyncHSMTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
	}

	s.assertEqualTasks(syncHSMTask)
}

func (s *taskSerializerSuite) TestSyncWorkflowStateTask() {
	syncWorkflowStateTask := &tasks.SyncWorkflowStateTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
		Priority:            enumsspb.TASK_PRIORITY_LOW,
	}

	s.assertEqualTasks(syncWorkflowStateTask)
}

func (s *taskSerializerSuite) TestDeleteExecutionVisibilityTask() {
	deleteExecutionVisibilityTask := &tasks.DeleteExecutionVisibilityTask{
		WorkflowKey:                    s.workflowKey,
		VisibilityTimestamp:            time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:                         rand.Int63(),
		CloseExecutionVisibilityTaskID: rand.Int63(),
	}

	s.assertEqualTasks(deleteExecutionVisibilityTask)
}

func (s *taskSerializerSuite) TestDeleteExecutionTask() {
	deleteExecutionTask := &tasks.DeleteExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
	}

	s.assertEqualTasks(deleteExecutionTask)
}

func (s *taskSerializerSuite) TestArchiveExecutionTask() {
	task := &tasks.ArchiveExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}
	s.Assert().Equal(tasks.CategoryArchival, task.GetCategory())
	s.Assert().Equal(enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION, task.GetType())

	s.assertEqualTasks(task)
}

func (s *taskSerializerSuite) TestStateMachineOutboundTask() {
	task := &tasks.StateMachineOutboundTask{
		StateMachineTask: tasks.StateMachineTask{
			WorkflowKey:         s.workflowKey,
			VisibilityTimestamp: time.Now().UTC(),
			TaskID:              rand.Int63(),
			Info: &persistence.StateMachineTaskInfo{
				Ref: &persistence.StateMachineRef{
					Path: []*persistence.StateMachineKey{
						{
							Type: "some-type",
							Id:   "some-id",
						},
					},
					MutableStateVersionedTransition: &persistence.VersionedTransition{
						NamespaceFailoverVersion: rand.Int63(),
						TransitionCount:          rand.Int63(),
					},
					MachineInitialVersionedTransition: &persistence.VersionedTransition{
						NamespaceFailoverVersion: rand.Int63(),
						TransitionCount:          rand.Int63(),
					},
					MachineLastUpdateVersionedTransition: &persistence.VersionedTransition{
						NamespaceFailoverVersion: rand.Int63(),
						TransitionCount:          rand.Int63(),
					},
					MachineTransitionCount: rand.Int63(),
				},
				Type: "some-type",
				Data: []byte{},
			},
		},
		Destination: "foo",
	}

	s.Assert().Equal(tasks.CategoryOutbound, task.GetCategory())
	s.Assert().Equal(enumsspb.TASK_TYPE_STATE_MACHINE_OUTBOUND, task.GetType())

	blob, err := s.taskSerializer.SerializeTask(task)
	s.NoError(err)
	deserializedTaskIface, err := s.taskSerializer.DeserializeTask(task.GetCategory(), blob)
	deserializedTask := deserializedTaskIface.(*tasks.StateMachineOutboundTask)
	s.NoError(err)

	protorequire.ProtoEqual(s.T(), task.Info, deserializedTask.Info)
	task.Info = nil
	deserializedTask.Info = nil
	s.Equal(task, deserializedTask)
}

func (s *taskSerializerSuite) TestStateMachineTimerTask() {
	task := &tasks.StateMachineTimerTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              rand.Int63(),
		Version:             rand.Int63(),
	}

	s.Assert().Equal(tasks.CategoryTimer, task.GetCategory())
	s.Assert().Equal(enumsspb.TASK_TYPE_STATE_MACHINE_TIMER, task.GetType())

	blob, err := s.taskSerializer.SerializeTask(task)
	s.NoError(err)
	deserializedTaskIface, err := s.taskSerializer.DeserializeTask(task.GetCategory(), blob)
	deserializedTask := deserializedTaskIface.(*tasks.StateMachineTimerTask)
	s.NoError(err)

	s.Equal(task, deserializedTask)
}

func (s *taskSerializerSuite) assertEqualTasks(
	task tasks.Task,
) {
	blob, err := s.taskSerializer.SerializeTask(task)
	s.NoError(err)
	deserializedTask, err := s.taskSerializer.DeserializeTask(task.GetCategory(), blob)
	s.NoError(err)
	s.Equal(task, deserializedTask)
}
