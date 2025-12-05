package serialization

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
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

func (s *taskSerializerSuite) TestTransferChasmTask() {
	transferChasmTask := &tasks.ChasmTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Category:            tasks.CategoryTransfer,
		Info: &persistencespb.ChasmTaskInfo{
			Data: &commonpb.DataBlob{
				Data: []byte("some-data"),
			},
			ArchetypeId: rand.Uint32(),
		},
	}

	s.assertEqualTasks(transferChasmTask)
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
		ArchetypeID:         rand.Uint32(),
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

func (s *taskSerializerSuite) TestDeleteExecutionVisibilityTask() {
	deleteExecutionVisibilityTask := &tasks.DeleteExecutionVisibilityTask{
		WorkflowKey:                    s.workflowKey,
		VisibilityTimestamp:            time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:                         rand.Int63(),
		ArchetypeID:                    rand.Uint32(),
		CloseExecutionVisibilityTaskID: rand.Int63(),
		CloseTime:                      time.Unix(0, 0).UTC(),
	}

	s.assertEqualTasks(deleteExecutionVisibilityTask)
}

func (s *taskSerializerSuite) TestVisibilityChasmTask() {
	visibilityChasmTask := &tasks.ChasmTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Category:            tasks.CategoryVisibility,
		Info: &persistencespb.ChasmTaskInfo{
			Data: &commonpb.DataBlob{
				Data: []byte("some-data"),
			},
			ArchetypeId: rand.Uint32(),
		},
	}

	s.assertEqualTasks(visibilityChasmTask)
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

func (s *taskSerializerSuite) TestSyncVersionedTransitionTask() {
	syncVersionedTransitionTask := &tasks.SyncVersionedTransitionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		ArchetypeID:         rand.Uint32(),
		FirstEventID:        rand.Int63(),
		NextEventID:         rand.Int63(),
		NewRunID:            uuid.New().String(),
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		TaskEquivalents: []tasks.Task{
			&tasks.HistoryReplicationTask{
				WorkflowKey:         s.workflowKey,
				VisibilityTimestamp: time.Unix(0, 0).UTC(),
				FirstEventID:        rand.Int63(),
				NextEventID:         rand.Int63(),
				Version:             rand.Int63(),
				NewRunID:            uuid.New().String(),
			},
		},
	}

	s.assertEqualTasksWithOpts(syncVersionedTransitionTask,
		func(task, deserializedTask tasks.Task) {
			s.True(proto.Equal(task.(*tasks.SyncVersionedTransitionTask).VersionedTransition, deserializedTask.(*tasks.SyncVersionedTransitionTask).VersionedTransition))
		},
		cmpopts.IgnoreFields(tasks.SyncVersionedTransitionTask{}, "VersionedTransition"),
	)
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

func (s *taskSerializerSuite) TestDeleteExecutionTask() {
	deleteExecutionTask := &tasks.DeleteExecutionTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, 0).UTC(), // go == compare for location as well which is striped during marshaling/unmarshaling
		TaskID:              rand.Int63(),
		ArchetypeID:         rand.Uint32(),
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

func (s *taskSerializerSuite) TestOutboundChasmTask() {
	task := &tasks.ChasmTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Category:            tasks.CategoryOutbound,
		Info: &persistencespb.ChasmTaskInfo{
			Data: &commonpb.DataBlob{
				Data: []byte("some-data"),
			},
			ArchetypeId: rand.Uint32(),
		},
		Destination: "somewhere",
	}

	s.assertEqualTasks(task)
}

func (s *taskSerializerSuite) TestStateMachineOutboundTask() {
	task := &tasks.StateMachineOutboundTask{
		StateMachineTask: tasks.StateMachineTask{
			WorkflowKey:         s.workflowKey,
			VisibilityTimestamp: time.Now().UTC(),
			TaskID:              rand.Int63(),
			Info: &persistencespb.StateMachineTaskInfo{
				Ref: &persistencespb.StateMachineRef{
					Path: []*persistencespb.StateMachineKey{
						{
							Type: "some-type",
							Id:   "some-id",
						},
					},
					MutableStateVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: rand.Int63(),
						TransitionCount:          rand.Int63(),
					},
					MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: rand.Int63(),
						TransitionCount:          rand.Int63(),
					},
					MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
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

func (s *taskSerializerSuite) TestTimerChasmTask() {
	task := &tasks.ChasmTask{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		Category:            tasks.CategoryTimer,
		Info: &persistencespb.ChasmTaskInfo{
			Data: &commonpb.DataBlob{
				Data: []byte("some-data"),
			},
			ArchetypeId: rand.Uint32(),
		},
	}

	s.assertEqualTasks(task)
}

func (s *taskSerializerSuite) TestTimerChasmPureTask() {
	task := &tasks.ChasmTaskPure{
		WorkflowKey:         s.workflowKey,
		VisibilityTimestamp: time.Unix(0, rand.Int63()).UTC(),
		TaskID:              rand.Int63(),
		ArchetypeID:         rand.Uint32(),
	}

	s.assertEqualTasks(task)
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

func (s *taskSerializerSuite) assertEqualTasksWithOpts(
	task tasks.Task,
	cmpFunc func(task, deserializedTask tasks.Task),
	opts ...cmp.Option,
) {
	blob, err := s.taskSerializer.SerializeTask(task)
	s.NoError(err)
	deserializedTask, err := s.taskSerializer.DeserializeTask(task.GetCategory(), blob)
	s.NoError(err)
	s.Empty(cmp.Diff(task, deserializedTask, opts...))
	if cmpFunc != nil {
		cmpFunc(task, deserializedTask)
	}
}

func (s *taskSerializerSuite) assertEqualTasks(
	task tasks.Task,
) {
	blob, err := s.taskSerializer.SerializeTask(task)
	s.NoError(err)
	deserializedTask, err := s.taskSerializer.DeserializeTask(task.GetCategory(), blob)
	s.NoError(err)

	// Find all top-level protobuf fields and compare them, then unset them, as their
	// internal state parameters can cause direct struct comparison to fail.
	taskValue := reflect.ValueOf(task).Elem()
	deserializedTaskValue := reflect.ValueOf(deserializedTask).Elem()
	s.Equal(taskValue.Type(), deserializedTaskValue.Type())

	for i := range deserializedTaskValue.NumField() {
		deField := deserializedTaskValue.Field(i)
		if !deField.CanInterface() {
			continue
		}

		if innerProto, ok := deField.Interface().(proto.Message); ok {
			originalProto, _ := taskValue.Field(i).Interface().(proto.Message)
			protorequire.ProtoEqual(s.T(), originalProto, innerProto)

			taskValue.Field(i).SetZero()
			deserializedTaskValue.Field(i).SetZero()
		}
	}

	s.Equal(task, deserializedTask)
}
