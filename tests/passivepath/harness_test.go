package passivepath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMutableStateDiffNormalizesOnlyLocalFields(t *testing.T) {
	expected := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			LastUpdateTime:               timestamppb.New(time.Unix(1, 0)),
			LastFirstEventTxnId:          11,
			StateTransitionCount:         12,
			CloseTransferTaskId:          14,
			CloseVisibilityTaskId:        15,
			StickyTaskQueue:              "active-sticky-queue",
			StickyScheduleToStartTimeout: durationpb.New(time.Second),
			ExecutionStats:               &persistencespb.ExecutionStats{HistorySize: 13},
			UpdateCount:                  1,
			WorkflowTaskStartedTime:      timestamppb.New(time.Unix(0, 0)),
		},
		SignalRequestedIds: []string{"b", "a"},
	}
	actual := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			LastUpdateTime:       timestamppb.New(time.Unix(2, 0)),
			LastFirstEventTxnId:  21,
			StateTransitionCount: 22,
			AutoResetPoints:      &workflowpb.ResetPoints{},
			ExecutionStats:       &persistencespb.ExecutionStats{HistorySize: 23},
			UpdateCount:          1,
		},
		SignalRequestedIds: []string{"a", "b"},
	}
	require.Empty(t, mutableStateDiff(expected, actual))

	actual.ExecutionInfo.UpdateCount = 2
	require.Contains(t, mutableStateDiff(expected, actual), "update_count")
}

func TestMutableStateDiffComparesStateMachineTimers(t *testing.T) {
	state := func(machineTransitionCount int64) *persistencespb.WorkflowMutableState {
		return &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				StateMachineTimers: []*persistencespb.StateMachineTimerGroup{{
					Deadline:  timestamppb.New(time.Unix(10, 0)),
					Scheduled: true,
					Infos: []*persistencespb.StateMachineTaskInfo{{
						Type: "callback-timeout",
						Data: []byte("timer-data"),
						Ref: &persistencespb.StateMachineRef{
							Path: []*persistencespb.StateMachineKey{{Type: "callback", Id: "id"}},
							MutableStateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: 1,
								TransitionCount:          2,
							},
							MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: 1,
								TransitionCount:          1,
							},
							MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: 1,
								TransitionCount:          2,
							},
							MachineTransitionCount: machineTransitionCount,
						},
					}},
				}},
			},
		}
	}

	expected := state(10)
	actual := state(20)
	require.Empty(t, mutableStateDiff(expected, actual))

	actual.ExecutionInfo.StateMachineTimers[0].Scheduled = false
	require.Contains(t, mutableStateDiff(expected, actual), "scheduled")

	actual = state(20)
	actual.ExecutionInfo.StateMachineTimers[0].Infos[0].Data = []byte("different")
	require.Contains(t, mutableStateDiff(expected, actual), "data")
}

func TestForceSnapshotReplication(t *testing.T) {
	harness := NewHarness(log.NewNoopLogger())
	transition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          2,
	}
	require.Same(t, transition, harness.artifactStartTransition(transition))

	harness.ForceSnapshotReplication()
	require.Nil(t, harness.artifactStartTransition(transition))
}

func TestMutableStateDiffNormalizesEventRebuildFieldsOnlyForNewRun(t *testing.T) {
	state := func(branchToken []byte, requestID string) *persistencespb.WorkflowMutableState {
		return &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId: "workflow",
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{{BranchToken: branchToken}},
				},
				WorkflowTaskOriginalScheduledTime: timestamppb.New(time.Unix(1, 0)),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: requestID,
				RequestIds:      map[string]*persistencespb.RequestIDInfo{requestID: {}},
			},
		}
	}
	expected := state([]byte("active-branch"), "active-request")
	expected.ExecutionInfo.SubStateMachineTombstoneBatches = []*persistencespb.StateMachineTombstoneBatch{{}}
	actual := state([]byte("passive-branch"), "passive-request")
	actual.ExecutionInfo.VisibilityLastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          1,
	}
	actual.ExecutionInfo.WorkflowTaskOriginalScheduledTime = timestamppb.New(time.Unix(2, 0))

	require.NotEmpty(t, mutableStateDiff(expected, actual))
	require.Empty(t, mutableStateDiffWithOptions(expected, actual, true))

	actual.ExecutionInfo.WorkflowId = "different-workflow"
	require.Contains(t, mutableStateDiffWithOptions(expected, actual, true), "workflow_id")
}

func TestMissingTaskFingerprints(t *testing.T) {
	require.Empty(t, missingTaskFingerprints(
		[]string{"activity", "activity"},
		[]string{"activity", "activity", "passive-only"},
	))
	require.Equal(t, []string{"activity"}, missingTaskFingerprints(
		[]string{"activity", "activity"},
		[]string{"activity"},
	))
	require.Equal(t, []string{"passive-only"}, missingTaskFingerprints(
		[]string{"activity", "activity", "passive-only"},
		[]string{"activity", "activity"},
	))
}

func TestSyncVersionedTransitionTask(t *testing.T) {
	_, err := syncVersionedTransitionTask(nil)
	require.Error(t, err)

	expected := &historytasks.SyncVersionedTransitionTask{}
	tasksByCategory := map[historytasks.Category][]historytasks.Task{
		historytasks.CategoryReplication: {
			&historytasks.HistoryReplicationTask{},
			expected,
		},
	}

	actual, err := syncVersionedTransitionTask(tasksByCategory)
	require.NoError(t, err)
	require.Same(t, expected, actual)

	tasksByCategory[historytasks.CategoryReplication] = append(
		tasksByCategory[historytasks.CategoryReplication],
		&historytasks.SyncVersionedTransitionTask{},
	)
	_, err = syncVersionedTransitionTask(tasksByCategory)
	require.Error(t, err)
}

func TestTasksWithoutReplication(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("namespace", "workflow", "run")
	tasksByCategory := map[historytasks.Category][]historytasks.Task{
		historytasks.CategoryTransfer: {
			&historytasks.WorkflowTask{WorkflowKey: workflowKey},
		},
		historytasks.CategoryReplication: {
			&historytasks.SyncVersionedTransitionTask{WorkflowKey: workflowKey},
		},
	}

	filtered := tasksWithoutReplication(tasksByCategory)
	require.Contains(t, filtered, historytasks.CategoryTransfer)
	require.NotContains(t, filtered, historytasks.CategoryReplication)
	require.Contains(t, tasksByCategory, historytasks.CategoryReplication)
}

func TestTaskFingerprintsNormalizeOnlyPersistenceAndJitterFields(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("namespace", "workflow", "run")
	activity := func(taskID int64) map[historytasks.Category][]historytasks.Task {
		return map[historytasks.Category][]historytasks.Task{
			historytasks.CategoryTransfer: {&historytasks.ActivityTask{
				WorkflowKey:      workflowKey,
				TaskID:           taskID,
				TaskQueue:        "queue",
				ScheduledEventID: 7,
			}},
		}
	}
	first, err := taskFingerprints(activity(1))
	require.NoError(t, err)
	second, err := taskFingerprints(activity(2))
	require.NoError(t, err)
	require.Equal(t, first, second)

	deleteTask := func(visibilityTime time.Time) map[historytasks.Category][]historytasks.Task {
		return map[historytasks.Category][]historytasks.Task{
			historytasks.CategoryTimer: {&historytasks.DeleteHistoryEventTask{
				WorkflowKey:         workflowKey,
				VisibilityTimestamp: visibilityTime,
			}},
		}
	}
	first, err = taskFingerprints(deleteTask(time.Unix(1, 0)))
	require.NoError(t, err)
	second, err = taskFingerprints(deleteTask(time.Unix(2, 0)))
	require.NoError(t, err)
	require.Equal(t, first, second)

	userTimer := func(visibilityTime time.Time) map[historytasks.Category][]historytasks.Task {
		return map[historytasks.Category][]historytasks.Task{
			historytasks.CategoryTimer: {&historytasks.UserTimerTask{
				WorkflowKey:         workflowKey,
				VisibilityTimestamp: visibilityTime,
			}},
		}
	}
	first, err = taskFingerprints(userTimer(time.Unix(1, 0)))
	require.NoError(t, err)
	second, err = taskFingerprints(userTimer(time.Unix(2, 0)))
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestTaskFingerprintsNormalizeStickyWorkflowTaskForPassive(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("namespace", "workflow", "run")
	eventID := int64(7)
	active := map[historytasks.Category][]historytasks.Task{
		historytasks.CategoryTransfer: {&historytasks.WorkflowTask{
			WorkflowKey:      workflowKey,
			TaskQueue:        "sticky-queue",
			ScheduledEventID: eventID,
		}},
		historytasks.CategoryTimer: {&historytasks.WorkflowTaskTimeoutTask{
			WorkflowKey: workflowKey,
			EventID:     eventID,
			TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		}},
	}
	passive := map[historytasks.Category][]historytasks.Task{
		historytasks.CategoryTransfer: {&historytasks.WorkflowTask{
			WorkflowKey:      workflowKey,
			TaskQueue:        "normal-queue",
			ScheduledEventID: eventID,
		}},
	}

	stickyEvents := stickyWorkflowTaskEventIDs(active)
	activeFingerprints, err := taskFingerprintsForComparison(active, taskFingerprintOptions{
		stickyWorkflowTaskEvents: stickyEvents,
		ignoreStickyTimeouts:     true,
	})
	require.NoError(t, err)
	passiveFingerprints, err := taskFingerprintsForComparison(passive, taskFingerprintOptions{
		stickyWorkflowTaskEvents: stickyEvents,
	})
	require.NoError(t, err)
	require.Equal(t, activeFingerprints, passiveFingerprints)
}

func TestPassiveUpdateRunsClosedTransaction(t *testing.T) {
	harness := NewHarness(log.NewNoopLogger())
	prepared := false
	closed := false
	executed := false
	payload := &workflow.ExecutionTransactionPayload{}
	err := harness.InterceptUpdate(
		context.Background(),
		&workflow.TestHookUpdateExecutionRequest{
			UpdateExecutionTransactionPolicy: historyi.TransactionPolicyPassive,
			PrepareMutableStateTransaction: func() error {
				prepared = true
				return nil
			},
			CloseMutableStateTransaction: func() (*workflow.ExecutionTransactionPayload, error) {
				closed = true
				return payload, nil
			},
			ExecuteExecutionTransaction: func(actual *workflow.ExecutionTransactionPayload) error {
				executed = true
				require.Same(t, payload, actual)
				return nil
			},
		},
		func() error { return nil },
	)
	require.NoError(t, err)
	require.True(t, prepared)
	require.True(t, closed)
	require.True(t, executed)
	require.Equal(t, map[BailReason]int{BailPassivePolicy: 1}, harness.AllBailouts())
}

func TestTransientWorkflowContextForReplicationIsScopedToMarkedContext(t *testing.T) {
	harness := NewHarness(log.NewNoopLogger())
	require.False(t, harness.UseTransientWorkflowContextForReplication(context.Background()))

	ctx := context.WithValue(context.Background(), replicationApplyContextKey{}, replicationApplyContext{})
	require.True(t, harness.UseTransientWorkflowContextForReplication(ctx))
}
