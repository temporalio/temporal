package passivepath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

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
