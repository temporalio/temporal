package passivepath

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/parallelsuite"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type HarnessSuite struct {
	parallelsuite.Suite[*HarnessSuite]
}

func TestHarnessSuite(t *testing.T) { parallelsuite.Run(t, &HarnessSuite{}) }

func (s *HarnessSuite) TestMissingTaskFingerprints() {
	s.Empty(missingTaskFingerprints(
		[]string{"activity", "activity"},
		[]string{"activity", "activity", "passive-only"},
	))
	s.Equal([]string{"activity"}, missingTaskFingerprints(
		[]string{"activity", "activity"},
		[]string{"activity"},
	))
	s.Equal([]string{"passive-only"}, missingTaskFingerprints(
		[]string{"activity", "activity", "passive-only"},
		[]string{"activity", "activity"},
	))
}

func (s *HarnessSuite) TestTaskFingerprintsNormalizeOnlyPersistenceAndJitterFields() {
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
	s.NoError(err)
	second, err := taskFingerprints(activity(2))
	s.NoError(err)
	s.Equal(first, second)

	deleteTask := func(visibilityTime time.Time) map[historytasks.Category][]historytasks.Task {
		return map[historytasks.Category][]historytasks.Task{
			historytasks.CategoryTimer: {&historytasks.DeleteHistoryEventTask{
				WorkflowKey:         workflowKey,
				VisibilityTimestamp: visibilityTime,
			}},
		}
	}
	first, err = taskFingerprints(deleteTask(time.Unix(1, 0)))
	s.NoError(err)
	second, err = taskFingerprints(deleteTask(time.Unix(2, 0)))
	s.NoError(err)
	s.Equal(first, second)

	userTimer := func(visibilityTime time.Time) map[historytasks.Category][]historytasks.Task {
		return map[historytasks.Category][]historytasks.Task{
			historytasks.CategoryTimer: {&historytasks.UserTimerTask{
				WorkflowKey:         workflowKey,
				VisibilityTimestamp: visibilityTime,
			}},
		}
	}
	first, err = taskFingerprints(userTimer(time.Unix(1, 0)))
	s.NoError(err)
	second, err = taskFingerprints(userTimer(time.Unix(2, 0)))
	s.NoError(err)
	s.NotEqual(first, second)
}

func (s *HarnessSuite) TestTaskFingerprintsNormalizeStickyWorkflowTaskForPassive() {
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
	s.NoError(err)
	passiveFingerprints, err := taskFingerprintsForComparison(passive, taskFingerprintOptions{
		stickyWorkflowTaskEvents: stickyEvents,
	})
	s.NoError(err)
	s.Equal(activeFingerprints, passiveFingerprints)
}

func (s *HarnessSuite) TestPassiveUpdateRunsClosedTransaction() {
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
				s.Same(payload, actual)
				return nil
			},
		},
		func() error { return nil },
	)
	s.NoError(err)
	s.True(prepared)
	s.True(closed)
	s.True(executed)
	s.Equal(map[BailReason]int{BailPassivePolicy: 1}, harness.AllBailouts())
}

func (s *HarnessSuite) TestTransientWorkflowContextForReplicationIsScopedToMarkedContext() {
	harness := NewHarness(log.NewNoopLogger())
	s.False(harness.UseTransientWorkflowContextForReplication(context.Background()))

	ctx := context.WithValue(context.Background(), replicationApplyContextKey{}, replicationApplyContext{})
	s.True(harness.UseTransientWorkflowContextForReplication(ctx))
}
