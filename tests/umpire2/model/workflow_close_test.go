package model

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

func TestWorkflowRunRecordsEveryCloseOutcomeAtEventTime(t *testing.T) {
	for _, test := range []struct {
		outcome     string
		state       string
		disposition umpire.Disposition
	}{
		{outcome: telemetry.WorkflowCloseOutcomeCompleted, state: WorkflowRunCompleted, disposition: umpire.Success},
		{outcome: telemetry.WorkflowCloseOutcomeFailed, state: WorkflowRunFailed, disposition: umpire.Failure},
		{outcome: telemetry.WorkflowCloseOutcomeCanceled, state: WorkflowRunCanceled},
		{outcome: telemetry.WorkflowCloseOutcomeTerminated, state: WorkflowRunTerminated},
		{outcome: telemetry.WorkflowCloseOutcomeTimedOut, state: WorkflowRunTimedOut, disposition: umpire.Failure},
		{outcome: telemetry.WorkflowCloseOutcomeContinuedAsNew, state: WorkflowRunContinuedAsNew},
	} {
		t.Run(test.outcome, func(t *testing.T) {
			closedAt := time.Date(2026, time.August, 12, 13, 0, 0, 0, time.UTC)
			run := NewWorkflowRun()
			closed := workflowRunClosedFact(test.outcome, "successor-run", closedAt)

			require.NoError(t, run.OnFact(context.Background(), closed.TargetEntity(), slices.Values([]umpire.Fact{closed})))

			require.Equal(t, test.state, run.FSM.Current())
			require.Equal(t, test.disposition, run.FSM.CurrentDisposition())
			require.Equal(t, closedAt, run.ClosedAt)
			require.Equal(t, test.outcome, run.CloseOutcome)
			require.Equal(t, "successor-run", run.SuccessorRunID)
		})
	}
}

func TestWorkflowRunCloseDuplicateIsNoOpAndContradictionIsIllegal(t *testing.T) {
	closedAt := time.Date(2026, time.August, 12, 13, 0, 0, 0, time.UTC)
	run := NewWorkflowRun()
	completed := workflowRunClosedFact(telemetry.WorkflowCloseOutcomeCompleted, "", closedAt)

	require.NoError(t, run.OnFact(context.Background(), completed.TargetEntity(), slices.Values([]umpire.Fact{completed, completed})))
	require.Empty(t, run.FSM.Illegal())

	failed := workflowRunClosedFact(telemetry.WorkflowCloseOutcomeFailed, "", closedAt.Add(time.Second))
	require.NoError(t, run.OnFact(context.Background(), failed.TargetEntity(), slices.Values([]umpire.Fact{failed})))
	require.Equal(t, WorkflowRunCompleted, run.FSM.Current())
	require.Len(t, run.FSM.Illegal(), 1)
	require.Equal(t, closedAt.Add(time.Second), run.FSM.Illegal()[0].At)
}

func TestWorkflowClosesOnlyWhenRunHasNoSuccessor(t *testing.T) {
	workflow := NewWorkflow()
	workflow.FSM.SetState(WorkflowStarted)
	intermediateAt := time.Date(2026, time.August, 12, 13, 0, 0, 0, time.UTC)
	intermediate := workflowClosedFact(telemetry.WorkflowCloseOutcomeContinuedAsNew, "next-run", intermediateAt)

	require.NoError(t, workflow.OnFact(context.Background(), intermediate.TargetEntity(), slices.Values([]umpire.Fact{intermediate})))
	require.Equal(t, WorkflowStarted, workflow.FSM.Current())

	finalAt := intermediateAt.Add(time.Second)
	final := workflowClosedFact(telemetry.WorkflowCloseOutcomeFailed, "", finalAt)
	require.NoError(t, workflow.OnFact(context.Background(), final.TargetEntity(), slices.Values([]umpire.Fact{final})))
	require.Equal(t, WorkflowFailed, workflow.FSM.Current())
	require.Equal(t, finalAt, workflow.ClosedAt)
}

func workflowRunClosedFact(outcome, successor string, eventTime time.Time) *fact.WorkflowRunClosed {
	closed := &fact.WorkflowRunClosed{}
	closed.NamespaceID = "namespace-id"
	closed.WorkflowID = "workflow-id"
	closed.RunID = "run-id"
	closed.Outcome = outcome
	closed.SuccessorRunID = successor
	closed.SetEventTime(eventTime)
	closed.EntityPath = &umpire.EntityPath{
		EntityID: umpire.NewEntityID(WorkflowRunType, "run-id"),
		Ancestors: []umpire.EntityID{
			umpire.NewEntityID(NamespaceType, "namespace-id"),
			umpire.NewEntityID(WorkflowType, "workflow-id"),
		},
	}
	return closed
}

func workflowClosedFact(outcome, successor string, eventTime time.Time) *fact.WorkflowExecutionClosed {
	closed := &fact.WorkflowExecutionClosed{}
	closed.NamespaceID = "namespace-id"
	closed.WorkflowID = "workflow-id"
	closed.RunID = "run-id"
	closed.Outcome = outcome
	closed.SuccessorRunID = successor
	closed.SetEventTime(eventTime)
	closed.EntityPath = &umpire.EntityPath{
		EntityID:  umpire.NewEntityID(WorkflowType, "workflow-id"),
		Ancestors: []umpire.EntityID{umpire.NewEntityID(NamespaceType, "namespace-id")},
	}
	return closed
}
