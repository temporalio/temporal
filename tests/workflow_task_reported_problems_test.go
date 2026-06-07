package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WFTFailureReportedProblemsTestSuite struct {
	parallelsuite.Suite[*WFTFailureReportedProblemsTestSuite]
}

func TestWFTFailureReportedProblemsTestSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &WFTFailureReportedProblemsTestSuite{})
}

type internalWFTProblemsTestWorkflow struct {
	env        *testcore.TestEnv
	shouldFail atomic.Bool
}

func newInternalWFTProblemsTestWorkflow(env *testcore.TestEnv) *internalWFTProblemsTestWorkflow {
	return &internalWFTProblemsTestWorkflow{env: env}
}

func (w *internalWFTProblemsTestWorkflow) SimpleWorkflowWithShouldFail(_ workflow.Context) (string, error) {
	if w.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}
	return "done!", nil
}

func (w *internalWFTProblemsTestWorkflow) SimpleActivity() (string, error) {
	return "done!", nil
}

// WorkflowWithSignalsThatFails creates a workflow that listens for signals and fails on each workflow task.
// This is used to test that the TemporalReportedProblems search attribute is not incorrectly removed
// when signals keep coming in despite continuous workflow task failures.
func (w *internalWFTProblemsTestWorkflow) WorkflowWithSignalsThatFails(ctx workflow.Context) (string, error) {
	// Signal ourselves to create buffered events
	err := w.env.SdkClient().SignalWorkflow(context.Background(), workflow.GetInfo(ctx).WorkflowExecution.ID, "", "test-signal", "self-signal")
	if err != nil {
		return "", err
	}
	panic("forced-panic-after-self-signal")
}

// WorkflowWithActivity creates a workflow that executes an activity before potentially failing.
// This is used to test workflow task failure scenarios in a more realistic context where the workflow
// has already executed some operations (activities) before encountering a workflow task failure.
// The activity itself succeeds, but the workflow task may fail afterward, which triggers the server
// to clear the sticky task queue and transition to a normal task queue for subsequent workflow tasks.
func (w *internalWFTProblemsTestWorkflow) WorkflowWithActivity(ctx workflow.Context) (string, error) {
	var ret string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
	}), w.SimpleActivity).Get(ctx, &ret)
	if err != nil {
		return "", err
	}

	if w.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}

	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) newWFTProblemsEnv() *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2),
	)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear() {
	env := s.newWFTProblemsEnv()

	tw := newInternalWFTProblemsTestWorkflow(env)
	tw.shouldFail.Store(true)

	env.SdkWorker().RegisterWorkflow(tw.SimpleWorkflowWithShouldFail)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, tw.SimpleWorkflowWithShouldFail)
	s.NoError(err)

	// Check if the search attributes are not empty and has TemporalReportedProblems
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saVal)
		require.Contains(t, saVal, "category=WorkflowTaskFailed")
		require.Contains(t, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")

		execution, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, execution.PendingWorkflowTask.Attempt, int32(2))
	}, 20*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	tw.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(s.Context(), &out))

	description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_NotClearedBySignals() {
	env := s.newWFTProblemsEnv()

	tw := newInternalWFTProblemsTestWorkflow(env)

	env.SdkWorker().RegisterWorkflow(tw.WorkflowWithSignalsThatFails)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, tw.WorkflowWithSignalsThatFails)
	s.NoError(err)

	// The workflow will signal itself and panic on each WFT, creating buffered events naturally.
	// Wait for the search attribute to be set due to consecutive failures
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saVal)
		require.Contains(t, saVal, "category=WorkflowTaskFailed")
		require.Contains(t, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 20*time.Second, 500*time.Millisecond)

	// Validate the workflow history shows the repeating pattern:
	// signal -> task scheduled -> task started -> task failed
	// This demonstrates that signals are being buffered between workflow task failures.
	s.EventuallyWithT(func(t *assert.CollectT) {
		var events []*historypb.HistoryEvent
		iter := env.SdkClient().GetWorkflowHistory(s.Context(), workflowRun.GetID(), workflowRun.GetRunID(), false, 0)
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)
			events = append(events, event)
		}

		// Validate the expected pattern structure showing repeated cycles of task failures and signals
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed
  9 WorkflowExecutionSignaled`, events[:9])
	}, 10*time.Second, 500*time.Millisecond)

	// Verify the search attribute persists even as the workflow continues to fail and create buffered events
	// This is the key part of the test - buffered events should not cause the search attribute to be cleared
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		s.True(ok, "Search attribute should still be present during continued failures")
		s.NotEmpty(saVal, "Search attribute should not be empty during continued failures")
	}, 5*time.Second, 500*time.Millisecond)

	// Terminate the workflow for cleanup
	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID(), "test cleanup"))
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear_FailAfterActivity() {
	env := s.newWFTProblemsEnv()

	tw := newInternalWFTProblemsTestWorkflow(env)
	tw.shouldFail.Store(true)

	env.SdkWorker().RegisterWorkflow(tw.WorkflowWithActivity)
	env.SdkWorker().RegisterActivity(tw.SimpleActivity)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, tw.WorkflowWithActivity)
	s.NoError(err)

	// Validate the search attributes are not empty and has TemporalReportedProblems with 2 entries
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 20*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	tw.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(s.Context(), &out))

	description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_DynamicConfigChanges() {
	env := s.newWFTProblemsEnv()

	cleanup := env.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 0)
	defer cleanup()

	tw := newInternalWFTProblemsTestWorkflow(env)
	tw.shouldFail.Store(true)

	env.SdkWorker().RegisterWorkflow(tw.SimpleWorkflowWithShouldFail)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, tw.SimpleWorkflowWithShouldFail)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)

		exec, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, exec.PendingWorkflowTask.Attempt, int32(2))
	}, 10*time.Second, 500*time.Millisecond)

	cleanup2 := env.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
	defer cleanup2()

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 15*time.Second, 500*time.Millisecond)

	tw.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(s.Context(), &out))

	description, err := env.SdkClient().DescribeWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}
