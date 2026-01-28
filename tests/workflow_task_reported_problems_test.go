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
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/tests/testcore"
)

func TestWFTFailureReportedProblems(t *testing.T) {
	t.Run("SetAndClear", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var shouldFail atomic.Bool
		shouldFail.Store(true)

		// Workflow that panics when shouldFail is true
		simpleWorkflowWithShouldFail := func(ctx workflow.Context) (string, error) {
			if shouldFail.Load() {
				panic("forced-panic-to-fail-wft")
			}
			return "done!", nil
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(simpleWorkflowWithShouldFail)
		require.NoError(t, w.Start())
		defer w.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, simpleWorkflowWithShouldFail)
		require.NoError(t, err)

		// Check if the search attributes are not empty and has TemporalReportedProblems
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.True(ct, ok)
			require.NotEmpty(ct, saVal)
			require.Contains(ct, saVal, "category=WorkflowTaskFailed")
			require.Contains(ct, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")

			execution, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.GreaterOrEqual(ct, execution.PendingWorkflowTask.Attempt, int32(2))
		}, 20*time.Second, 500*time.Millisecond)

		// Unblock the workflow
		shouldFail.Store(false)

		var out string
		require.NoError(t, workflowRun.Get(ctx, &out))

		description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.TypedSearchAttributes)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	})

	t.Run("NotClearedBySignals", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		// Workflow that signals itself and then panics
		workflowWithSignalsThatFails := func(ctx workflow.Context) (string, error) {
			// Signal ourselves to create buffered events
			err := sdkClient.SignalWorkflow(context.Background(), workflow.GetInfo(ctx).WorkflowExecution.ID, "", "test-signal", "self-signal")
			if err != nil {
				return "", err
			}
			panic("forced-panic-after-self-signal")
		}

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(workflowWithSignalsThatFails)
		require.NoError(t, w.Start())
		defer w.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowWithSignalsThatFails)
		require.NoError(t, err)

		// The workflow will signal itself and panic on each WFT, creating buffered events naturally.
		// Wait for the search attribute to be set due to consecutive failures
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.True(ct, ok)
			require.NotEmpty(ct, saVal)
			require.Contains(ct, saVal, "category=WorkflowTaskFailed")
			require.Contains(ct, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
		}, 20*time.Second, 500*time.Millisecond)

		// Validate the workflow history shows the repeating pattern:
		// signal -> task scheduled -> task started -> task failed
		// This demonstrates that signals are being buffered between workflow task failures.
		s.EventuallyWithT(func(ct *assert.CollectT) {
			var events []*historypb.HistoryEvent
			iter := sdkClient.GetWorkflowHistory(ctx, workflowRun.GetID(), workflowRun.GetRunID(), false, 0)
			for iter.HasNext() {
				event, err := iter.Next()
				require.NoError(ct, err)
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
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.True(ct, ok, "Search attribute should still be present during continued failures")
			require.NotEmpty(ct, saVal, "Search attribute should not be empty during continued failures")
		}, 5*time.Second, 500*time.Millisecond)

		// Terminate the workflow for cleanup
		require.NoError(t, sdkClient.TerminateWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID(), "test cleanup"))
	})

	t.Run("SetAndClear_FailAfterActivity", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var shouldFail atomic.Bool
		shouldFail.Store(true)

		// Simple activity that returns success
		simpleActivity := func() (string, error) {
			return "done!", nil
		}

		// Workflow that executes an activity then potentially fails
		workflowWithActivity := func(ctx workflow.Context) (string, error) {
			var ret string
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second,
			}), simpleActivity).Get(ctx, &ret)
			if err != nil {
				return "", err
			}

			if shouldFail.Load() {
				panic("forced-panic-to-fail-wft")
			}

			return "done!", nil
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(workflowWithActivity)
		w.RegisterActivity(simpleActivity)
		require.NoError(t, w.Start())
		defer w.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowWithActivity)
		require.NoError(t, err)

		// Validate the search attributes are not empty and has TemporalReportedProblems with 2 entries
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.True(ct, ok)
			require.NotEmpty(ct, saValues)
			require.Len(ct, saValues, 2)
			require.Contains(ct, saValues, "category=WorkflowTaskFailed")
			require.Contains(ct, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
		}, 20*time.Second, 500*time.Millisecond)

		// Unblock the workflow
		shouldFail.Store(false)

		var out string
		require.NoError(t, workflowRun.Get(ctx, &out))

		description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.TypedSearchAttributes)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	})

	t.Run("DynamicConfigChanges", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 0),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var shouldFail atomic.Bool
		shouldFail.Store(true)

		// Workflow that panics when shouldFail is true
		simpleWorkflowWithShouldFail := func(ctx workflow.Context) (string, error) {
			if shouldFail.Load() {
				panic("forced-panic-to-fail-wft")
			}
			return "done!", nil
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(simpleWorkflowWithShouldFail)
		require.NoError(t, w.Start())
		defer w.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, simpleWorkflowWithShouldFail)
		require.NoError(t, err)

		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.False(ct, ok)

			exec, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.GreaterOrEqual(ct, exec.PendingWorkflowTask.Attempt, int32(2))
		}, 10*time.Second, 500*time.Millisecond)

		s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)

		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.True(ct, ok)
			require.NotEmpty(ct, saValues)
			require.Len(ct, saValues, 2)
			require.Contains(ct, saValues, "category=WorkflowTaskFailed")
			require.Contains(ct, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
		}, 15*time.Second, 500*time.Millisecond)

		shouldFail.Store(false)

		var out string
		require.NoError(t, workflowRun.Get(ctx, &out))

		description, err := sdkClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.TypedSearchAttributes)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	})
}
