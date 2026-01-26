package tests

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

func TestMaxBufferedEvent(t *testing.T) {
	t.Run("MaxBufferedEventsLimit", func(t *testing.T) {
		/*
			This test starts a workflow, and block its workflow task, then sending
			signals to it which will be buffered. The default max buffered event
			count limit is 100. When the test sends 101 signal, the blocked workflow
			task will be forced to close.
		*/
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.MaximumBufferedEventsSizeInBytes, 10*1024*1024), // 10MB
			testcore.WithDynamicConfig(dynamicconfig.MutableStateSizeLimitWarn, 200),
			testcore.WithDynamicConfig(dynamicconfig.MutableStateSizeLimitError, 410*1024), // 410KB
		)

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		closeStartChanOnce := sync.Once{}
		waitStartChan := make(chan struct{})
		waitSignalChan := make(chan struct{})

		localActivityFn := func(ctx context.Context) error {
			// notify that workflow task has started
			closeStartChanOnce.Do(func() {
				close(waitStartChan)
			})

			// block workflow task so all signals will be buffered.
			<-waitSignalChan
			return nil
		}

		workflowFn := func(ctx workflow.Context) (int, error) {
			ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
				StartToCloseTimeout: 20 * time.Second,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
			})
			f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
			if err := f1.Get(ctx, nil); err != nil {
				return 0, err
			}

			sigCh := workflow.GetSignalChannel(ctx, "test-signal")

			sigCount := 0
			for sigCh.ReceiveAsync(nil) {
				sigCount++
			}
			return sigCount, nil
		}

		worker.RegisterWorkflow(workflowFn)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		testCtx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()

		wid := "test-max-buffered-events-limit"
		wf1, err1 := sdkClient.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
			ID:                  wid,
			TaskQueue:           taskQueue,
			WorkflowTaskTimeout: time.Second * 20,
		}, workflowFn)

		require.NoError(t, err1)

		// block until workflow task started
		<-waitStartChan

		// now send 100 signals, all of them will be buffered
		for i := 0; i < 100; i++ {
			err := sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", i)
			require.NoError(t, err)
		}

		// send 101 signal, this will fail the started workflow task
		err = sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", 100)
		require.NoError(t, err)

		// unblock goroutine that runs local activity
		close(waitSignalChan)

		var sigCount int
		err = wf1.Get(testCtx, &sigCount)
		require.NoError(t, err)
		require.Equal(t, 101, sigCount)

		historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: wf1.GetID()})
		// Not using historyrequire here because history is not deterministic.
		var failedCause enumspb.WorkflowTaskFailedCause
		var failedCount int
		for _, evt := range historyEvents {
			if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
				failedCause = evt.GetWorkflowTaskFailedEventAttributes().Cause
				failedCount++
			}
		}
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND, failedCause)
		require.Equal(t, 1, failedCount)
	})

	t.Run("BufferedEventsMutableStateSizeLimit", func(t *testing.T) {
		/*
			This test starts a workflow, and blocks its workflow task, then sends
			signals to it which will be buffered. The test is configured with
			MaximumBufferedEventsSizeInBytes set to 10MB (high) and MutableStateSizeLimitError
			set to 410KB (low). Each signal has a 100KB payload. The first three signals
			succeed, and the fourth signal causes the mutable state size to exceed the limit,
			resulting in workflow termination.
		*/
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.MaximumBufferedEventsSizeInBytes, 10*1024*1024), // 10MB
			testcore.WithDynamicConfig(dynamicconfig.MutableStateSizeLimitWarn, 200),
			testcore.WithDynamicConfig(dynamicconfig.MutableStateSizeLimitError, 410*1024), // 410KB
		)

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		closeStartChanOnce := sync.Once{}
		waitStartChan := make(chan struct{})
		waitSignalChan := make(chan struct{})

		localActivityFn := func(ctx context.Context) error {
			// notify that workflow task has started
			closeStartChanOnce.Do(func() {
				close(waitStartChan)
			})

			// block workflow task so all signals will be buffered.
			<-waitSignalChan
			return nil
		}

		workflowFn := func(ctx workflow.Context) (int, error) {
			ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
				StartToCloseTimeout: 20 * time.Second,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
			})
			f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
			if err := f1.Get(ctx, nil); err != nil {
				return 0, err
			}

			sigCh := workflow.GetSignalChannel(ctx, "test-signal")

			sigCount := 0
			for sigCh.ReceiveAsync(nil) {
				sigCount++
			}
			return sigCount, nil
		}

		worker.RegisterWorkflow(workflowFn)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		testCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()

		wid := "test-max-buffered-events-limit"
		wf1, err1 := sdkClient.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
			ID:                  wid,
			TaskQueue:           taskQueue,
			WorkflowTaskTimeout: time.Second * 20,
		}, workflowFn)

		require.NoError(t, err1)

		// block until workflow task started
		<-waitStartChan

		// now send signals with 100KB payload each, which will be buffered
		buf := make([]byte, 100*1024) // 100KB
		// fill the slice with random data to make sure the
		// encoder does not zero out the data
		_, err = rand.Read(buf)
		require.NoError(t, err)
		largePayload := payloads.EncodeBytes(buf)

		// Send signals until mutable state size limit is exceeded
		// With 410KB limit and 100KB payloads, the first 3 signals succeed but the 4th exceeds the limit
		// First three signals should succeed
		for i := 0; i < 3; i++ {
			err = sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", largePayload)
			require.NoError(t, err, "Signal %d should succeed", i+1)
		}

		// Fourth signal should fail due to mutable state size limit
		err = sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", largePayload)
		require.Error(t, err, "Fourth signal should fail due to mutable state size limit")
		require.Contains(t, err.Error(), "mutable state size exceeds limit", "Expected mutable state size limit error")

		// unblock goroutine that runs local activity
		close(waitSignalChan)

		var sigCount int
		err = wf1.Get(testCtx, &sigCount)
		// The workflow should be terminated, so we expect an error
		require.Error(t, err)

		historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: wf1.GetID()})

		// Verify that the workflow was terminated due to mutable state size limit
		var terminated bool
		var terminationReason string
		for _, evt := range historyEvents {
			if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
				terminated = true
				attrs := evt.GetWorkflowExecutionTerminatedEventAttributes()
				terminationReason = attrs.GetReason()
				break
			}
		}
		require.True(t, terminated, "Expected workflow to be terminated")
		require.Equal(t, common.FailureReasonMutableStateSizeExceedsLimit, terminationReason,
			"Expected workflow to be terminated due to mutable state size limit")
	})
}
