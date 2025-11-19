package tests

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

type MaxBufferedEventSuite struct {
	testcore.FunctionalTestBase
}

func TestMaxBufferedEventSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(MaxBufferedEventSuite))
}

func (s *MaxBufferedEventSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// Set MaximumBufferedEventsSizeInBytes high so we don't hit that limit
		dynamicconfig.MaximumBufferedEventsSizeInBytes.Key(): 10 * 1024 * 1024, // 10MB
		// Set MutableStateSizeLimitError low so buffered events exhaust mutable state size
		dynamicconfig.MutableStateSizeLimitWarn.Key():  200,
		dynamicconfig.MutableStateSizeLimitError.Key(): 410 * 1024, // 410KB
	}
	s.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *MaxBufferedEventSuite) TestMaxBufferedEventsLimit() {
	/*
		This test starts a workflow, and block its workflow task, then sending
		signals to it which will be buffered. The default max buffered event
		count limit is 100. When the test sends 101 signal, the blocked workflow
		task will be forced to close.
	*/
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

	s.Worker().RegisterWorkflow(workflowFn)

	testCtx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	wid := "test-max-buffered-events-limit"
	wf1, err1 := s.SdkClient().ExecuteWorkflow(testCtx, client.StartWorkflowOptions{
		ID:                  wid,
		TaskQueue:           s.TaskQueue(),
		WorkflowTaskTimeout: time.Second * 20,
	}, workflowFn)

	s.NoError(err1)

	// block until workflow task started
	<-waitStartChan

	// now send 100 signals, all of them will be buffered
	for i := 0; i < 100; i++ {
		err := s.SdkClient().SignalWorkflow(testCtx, wid, "", "test-signal", i)
		s.NoError(err)
	}

	// send 101 signal, this will fail the started workflow task
	err := s.SdkClient().SignalWorkflow(testCtx, wid, "", "test-signal", 100)
	s.NoError(err)

	// unblock goroutine that runs local activity
	close(waitSignalChan)

	var sigCount int
	err = wf1.Get(testCtx, &sigCount)
	s.NoError(err)
	s.Equal(101, sigCount)

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
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND, failedCause)
	s.Equal(1, failedCount)
}

func (s *MaxBufferedEventSuite) TestBufferedEventsMutableStateSizeLimit() {
	/*
		This test starts a workflow, and blocks its workflow task, then sends
		signals to it which will be buffered. The test is configured with
		MaximumBufferedEventsSizeInBytes set to 10MB (high) and MutableStateSizeLimitError
		set to 410KB (low). Each signal has a 100KB payload. The first three signals
		succeed, and the fourth signal causes the mutable state size to exceed the limit,
		resulting in workflow termination.
	*/
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

	s.Worker().RegisterWorkflow(workflowFn)

	testCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	wid := "test-max-buffered-events-limit"
	wf1, err1 := s.SdkClient().ExecuteWorkflow(testCtx, client.StartWorkflowOptions{
		ID:                  wid,
		TaskQueue:           s.TaskQueue(),
		WorkflowTaskTimeout: time.Second * 20,
	}, workflowFn)

	s.NoError(err1)

	// block until workflow task started
	<-waitStartChan

	// now send signals with 100KB payload each, which will be buffered
	buf := make([]byte, 100*1024) // 100KB
	// fill the slice with random data to make sure the
	// encoder does not zero out the data
	_, err := rand.Read(buf)
	s.NoError(err)
	largePayload := payloads.EncodeBytes(buf)

	// Send signals until mutable state size limit is exceeded
	// With 410KB limit and 100KB payloads, the first 3 signals succeed but the 4th exceeds the limit
	// First three signals should succeed
	for i := 0; i < 3; i++ {
		err = s.SdkClient().SignalWorkflow(testCtx, wid, "", "test-signal", largePayload)
		s.NoError(err, "Signal %d should succeed", i+1)
	}

	// Fourth signal should fail due to mutable state size limit
	err = s.SdkClient().SignalWorkflow(testCtx, wid, "", "test-signal", largePayload)
	s.Error(err, "Fourth signal should fail due to mutable state size limit")
	s.Contains(err.Error(), "mutable state size exceeds limit", "Expected mutable state size limit error")

	// unblock goroutine that runs local activity
	close(waitSignalChan)

	var sigCount int
	err = wf1.Get(testCtx, &sigCount)
	// The workflow should be terminated, so we expect an error
	s.Error(err)

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
	s.True(terminated, "Expected workflow to be terminated")
	s.Equal(common.FailureReasonMutableStateSizeExceedsLimit, terminationReason,
		"Expected workflow to be terminated due to mutable state size limit")
}
