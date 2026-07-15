package tests

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func experimentAlwaysFailActivity(_ context.Context) error {
	return errors.New("always fails")
}

func experimentBusyLoopWorkflow(ctx workflow.Context) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 1_000_000 * time.Hour,
		RetryPolicy: &sdktemporal.RetryPolicy{
			InitialInterval:    time.Hour,
			BackoffCoefficient: 1.0,
			MaximumAttempts:    0, // unlimited — the activity never stops failing
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	return workflow.ExecuteActivity(ctx, "ExperimentAlwaysFail").Get(ctx, nil)
}

// TestTimeSkipping_Experiment_BusyLoopFrequency measures the natural skip rate of a real
// busy loop (SDK worker + local server) with the circuit breaker disabled: an activity that
// always fails with a 1h retry backoff that time skipping fast-forwards to instantly.
func (s *TimeSkippingTestSuite) TestTimeSkipping_Experiment_BusyLoopFrequency() {
	const measureWindow = 5 * time.Second

	env := testcore.NewEnv(
		s.T(),
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true),
		// Disable the circuit breaker so we measure the *natural* busy-loop rate.
		testcore.WithDynamicConfig(dynamicconfig.TimeSkippingCircuitBreaker,
			dynamicconfig.TimeSkippingCircuitBreakerSettings{Window: time.Minute, MaxSkipsPerWindow: 0}),
		// EXPERIMENT KNOB: floors newly-created scheduled tasks to ~now+this; suspected 1s gate.
		testcore.WithDynamicConfig(dynamicconfig.TimerProcessorMaxTimeShift, 50*time.Millisecond),
	)

	env.SdkWorker().RegisterWorkflowWithOptions(experimentBusyLoopWorkflow, workflow.RegisterOptions{Name: "ExperimentBusyLoopWF"})
	env.SdkWorker().RegisterActivityWithOptions(experimentAlwaysFailActivity, activity.RegisterOptions{Name: "ExperimentAlwaysFail"})

	wfID := "busyloop-" + uuid.NewString()
	startResp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        &commonpb.WorkflowType{Name: "ExperimentBusyLoopWF"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue()},
		WorkflowRunTimeout:  durationpb.New(1_000_000 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)

	exec := &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: startResp.RunId}
	s.T().Cleanup(func() {
		_, _ = env.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: exec,
			Reason:            "experiment cleanup",
		})
	})

	start := time.Now()
	time.Sleep(measureWindow) //nolint:forbidigo
	elapsed := time.Since(start)

	history := env.GetHistory(env.Namespace().String(), exec)
	skips := 0
	for _, e := range history {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			skips++
		}
	}

	rate := float64(skips) / elapsed.Seconds()
	var perSkipMs float64
	if skips > 0 {
		perSkipMs = float64(elapsed.Microseconds()) / float64(skips) / 1000
	}
	s.T().Logf("BUSY LOOP EXPERIMENT: %d skips in %s => %.1f skips/sec, %.2f ms/skip (total history events: %d)",
		skips, elapsed.Round(time.Millisecond), rate, perSkipMs, len(history))
}
