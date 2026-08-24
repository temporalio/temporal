package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	chasmactivity "go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NexusHandler-variant completion callbacks are gated per execution type by an
// "enabledCallbackKinds" dynamic config setting, which never enables the NexusHandler kind by
// default. These tests exercise both sides of that gate for every execution type that accepts
// completion callbacks:
//
//   - without "nexusHandler" in the setting, attaching a NexusHandler callback is rejected up front;
//   - with "nexusHandler" added to the setting, the callback is accepted and registered on the
//     execution, is triggered when the execution closes, and then fails to be delivered.
//
// The delivery failure is expected: the server recognizes and persists the NexusHandler variant, but
// the invocation path that hands a completion to a Nexus handler is not implemented yet. Today
// that path rejects the NexusHandler variant before it can record an attempt, so the callback sits in
// SCHEDULED while its invocation task fails and is retried, until it is eventually DLQ'd.
// Once delivery is implemented, requireNexusHandlerCallbackRetriedWithoutDelivery becomes the place to
// assert real delivery.

const (
	nexusHandlerCallbackNotEnabledErr = "nexusHandler callbacks are not enabled for this execution type"

	nexusHandlerCallbackService   = "HTTPAdapter"
	nexusHandlerCallbackOperation = "DeliverAsWebhook"

	// nexusHandlerCallbackInvocationTaskType is the history task type the CHASM callback invocation task
	// reports itself as, used to pick its failures out of the task_errors metric.
	nexusHandlerCallbackInvocationTaskType = "OutboundActive.callback.invoke"

	// Number of failed invocation attempts that count as evidence the callback is being retried
	// rather than merely having been scheduled once.
	minNexusHandlerCallbackInvocationFailures = 2
)

// observedCallback normalizes the callback info reported by DescribeWorkflowExecution and
// DescribeActivityExecution, which use different (though near-identical) protos.
type observedCallback struct {
	callback *commonpb.Callback
	state    enumspb.CallbackState
	// trigger is only reported for workflow executions.
	trigger                 *workflowpb.CallbackInfo_Trigger
	attempt                 int32
	lastAttemptFailure      *failurepb.Failure
	lastAttemptCompleteTime *timestamppb.Timestamp
	nextAttemptScheduleTime *timestamppb.Timestamp
}

// describeCallbacksFn reads the callbacks currently attached to an execution.
type describeCallbacksFn func() ([]observedCallback, error)

func nexusHandlerCallback(taskQueue string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_NexusHandler_{
			NexusHandler: &commonpb.Callback_NexusHandler{
				TaskQueueName: taskQueue,
				Service:       nexusHandlerCallbackService,
				Operation:     nexusHandlerCallbackOperation,
			},
		},
	}
}

// requireNexusHandlerCallbackRegistered asserts that the execution carries exactly the one NexusHandler
// callback that was attached to it.
func requireNexusHandlerCallbackRegistered(t require.TestingT, cbs []observedCallback, taskQueue string) {
	require.Len(t, cbs, 1)
	nexusHandler := cbs[0].callback.GetNexusHandler()
	require.NotNil(t, nexusHandler, "callback should round-trip as the NexusHandler variant")
	require.Equal(t, taskQueue, nexusHandler.GetTaskQueueName())
	require.Equal(t, nexusHandlerCallbackService, nexusHandler.GetService())
	require.Equal(t, nexusHandlerCallbackOperation, nexusHandler.GetOperation())
}

// requireNexusHandlerCallbackTriggered waits for the callback to leave STANDBY, which happens once the
// execution it is attached to reaches its terminal state.
func requireNexusHandlerCallbackTriggered(t *testing.T, describe describeCallbacksFn, taskQueue string) {
	t.Helper()
	await.Require(testcontext.For(t), t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireNexusHandlerCallbackRegistered(c, cbs, taskQueue)
		require.NotEqual(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state,
			"callback should be triggered once the execution closes")
	}, 15*time.Second, 200*time.Millisecond)
}

// countCallbackInvocationFailures reports how many times the callback invocation task has failed
// in the test's namespace.
func countCallbackInvocationFailures(capture *testcore.NamespaceMetricCapture) int {
	return len(capture.CollectMetric("task_errors", func(rec *metricstest.CapturedRecording) bool {
		return rec.Tags[metrics.TaskTypeTagName] == nexusHandlerCallbackInvocationTaskType
	}))
}

// requireNexusHandlerCallbackRetriedWithoutDelivery waits for positive evidence that the callback's
// invocation task ran and was retried, then asserts the callback still has not been delivered.
//
// The retry evidence comes from the task_errors metric rather than from the callback itself:
// invocation rejects the NexusHandler variant before an attempt is recorded, so the callback's attempt,
// last_attempt_failure, and next_attempt_schedule_time all stay empty no matter how many times
// the task is retried.
func requireNexusHandlerCallbackRetriedWithoutDelivery(
	t *testing.T,
	capture *testcore.NamespaceMetricCapture,
	describe describeCallbacksFn,
) observedCallback {
	t.Helper()

	var last observedCallback
	await.Require(testcontext.For(t), t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		require.Len(c, cbs, 1)
		last = cbs[0]
		require.GreaterOrEqual(c, countCallbackInvocationFailures(capture), minNexusHandlerCallbackInvocationFailures,
			"the callback's invocation task should run and be retried")
	}, 30*time.Second, 200*time.Millisecond)

	require.NotEqual(t, enumspb.CALLBACK_STATE_SUCCEEDED, last.state,
		"NexusHandler callback delivery is not implemented; the callback must not report success")
	// Logged rather than asserted: exactly where a callback whose invocation never starts comes to
	// rest is an implementation detail of the unimplemented delivery path. The empty per-attempt
	// fields here are why the retry assertion above reads the task_errors metric instead.
	t.Logf("NexusHandler callback retried without delivery. "+
		"state=%s attempt=%d last_attempt_complete_time=%v last_attempt_failure=%v next_attempt_schedule_time=%v",
		last.state, last.attempt, last.lastAttemptCompleteTime, last.lastAttemptFailure, last.nextAttemptScheduleTime)
	return last
}

func TestNexusHandlerCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("Workflow", testNexusHandlerCallbackOnWorkflow)
	t.Run("WorkflowUpdate", testNexusHandlerCallbackOnWorkflowUpdate)
	t.Run("StandaloneActivity", testNexusHandlerCallbackOnStandaloneActivity)
	t.Run("StandaloneNexusOperation", testNexusHandlerCallbackOnStandaloneNexusOperation)
}

// testNexusHandlerCallbackOnWorkflow attaches a NexusHandler callback to a workflow execution via
// StartWorkflowExecution.
func testNexusHandlerCallbackOnWorkflow(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	workflowType := "nexushandler-callback-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: workflowType})

	cbTaskQueue := testcore.RandomizeStr("nexushandler-callback-workflow-completions")
	newStartRequest := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          testcore.RandomizeStr("nexushandler-callback-workflow"),
			WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			Identity:            t.Name(),
			CompletionCallbacks: []*commonpb.Callback{nexusHandlerCallback(cbTaskQueue)},
		}
	}

	// With the setting at its Nexus-only default, the callback is rejected before the workflow is
	// created.
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, newStartRequest())
	require.ErrorContains(t, err, nexusHandlerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmworkflow.EnabledCallbackKinds, []string{"nexus", "nexusHandler"})

	req := newStartRequest()
	_, err = env.FrontendClient().StartWorkflowExecution(ctx, req)
	require.NoError(t, err)

	describe := describeWorkflowCallbacks(ctx, env, req.WorkflowId, "")

	// The callback is registered on the running workflow, and has not been triggered yet.
	await.Require(ctx, t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireNexusHandlerCallbackRegistered(c, cbs, cbTaskQueue)
		require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state)
	}, 15*time.Second, 200*time.Millisecond)

	// Close the workflow, which triggers the callback.
	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, req.WorkflowId, "", "continue", nil))
	require.NoError(t, env.SdkClient().GetWorkflow(ctx, req.WorkflowId, "").Get(ctx, nil))

	requireNexusHandlerCallbackTriggered(t, describe, cbTaskQueue)

	cbInfo := requireNexusHandlerCallbackRetriedWithoutDelivery(t, capture, describe)
	require.NotNil(t, cbInfo.trigger.GetWorkflowClosed(),
		"callback should be triggered by the workflow closing")
}

// testNexusHandlerCallbackOnWorkflowUpdate attaches a NexusHandler callback to a workflow update via
// UpdateWorkflowExecution.
func testNexusHandlerCallbackOnWorkflowUpdate(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	const updateName = "update"
	const workflowType = "nexushandler-callback-update-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		if err := workflow.SetUpdateHandler(ctx, updateName, func(ctx workflow.Context) (string, error) {
			return "updated", nil
		}); err != nil {
			return err
		}
		workflow.GetSignalChannel(ctx, "stop").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: workflowType})

	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowType)
	require.NoError(t, err)

	cbTaskQueue := testcore.RandomizeStr("nexushandler-callback-update-completions")
	newUpdateRequest := func() *workflowservice.UpdateWorkflowExecutionRequest {
		return &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
			Request: &updatepb.Request{
				Meta:                &updatepb.Meta{UpdateId: uuid.NewString()},
				Input:               &updatepb.Input{Name: updateName},
				RequestId:           uuid.NewString(),
				CompletionCallbacks: []*commonpb.Callback{nexusHandlerCallback(cbTaskQueue)},
			},
		}
	}

	// With the setting at its Nexus-only default, the callback is rejected before the update is
	// admitted.
	_, err = env.FrontendClient().UpdateWorkflowExecution(ctx, newUpdateRequest())
	require.ErrorContains(t, err, nexusHandlerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmworkflow.EnabledCallbackKinds, []string{"nexus", "nexusHandler"})

	// The update runs to completion, which triggers the callback.
	updateResp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, newUpdateRequest())
	require.NoError(t, err)
	require.Equal(t,
		enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		updateResp.GetStage())

	describe := describeWorkflowCallbacks(ctx, env, run.GetID(), run.GetRunID())

	requireNexusHandlerCallbackTriggered(t, describe, cbTaskQueue)

	cbInfo := requireNexusHandlerCallbackRetriedWithoutDelivery(t, capture, describe)
	require.NotNil(t, cbInfo.trigger.GetUpdateWorkflowExecutionCompleted(),
		"callback should be triggered by the update completing")

	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

// testNexusHandlerCallbackOnStandaloneActivity attaches a NexusHandler callback to a standalone activity via
// StartActivityExecution.
func testNexusHandlerCallbackOnStandaloneActivity(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(chasmactivity.Enabled, true),
		testcore.WithDynamicConfig(chasmactivity.EnableCallbacks, true),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	activityID := testcore.RandomizeStr("nexushandler-callback-activity")
	taskQueue := testcore.RandomizeStr("nexushandler-callback-activity-tq")
	cbTaskQueue := testcore.RandomizeStr("nexushandler-callback-activity-completions")

	newStartRequest := func() *workflowservice.StartActivityExecutionRequest {
		return &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           uuid.NewString(),
			CompletionCallbacks: []*commonpb.Callback{nexusHandlerCallback(cbTaskQueue)},
		}
	}

	// With the setting at its Nexus-only default, the callback is rejected before the activity is
	// created.
	_, err := env.FrontendClient().StartActivityExecution(ctx, newStartRequest())
	require.ErrorContains(t, err, nexusHandlerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmactivity.EnabledCallbackKinds, []string{"nexus", "nexusHandler"})

	startResp, err := env.FrontendClient().StartActivityExecution(ctx, newStartRequest())
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	describe := describeActivityCallbacks(ctx, env, activityID, startResp.GetRunId())

	await.Require(ctx, t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireNexusHandlerCallbackRegistered(c, cbs, cbTaskQueue)
		require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state)
	}, 15*time.Second, 200*time.Millisecond)

	// Close the activity, which triggers the callback.
	pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  defaultIdentity,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pollResp.GetTaskToken())

	_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.GetTaskToken(),
		Result:    defaultResult,
		Identity:  defaultIdentity,
	})
	require.NoError(t, err)

	requireNexusHandlerCallbackTriggered(t, describe, cbTaskQueue)
	requireNexusHandlerCallbackRetriedWithoutDelivery(t, capture, describe)
}

// testWorkerCallbackOnStandaloneNexusOperation attaches a Worker callback to a standalone Nexus
// operation via StartNexusOperationExecution.
func testWorkerCallbackOnStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	// Unlike the other execution types, standalone Nexus operations accept no callback kinds by
	// default, so the Nexus-only baseline the rejection below exercises has to be set explicitly.
	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus"}),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	// A worker-target endpoint whose task queue nobody polls: the operation starts and then stays
	// open until the test terminates it.
	endpointName := env.createRandomNexusEndpoint(ctx, t).GetSpec().GetName()

	operationID := testcore.RandomizeStr("worker-callback-nexus-operation")
	cbTaskQueue := testcore.RandomizeStr("worker-callback-nexus-operation-completions")
	newStartRequest := func() *workflowservice.StartNexusOperationExecutionRequest {
		return &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           uuid.NewString(),
			CompletionCallbacks: []*commonpb.Callback{workerCallback(cbTaskQueue)},
		}
	}

	// With only the Nexus kind enabled, the callback is rejected before the operation is created.
	_, err := env.startNexusOperation(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus", "worker"})

	startResp, err := env.startNexusOperation(ctx, newStartRequest())
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	describe := describeNexusOperationCallbacks(ctx, env, operationID, startResp.GetRunId())

	// The callback is registered on the open operation, and has not been triggered yet.
	await.Require(ctx, t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireWorkerCallbackRegistered(c, cbs, cbTaskQueue)
		require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state)
	}, 15*time.Second, 200*time.Millisecond)

	// Every callback on a standalone operation is triggered by the operation completing. The trigger
	// is reported by a Nexus-operation-specific proto, so it is read here rather than through
	// observedCallback.
	cbInfos := env.describeNexusOperation(ctx, t, operationID).GetCompletionCallbacks()
	require.Len(t, cbInfos, 1)
	require.NotNil(t, cbInfos[0].GetTrigger().GetOperationCompleted())

	// Close the operation, which triggers the callback.
	_, err = env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: operationID,
		RunId:       startResp.GetRunId(),
		RequestId:   uuid.NewString(),
		Identity:    t.Name(),
		Reason:      "close the operation to trigger its completion callback",
	})
	require.NoError(t, err)

	requireWorkerCallbackTriggered(t, describe, cbTaskQueue)
	requireWorkerCallbackRetriedWithoutDelivery(t, capture, describe)
}

func describeWorkflowCallbacks(ctx context.Context, env *testcore.TestEnv, workflowID, runID string) describeCallbacksFn {
	return func() ([]observedCallback, error) {
		resp, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		if err != nil {
			return nil, err
		}
		cbs := make([]observedCallback, 0, len(resp.GetCallbacks()))
		for _, cb := range resp.GetCallbacks() {
			cbs = append(cbs, observedCallback{
				callback:                cb.GetCallback(),
				state:                   cb.GetState(),
				trigger:                 cb.GetTrigger(),
				attempt:                 cb.GetAttempt(),
				lastAttemptFailure:      cb.GetLastAttemptFailure(),
				lastAttemptCompleteTime: cb.GetLastAttemptCompleteTime(),
				nextAttemptScheduleTime: cb.GetNextAttemptScheduleTime(),
			})
		}
		return cbs, nil
	}
}

func describeActivityCallbacks(ctx context.Context, env *testcore.TestEnv, activityID, runID string) describeCallbacksFn {
	return func() ([]observedCallback, error) {
		resp, err := env.FrontendClient().DescribeActivityExecution(
			ctx,
			&workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
		if err != nil {
			return nil, err
		}
		cbs := make([]observedCallback, 0, len(resp.GetCallbacks()))
		for _, cb := range resp.GetCallbacks() {
			cbs = append(cbs, observedCallback{
				callback:                cb.GetInfo().GetCallback(),
				state:                   cb.GetInfo().GetState(),
				attempt:                 cb.GetInfo().GetAttempt(),
				lastAttemptFailure:      cb.GetInfo().GetLastAttemptFailure(),
				lastAttemptCompleteTime: cb.GetInfo().GetLastAttemptCompleteTime(),
				nextAttemptScheduleTime: cb.GetInfo().GetNextAttemptScheduleTime(),
			})
		}
		return cbs, nil
	}
}

func describeNexusOperationCallbacks(ctx context.Context, env *NexusTestEnv, operationID, runID string) describeCallbacksFn {
	return func() ([]observedCallback, error) {
		resp, err := env.FrontendClient().DescribeNexusOperationExecution(
			ctx,
			&workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: operationID,
				RunId:       runID,
			})
		if err != nil {
			return nil, err
		}
		cbs := make([]observedCallback, 0, len(resp.GetCompletionCallbacks()))
		for _, cb := range resp.GetCompletionCallbacks() {
			cbs = append(cbs, observedCallback{
				callback:                cb.GetInfo().GetCallback(),
				state:                   cb.GetInfo().GetState(),
				attempt:                 cb.GetInfo().GetAttempt(),
				lastAttemptFailure:      cb.GetInfo().GetLastAttemptFailure(),
				lastAttemptCompleteTime: cb.GetInfo().GetLastAttemptCompleteTime(),
				nextAttemptScheduleTime: cb.GetInfo().GetNextAttemptScheduleTime(),
			})
		}
		return cbs, nil
	}
}
