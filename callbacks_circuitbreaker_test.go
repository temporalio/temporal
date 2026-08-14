package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// WorkerCallbacksCircuitBreakerSuite covers what happens when Worker-variant callback deliveries
// keep failing: the outbound queue's circuit breaker for the target task queue opens, and Describe
// reports the callback as BLOCKED instead of SCHEDULED.
//
// Completion callbacks are the same component whichever execution they hang off, so each case only
// differs in how the execution is started, completed, and described.
type WorkerCallbacksCircuitBreakerSuite struct {
	parallelsuite.Suite[*WorkerCallbacksCircuitBreakerSuite]
}

func TestWorkerCallbacksCircuitBreakerSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkerCallbacksCircuitBreakerSuite{})
}

// callbackStatus is the part of the Describe response every execution type reports the same way,
// under a different CallbackInfo proto.
type callbackStatus struct {
	state         enumspb.CallbackState
	blockedReason string
}

func (s *WorkerCallbacksCircuitBreakerSuite) TestBlockedWhenCircuitBreakerOpens() {
	// The callback retry policy is a global setting, and the test needs deliveries to be retried
	// fast enough to trip the breaker within the test's lifetime.
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),

		// Every execution type has its own gate for which callback kinds it accepts.
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true),
		testcore.WithDynamicConfig(callback.EnabledWorkflowCallbackKinds, []string{"nexus", "worker"}),
		testcore.WithDynamicConfig(callback.EnabledWorkflowUpdateCallbackKinds, []string{"nexus", "worker"}),
		testcore.WithDynamicConfig(activity.Enabled, true),
		testcore.WithDynamicConfig(activity.EnableCallbacks, true),
		testcore.WithDynamicConfig(activity.EnabledCallbackKinds, []string{"nexus", "worker"}),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnableCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus", "worker"}),

		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 10*time.Millisecond),
	)

	s.Run("Workflow", func(s *WorkerCallbacksCircuitBreakerSuite) {
		t := s.T()
		workflowID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		workflowType := "circuit-breaker-workflow"

		worker := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
		worker.RegisterWorkflowWithOptions(
			func(workflow.Context) error { return nil },
			workflow.RegisterOptions{Name: workflowType},
		)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          workflowID,
			WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:            t.Name(),
			CompletionCallbacks: []*commonpb.Callback{s.startFailingHandler(env, t)},
		})
		require.NoError(t, err)
		require.NoError(t, env.SdkClient().GetWorkflow(s.Context(), workflowID, "").Get(s.Context(), nil))

		s.awaitBlocked(func(ctx context.Context, t require.TestingT) callbackStatus {
			return s.describeWorkflowCallback(ctx, t, env, workflowID)
		})
	})

	s.Run("WorkflowUpdate", func(s *WorkerCallbacksCircuitBreakerSuite) {
		t := s.T()
		workflowID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		workflowType := "circuit-breaker-update-workflow"

		worker := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
		worker.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) error {
				if err := workflow.SetUpdateHandler(ctx, "update", func(workflow.Context) (string, error) {
					return "updated", nil
				}); err != nil {
					return err
				}
				// Stay running so the update's callback is the only one reported.
				workflow.GetSignalChannel(ctx, "never-sent").Receive(ctx, nil)
				return nil
			},
			workflow.RegisterOptions{Name: workflowType},
		)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   workflowID,
			WorkflowType: &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:     t.Name(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateWorkflowExecution(s.Context(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
			Request: &updatepb.Request{
				Meta:                &updatepb.Meta{UpdateId: "update-id"},
				Input:               &updatepb.Input{Name: "update"},
				RequestId:           uuid.NewString(),
				CompletionCallbacks: []*commonpb.Callback{s.startFailingHandler(env, t)},
			},
		})
		require.NoError(t, err)

		s.awaitBlocked(func(ctx context.Context, t require.TestingT) callbackStatus {
			return s.describeWorkflowCallback(ctx, t, env, workflowID)
		})
	})

	s.Run("StandaloneActivity", func(s *WorkerCallbacksCircuitBreakerSuite) {
		t := s.T()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{s.startFailingHandler(env, t)},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		s.awaitBlocked(func(ctx context.Context, t require.TestingT) callbackStatus {
			descResp, descErr := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(t, descErr)
			require.Len(t, descResp.GetCallbacks(), 1)
			info := descResp.GetCallbacks()[0].GetInfo()
			return callbackStatus{state: info.GetState(), blockedReason: info.GetBlockedReason()}
		})
	})

	s.Run("StandaloneNexusOperation", func(s *WorkerCallbacksCircuitBreakerSuite) {
		t := s.T()
		operationID := testcore.RandomizeStr(t.Name())
		endpointName := env.createSyncSuccessEndpoint(s.Context(), t, "operation-result")

		startResp, err := env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{s.startFailingHandler(env, t)},
		})
		require.NoError(t, err)
		require.True(t, startResp.GetStarted())

		s.awaitBlocked(func(ctx context.Context, t require.TestingT) callbackStatus {
			cbs := env.describeNexusOperation(ctx, t, operationID).GetCompletionCallbacks()
			require.Len(t, cbs, 1)
			info := cbs[0].GetInfo()
			return callbackStatus{state: info.GetState(), blockedReason: info.GetBlockedReason()}
		})
	})
}

// startFailingHandler starts a worker whose completion handler always fails with a retryable error,
// and returns a Worker-variant callback addressed to it. Each caller gets its own task queue, which
// is the destination the circuit breaker is keyed by, so cases cannot trip each other's breaker.
func (s *WorkerCallbacksCircuitBreakerSuite) startFailingHandler(env *NexusTestEnv, t *testing.T) *commonpb.Callback {
	t.Helper()

	taskQueue := testcore.RandomizeStr(t.Name() + "-callback-handler")
	service := nexus.NewService("completion-service")
	operation := nexus.NewSyncOperation(
		"on-complete",
		func(_ context.Context, _ *notificationpb.OnCompleteRequest, _ nexus.StartOperationOptions) (*notificationpb.OnCompleteResponse, error) {
			// A retryable handler error makes the delivery a failure against this task queue, which
			// is what the circuit breaker counts.
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
		},
	)
	require.NoError(t, service.Register(operation))

	worker := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
	worker.RegisterNexusService(service)
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)

	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: taskQueue,
				Service:       service.Name,
				Operation:     operation.Name(),
			},
		},
	}
}

func (s *WorkerCallbacksCircuitBreakerSuite) describeWorkflowCallback(
	ctx context.Context,
	t require.TestingT,
	env *NexusTestEnv,
	workflowID string,
) callbackStatus {
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	require.NoError(t, err)
	require.Len(t, descResp.GetCallbacks(), 1)
	info := descResp.GetCallbacks()[0]
	return callbackStatus{state: info.GetState(), blockedReason: info.GetBlockedReason()}
}

// awaitBlocked waits for enough deliveries to fail that the circuit breaker opens, at which point
// the callback is reported as blocked rather than scheduled. Deliveries are retried until then, so
// the callback passes through SCHEDULED and BACKING_OFF on the way.
func (s *WorkerCallbacksCircuitBreakerSuite) awaitBlocked(
	describe func(context.Context, require.TestingT) callbackStatus,
) {
	s.T().Helper()

	await.Require(s.Context(), s.T(), func(c *await.T) {
		status := describe(c.Context(), c)
		require.Equal(c, enumspb.CALLBACK_STATE_BLOCKED, status.state)
		require.Equal(c, "The circuit breaker is open.", status.blockedReason)
	}, 10*time.Second, 200*time.Millisecond)
}
