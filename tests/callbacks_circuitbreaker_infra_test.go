package tests

import (
	"context"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// callbackTarget reprsents the thing receiving the callback delivery. Failures reaching this target
// will open the circuit breaker. (e.g. an unavailable Nexus handler.)
type callbackTarget interface {
	// newCallback returns a new completion callback addressed to this destination.
	newCallback() *commonpb.Callback
	// stopFailing makes every delivery from here on succeed moving forward.
	stopFailing()
	// deliveries reports how many deliveries have reached the destination so far.
	deliveries() int
}

// Base implementation for all callback targets.
// Implementations must define the newCallback() method.
type commonTarget struct {
	failing       atomic.Bool
	deliveryCount atomic.Int32
}

func (ct *commonTarget) deliveries() int {
	return int(ct.deliveryCount.Load())
}

func (ct *commonTarget) stopFailing() {
	ct.failing.Store(false)
}

func (ct *commonTarget) CompleteOperation(_ context.Context, _ *nexusrpc.CompletionRequest) error {
	ct.deliveryCount.Add(1)
	if ct.failing.Load() {
		// A retryable error, so the delivery counts against the destination's circuit breaker.
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
	}
	return nil
}

// nexusCallbackTarget provides an implementation of callbackTarget for receiving
// Nexus-variant callbacks.
type nexusCallbackTarget struct {
	commonTarget
	url string
}

func (nct *nexusCallbackTarget) newCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: nct.url,
			},
		},
	}
}

// newNexusCallbackTarget creates a new Nexus-variant callback target. Optionally failing each request until
// [callbackTarget.stopFailing] is called. The server shuts down when t cleans up.
func newNexusCallbackTarget(t *testing.T, _ *NexusTestEnv, failing bool) callbackTarget {
	target := &nexusCallbackTarget{}
	target.failing.Store(failing)

	srv := httptest.NewServer(nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
		Handler: target,
	}))
	t.Cleanup(srv.Close)
	target.url = srv.URL

	return target
}

// callbackExecutionType abstracts a Temporal execution type for testling completion callbacks.
type callbackExecutionType interface {
	// startAndCompleteEx starts a new execution and has it complete successfully.
	// A completion callback from the given target should be attached.
	//
	// Returns an execution ID that can be used for polling.
	startAndCompleteEx(t *testing.T, env *NexusTestEnv, cbTarget callbackTarget) string

	// awaitCallbackState calls Describe- on for the execution type, and will return the
	// attached completion callback once it reaches [wantState]. Will fail if it ever
	// sees the callback in any of [errorStates]. Returns the last observed CallbackInfo.
	awaitCallbackState(
		t *testing.T,
		executionID string,
		env *NexusTestEnv,
		wantState enumspb.CallbackState,
		errorStates []enumspb.CallbackState,
	) *callbackpb.CallbackInfo
}

// standaloneActivityExecutionType implements the callbackExecutionType interface for the
// standalone Activity execution type.
type standaloneActivityExecutionType struct{}

var _ callbackExecutionType = (*standaloneActivityExecutionType)(nil)

func (saa *standaloneActivityExecutionType) startAndCompleteEx(t *testing.T, env *NexusTestEnv, cbTarget callbackTarget) string {
	t.Helper()
	ctx := t.Context()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	// The completion callback to be attached to the execution. The exact callback variant and whether or not
	// it is expected to succeede depends on the calling testcase, and the callbackTarget's implementation.
	compCallback := cbTarget.newCallback()

	_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        env.Tv().ActivityType(),
		Identity:            env.Tv().WorkerIdentity(),
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		RequestId:           env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{
			compCallback,
		},
	})
	require.NoError(t, err)

	// Simulate the worker polling and ack the Activity task.
	pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)

	_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Result:    defaultResult,
		Identity:  env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)

	return activityID
}

func (saa *standaloneActivityExecutionType) awaitCallbackState(
	t *testing.T,
	executionID string,
	env *NexusTestEnv,
	wantState enumspb.CallbackState,
	errorStates []enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	t.Helper()
	ctx := t.Context()

	var (
		errorStateSeen enumspb.CallbackState
		cbInfo         *callbackpb.CallbackInfo
	)
	await.Require(ctx, t, func(c *await.T) {
		ctx := c.Context()
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: executionID,
		})
		require.NoError(c, err)
		require.Len(c, descResp.GetCallbacks(), 1)

		cbInfo = descResp.GetCallbacks()[0].GetInfo()
		got := cbInfo.GetState()
		if slices.Contains(errorStates, got) {
			errorStateSeen = got
			c.Fatalf("Callback has forbidden state %s", got)
		}
		require.Equal(c, wantState, got)
	}, 10*time.Second, 200*time.Millisecond)

	// Confirm we never saw the callback in one of the error states.
	require.Equal(
		t,
		enumspb.CALLBACK_STATE_UNSPECIFIED,
		errorStateSeen,
		"Callback had error state %s", errorStateSeen)

	return cbInfo
}

// standaloneNexusOperationExecutionType implements the callbackExecutionType for the
// standalone Nexus operations.
type standaloneNexusOperationExecutionType struct{}

var _ callbackExecutionType = (*standaloneNexusOperationExecutionType)(nil)

func (saa *standaloneNexusOperationExecutionType) startAndCompleteEx(t *testing.T, env *NexusTestEnv, cbTarget callbackTarget) string {
	t.Helper()
	ctx := t.Context()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	operationID := testcore.RandomizeStr(t.Name())

	// The completion callback to be attached to the execution. The exact callback variant and whether or not
	// it is expected to succeede depends on the calling testcase, and the callbackTarget's implementation.
	compCallback := cbTarget.newCallback()

	alwaysSuccessNexusEndpoint := env.createSyncSuccessEndpoint(ctx, t, "operation-result")
	startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: operationID,
		Endpoint:    alwaysSuccessNexusEndpoint,
		Identity:    env.Tv().WorkerIdentity(),
		RequestId:   env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{
			compCallback,
		},
	})
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	// Starting the operation only schedules the outbound invocation, so wait for the handler to
	// have resolved it. Only then is the completion callback triggered.
	await.Require(ctx, t, func(c *await.T) {
		descResp := env.describeNexusOperation(c.Context(), c, operationID)
		require.Equal(c, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)

	return operationID
}

func (saa *standaloneNexusOperationExecutionType) awaitCallbackState(
	t *testing.T,
	executionID string,
	env *NexusTestEnv,
	wantState enumspb.CallbackState,
	errorStates []enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	t.Helper()
	ctx := t.Context()

	var (
		errorStateSeen enumspb.CallbackState
		cbInfo         *callbackpb.CallbackInfo
	)
	await.Require(ctx, t, func(c *await.T) {
		ctx := c.Context()
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: executionID,
		})
		require.NoError(c, err)
		require.Len(c, descResp.GetCompletionCallbacks(), 1)

		cbInfo = descResp.GetCompletionCallbacks()[0].GetInfo()
		got := cbInfo.GetState()
		if slices.Contains(errorStates, got) {
			errorStateSeen = got
			c.Fatalf("Callback has forbidden state %s", got)
		}
		require.Equal(c, wantState, got)
	}, 10*time.Second, 200*time.Millisecond)

	// Confirm we never saw the callback in one of the error states.
	require.Equal(
		t,
		enumspb.CALLBACK_STATE_UNSPECIFIED,
		errorStateSeen,
		"Callback had error state %s", errorStateSeen)

	return cbInfo
}
