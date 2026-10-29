package tests

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// completionCallbackTarget represents the thing receiving the callback delivery. Failures reaching this
// target will open the circuit breaker. (e.g. an unavailable Nexus handler.)
type completionCallbackTarget interface {
	// newCallback returns a new completion callback addressed to this destination.
	newCallback() *commonpb.Callback
	// changeBehavior updates the way the callback target operates moving forward.
	changeBehavior(completionCallbackBehavior)
	// deliveries reports how many deliveries have reached the destination so far.
	deliveries() int
}

// completionCallbackBehavior determines how a baseCompletionCallbackTarget should answer a
// delivery reaching it.
type completionCallbackBehavior int32

const (
	completionCallbackBehaviorSuccess             completionCallbackBehavior = 1
	completionCallbackBehaviorNonRetryableFailure completionCallbackBehavior = 2
	completionCallbackBehaviorRetryableFailure    completionCallbackBehavior = 3
)

// Base implementation for all callback targets.
// Implementations must define the newCallback() method.
type baseCompletionCallbackTarget struct {
	behavior      atomic.Int32 // completionCallbackBehavior
	deliveryCount atomic.Int32
}

func (ct *baseCompletionCallbackTarget) deliveries() int {
	return int(ct.deliveryCount.Load())
}

func (ct *baseCompletionCallbackTarget) changeBehavior(newBehavior completionCallbackBehavior) {
	ct.behavior.Store(int32(newBehavior))
}

// deliver records a delivery having reached the target and returns how the target answers it.
// A return value of nil means accepts the delivery, otherwise fail.
func (ct *baseCompletionCallbackTarget) deliver() *nexus.HandlerError {
	received := ct.deliveryCount.Add(1)
	switch ct.behavior.Load() {
	case int32(completionCallbackBehaviorSuccess):
		return nil
	case int32(completionCallbackBehaviorNonRetryableFailure):
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "terminal failure")
	case int32(completionCallbackBehaviorRetryableFailure):
		fallthrough
	default:
		// A retryable error, so the delivery counts against the destination's circuit breaker.
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "non-terminal failure (%d)", received)
	}
}

// nexusCompletionCallbackTarget provides an implementation of callbackTarget for receiving Nexus-variant callbacks.
type nexusCompletionCallbackTarget struct {
	baseCompletionCallbackTarget
	url string
}

func (nct *nexusCompletionCallbackTarget) newCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: nct.url,
			},
		},
	}
}

// CompleteOperation implements the nexusrpc handler interface.
func (nct *nexusCompletionCallbackTarget) CompleteOperation(_ context.Context, _ *nexusrpc.CompletionRequest) error {
	handlerErr := nct.deliver()
	if handlerErr == nil {
		// NOTE: deliver returns a typed *nexus.HandlerError, so it can't be returned unconditionally:
		// a nil pointer becomes a non-nil error interface, and the handler would treat every
		// successful delivery as a failure.
		return nil
	}
	return handlerErr
}

// newNexusCompletionCallbackTarget creates a new Nexus-variant callback target.
// Starts a new HTTP server, will be cleaned up with the testcase.
func newNexusCompletionCallbackTarget(t *testing.T, _ *testcore.TestEnv, behavior completionCallbackBehavior) completionCallbackTarget {
	target := &nexusCompletionCallbackTarget{}
	target.behavior.Store(int32(behavior))

	srv := httptest.NewServer(nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
		Handler: target,
	}))
	t.Cleanup(srv.Close)
	target.url = srv.URL

	return target
}

// nexusHandlerCompletionCallbackTarget provides an implementation of callbackTarget for receiving NexusHandler-variant callbacks.
type nexusHandlerCompletionCallbackTarget struct {
	baseCompletionCallbackTarget
	// taskQueue the target's worker polls on. Distinct per nexusHandlerCompletionCallbackTarget so that
	// an open circuitbreaker doesn't block all potential callback targets.
	taskQueue string
}

func (nhct *nexusHandlerCompletionCallbackTarget) newCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_NexusHandler_{
			NexusHandler: &commonpb.Callback_NexusHandler{
				TaskQueueName: nhct.taskQueue,
				Service:       "NexusHandlerService",
				Operation:     "OnComplete",
				SourceContext: &commonpb.Payload{
					Data: []byte("source context payload"),
				},
			},
		},
	}
}

// pollAndRespond is the worker side of a NexusHandler callback. It polls the target's task queue and
// answers every task it is handed according to the target's behavior at that moment.
//
// Runs until ctx is canceled.
func (nhct *nexusHandlerCompletionCallbackTarget) pollAndRespond(ctx context.Context, t *testing.T, env *NexusTestEnv) {
	for ctx.Err() == nil {
		// Issue a long-poll request.
		pollCtx, cancelPoll := rpc.NewContextFromParentWithTimeoutAndVersionHeaders(ctx, 30*time.Second)

		task, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			Identity:  env.Tv().WorkerIdentity(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: nhct.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		cancelPoll()

		if err != nil {
			// Cancelation is how the loop is stopped, so it isn't worth reporting.
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// The long poll ran out of time without a task, which is business as usual.
				continue
			}
			t.Logf("failed to poll Nexus task queue %q: %v", nhct.taskQueue, err)

			// If the context is still valid, pause a beat and retry.
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}

		// An empty task token means the long poll timed out without a task being dispatched.
		if len(task.GetTaskToken()) == 0 {
			continue
		}

		// Otherwise, respond to the Nexus invocation task.
		if err := nhct.respond(ctx, env, task); err != nil {
			t.Logf("failed to respond to Nexus task on %q: %v", nhct.taskQueue, err)
		}
	}
}

// respond answers a single delivered Nexus task the way the target's behavior dictates.
func (nhct *nexusHandlerCompletionCallbackTarget) respond(
	ctx context.Context,
	env *NexusTestEnv,
	task *workflowservice.PollNexusTaskQueueResponse,
) error {
	// The poller's own context has no deadline, it lives as long as the test does.
	ctx, cancel := rpc.NewContextFromParentWithTimeoutAndVersionHeaders(ctx, 10*time.Second)
	defer cancel()

	startOp := task.GetRequest().GetStartOperation()
	if startOp == nil {
		return fmt.Errorf("got unexpected NexusTask: %v", task.GetRequest())
	}

	// Determine the right response based on the current behavior, and respond.
	if deliveryErr := nhct.deliver(); deliveryErr != nil {
		return env.respondNexusTaskFailed(ctx, task.GetTaskToken(), deliveryErr)
	}

	// The handler accepted the completion. Report back as if it returned a sync response.
	result, err := payload.Encode("NexusHandler callback delivered")
	if err != nil {
		return err
	}
	_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  env.Tv().WorkerIdentity(),
		TaskToken: task.GetTaskToken(),
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: result},
					},
				},
			},
		},
	})
	return err
}

// newNexusHandlerCompletionCallbackTarget creates a new NexusHandler-variant callback target.
func newNexusHandlerCompletionCallbackTarget(t *testing.T, env *testcore.TestEnv, behavior completionCallbackBehavior) completionCallbackTarget {
	target := &nexusHandlerCompletionCallbackTarget{
		taskQueue: testcore.RandomizeStr("nh-callback-tq"),
	}
	target.behavior.Store(int32(behavior))

	// A NexusHandler callback is delivered as a Nexus task on the callback's task queue, in the
	// source execution's own namespace. So the "worker" here is a bare poll loop against that task
	// queue. It answers as the registered Nexus service, no endpoint required.

	// The poller needs a context of its own: t.Context is canceled before cleanups run, and the
	// poll loop has to outlive it long enough to be shut down in an orderly way.
	pollerCtx, stopPolling := context.WithCancel(context.Background())
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		nexusEnv := &NexusTestEnv{
			TestEnv:             env,
			useTemporalFailures: true,
		}
		target.pollAndRespond(pollerCtx, t, nexusEnv)
	}()
	t.Cleanup(func() {
		stopPolling()
		// The poll loop may report failures to t, so the poller must stop before the test does.
		<-pollerDone
	})

	return target
}

// executionWithCallbacks abstracts a Temporal execution type for testling completion callbacks.
// e.g. Workflows, standalone Activities, etc.
type executionWithCallbacks interface {
	// startAndCompleteEx starts a new execution and has it complete successfully, attaching the
	// supplied completion callback. Returns an execution ID that can be used for polling or any errors.
	startAndCompleteEx(t *testing.T, env *testcore.TestEnv, callback *commonpb.Callback) (string, error)

	// awaitCallbackState calls Describe- on for the execution type, and will return the
	// attached completion callback once it reaches [wantState]. Will fail if it ever
	// sees the callback in any of [errorStates]. Returns the last observed CallbackInfo.
	awaitCallbackState(
		t *testing.T,
		executionID string,
		env *testcore.TestEnv,
		wantState enumspb.CallbackState,
		errorStates []enumspb.CallbackState,
	) *callbackpb.CallbackInfo
}

// baseExecutionWithCallbacks provides basic methods to eliminate the boilerplate for
// implementations of the executionWithCallbacks interface.
type baseExecutionWithCallbacks struct{}

// baseAwaitCallbackState calls the Describe- function to get the execution's completion callback,
// and polls until it reaches wantState. It fails on any of errorStates, and returns the last
// observed CallbackInfo.
//
// Will abort the test on any errors returned from describeFn.
func (b *baseExecutionWithCallbacks) baseAwaitCallbackState(
	t *testing.T,
	describeFn func(context.Context) (*callbackpb.CallbackInfo, error),
	wantState enumspb.CallbackState,
	errorStates []enumspb.CallbackState) *callbackpb.CallbackInfo {

	t.Helper()
	ctx := t.Context()

	var (
		errorStateSeen enumspb.CallbackState
		cbInfo         *callbackpb.CallbackInfo
	)
	await.Require(ctx, t, func(c *await.T) {
		ctx := c.Context()
		// NOTE: assign to the captured cbInfo, don't shadow it with :=.
		var err error
		cbInfo, err = describeFn(ctx)
		require.NoError(c, err)

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

// workflowExecutionType implements the executionWithCallbacks using Workflows.
type workflowExecutionType struct {
	baseExecutionWithCallbacks
}

// startAndCompleteEx implements [executionWithCallbacks].
func (wf *workflowExecutionType) startAndCompleteEx(t *testing.T, env *testcore.TestEnv, callback *commonpb.Callback) (string, error) {
	t.Helper()
	ctx := t.Context()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	workflowID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          env.Namespace().String(),
		WorkflowId:         workflowID,
		WorkflowType:       env.Tv().WorkflowType(),
		Identity:           env.Tv().WorkerIdentity(),
		Input:              payloads.EncodeString("input payload"),
		TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout: durationpb.New(10 * time.Second),
		RequestId:          env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{
			callback,
		},
	})
	if err != nil {
		return "", err
	}

	// Simulate the worker polling for the Workflow task, and immediately completing the Workflow.
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  env.Tv().WorkerIdentity(),
	})
	if err != nil {
		return "", err
	}
	// An empty task token means the long poll timed out without a task being dispatched.
	if len(pollResp.GetTaskToken()) == 0 {
		return "", fmt.Errorf("no workflow task dispatched on task queue %q", taskQueue)
	}

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Identity:  env.Tv().WorkerIdentity(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("output payload"),
					},
				},
			},
		},
	})
	if err != nil {
		return "", err
	}

	return workflowID, nil
}

func (wf *workflowExecutionType) awaitCallbackState(t *testing.T, executionID string, env *testcore.TestEnv, wantState enumspb.CallbackState, errorStates []enumspb.CallbackState) *callbackpb.CallbackInfo {
	t.Helper()

	describeFn := func(ctx context.Context) (*callbackpb.CallbackInfo, error) {
		descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionID,
			},
		})
		if err != nil {
			return nil, err
		}
		callbacks := descResp.GetCallbacks()
		if len(callbacks) != 1 {
			return nil, fmt.Errorf("expected 1 callback, got %d", len(callbacks))
		}

		// NOTE: Workflows do not return a callbackpb.CallbackInfo proto. Instead, it forks (rather than embeds) the type.
		// So we convert the nearly identical proto to avoid wrapping it in an interface.
		wfCallbackInfo := callbacks[0]
		apiCallbackInfo := &callbackpb.CallbackInfo{
			Callback:                wfCallbackInfo.GetCallback(),
			RegistrationTime:        wfCallbackInfo.GetRegistrationTime(),
			State:                   wfCallbackInfo.GetState(),
			Attempt:                 wfCallbackInfo.GetAttempt(),
			LastAttemptCompleteTime: wfCallbackInfo.GetLastAttemptCompleteTime(),
			LastAttemptFailure:      wfCallbackInfo.GetLastAttemptFailure(),
			NextAttemptScheduleTime: wfCallbackInfo.GetNextAttemptScheduleTime(),
			BlockedReason:           wfCallbackInfo.GetBlockedReason(),
			RequestId:               wfCallbackInfo.GetRequestId(),
		}
		return apiCallbackInfo, nil
	}
	return wf.baseAwaitCallbackState(t, describeFn, wantState, errorStates)
}

var _ executionWithCallbacks = (*workflowExecutionType)(nil)

// standaloneActivityExecutionType implements the executionWithCallbacks using standalone Activities.
type standaloneActivityExecutionType struct {
	baseExecutionWithCallbacks
}

var _ executionWithCallbacks = (*standaloneActivityExecutionType)(nil)

func (saa *standaloneActivityExecutionType) startAndCompleteEx(
	t *testing.T,
	env *testcore.TestEnv,
	callback *commonpb.Callback,
) (string, error) {
	t.Helper()
	ctx := t.Context()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        env.Tv().ActivityType(),
		Identity:            env.Tv().WorkerIdentity(),
		Input:               payloads.EncodeString("input payload"),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(10 * time.Second),
		RequestId:           env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{
			callback,
		},
	})
	if err != nil {
		return "", err
	}

	// Simulate the worker polling and ack the Activity task.
	pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  env.Tv().WorkerIdentity(),
	})
	if err != nil {
		return "", err
	}
	// An empty task token means the long poll timed out without a task being dispatched.
	if len(pollResp.GetTaskToken()) == 0 {
		return "", fmt.Errorf("no Activity task dispatched on task queue %q", taskQueue)
	}

	_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Result:    payloads.EncodeString("output payload"),
		Identity:  env.Tv().WorkerIdentity(),
	})
	if err != nil {
		return "", err
	}

	return activityID, nil
}

func (saa *standaloneActivityExecutionType) awaitCallbackState(
	t *testing.T,
	executionID string,
	env *testcore.TestEnv,
	wantState enumspb.CallbackState,
	errorStates []enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	t.Helper()

	describeFn := func(ctx context.Context) (*callbackpb.CallbackInfo, error) {
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: executionID,
		})
		if err != nil {
			return nil, err
		}
		callbacks := descResp.GetCallbacks()
		if len(callbacks) != 1 {
			return nil, fmt.Errorf("expected 1 callback, got %d", len(callbacks))
		}
		return callbacks[0].GetInfo(), nil
	}
	return saa.baseAwaitCallbackState(t, describeFn, wantState, errorStates)
}

// standaloneNexusOperationExecutionType implements the executionWithCallbacks using standalone Nexus operations.
type standaloneNexusOperationExecutionType struct {
	baseExecutionWithCallbacks
}

var _ executionWithCallbacks = (*standaloneNexusOperationExecutionType)(nil)

func (sno *standaloneNexusOperationExecutionType) startAndCompleteEx(
	t *testing.T,
	env *testcore.TestEnv,
	callback *commonpb.Callback,
) (string, error) {
	t.Helper()
	ctx := t.Context()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	operationID := testcore.RandomizeStr(t.Name())
	nexusTestEnv := NexusTestEnv{
		TestEnv:             env,
		useTemporalFailures: true,
	}
	alwaysSuccessNexusEndpoint := nexusTestEnv.createSyncSuccessEndpoint(ctx, t, "operation-result")

	// Start the standalone Nexus operation.
	startReq := &workflowservice.StartNexusOperationExecutionRequest{
		Namespace: env.Namespace().String(),

		// The always success endpoint ignores the Service/Operation fields.
		Endpoint:  alwaysSuccessNexusEndpoint,
		Service:   "nexus-service",
		Operation: "nexus-operation",

		OperationId:            operationID,
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		CompletionCallbacks: []*commonpb.Callback{
			callback,
		},
	}
	startResp, err := env.FrontendClient().StartNexusOperationExecution(ctx, startReq)
	if err != nil {
		return "", err
	}

	require.True(t, startResp.GetStarted())

	// Wait for the Nexus operation to be resolved, only then are its completion callbacks triggered.
	await.Require(ctx, t, func(c *await.T) {
		descReq := &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:      env.Namespace().String(),
			OperationId:    operationID,
			IncludeOutcome: true,
		}
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(c.Context(), descReq)
		require.NoError(c, err)

		gotStatus := descResp.GetInfo().GetStatus()
		require.Equal(c, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, gotStatus)
	}, 10*time.Second, 200*time.Millisecond)

	return operationID, nil
}

func (sno *standaloneNexusOperationExecutionType) awaitCallbackState(
	t *testing.T,
	executionID string,
	env *testcore.TestEnv,
	wantState enumspb.CallbackState,
	errorStates []enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	t.Helper()

	describeFn := func(ctx context.Context) (*callbackpb.CallbackInfo, error) {
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: executionID,
		})
		if err != nil {
			return nil, err
		}
		callbacks := descResp.GetCompletionCallbacks()
		if len(callbacks) != 1 {
			return nil, fmt.Errorf("expected 1 callback, got %d", len(callbacks))
		}
		return callbacks[0].GetInfo(), nil
	}
	return sno.baseAwaitCallbackState(t, describeFn, wantState, errorStates)
}
