package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/umpire2/internal/kitchensink"
)

const KitchenSinkServiceName = "kitchen-sink"

type ClientActivities struct {
	Client client.Client
}

func (ca *ClientActivities) ExecuteClientActivity(ctx context.Context, clientActivity *kitchensink.ExecuteActivityAction_ClientActivity) error {
	info := activity.GetInfo(ctx)
	executor := &kitchensink.ClientActionsExecutor{
		Client:    ca.Client,
		Namespace: info.Namespace,
		WorkflowOptions: client.StartWorkflowOptions{
			ID:        info.WorkflowExecution.ID,
			TaskQueue: info.TaskQueue,
		},
		WorkflowType:  "kitchenSink",
		WorkflowInput: &kitchensink.WorkflowInput{},
	}
	return executor.ExecuteClientSequence(ctx, clientActivity.ClientSequence)
}

type KSWorkflowState struct {
	workflowState  *kitchensink.WorkflowState
	pendingActions []workflow.Future
}

func KitchenSinkWorkflow(ctx workflow.Context, params *kitchensink.WorkflowInput) (*commonpb.Payload, error) {
	workflow.GetLogger(ctx).Debug("Started kitchen sink workflow")

	state := KSWorkflowState{
		workflowState: &kitchensink.WorkflowState{},
	}

	if params != nil {
		// If ExpectedSignalCount is provided but no explicit IDs, generate them
		if len(params.ExpectedSignalIds) == 0 && params.ExpectedSignalCount > 0 {
			params.ExpectedSignalIds = make([]int32, params.ExpectedSignalCount)
			for i := int32(0); i < params.ExpectedSignalCount; i++ {
				params.ExpectedSignalIds[i] = i + 1
			}
		}
	}

	// Setup query handler.
	queryErr := workflow.SetQueryHandler(ctx, "report_state",
		func(input any) (*kitchensink.WorkflowState, error) {
			return state.workflowState, nil
		})
	if queryErr != nil {
		return nil, queryErr
	}

	// Setup update handler.
	updateErr := workflow.SetUpdateHandlerWithOptions(ctx, "do_actions_update",
		func(ctx workflow.Context, actions *kitchensink.DoActionsUpdate) (rval any, err error) {
			payload, err := state.handleActionSet(ctx, actions.GetDoActions())
			if payload != nil {
				return payload, err
			}
			return state.workflowState, err
		}, workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, actions *kitchensink.DoActionsUpdate) error {
				if actions.GetRejectMe() != nil {
					return errors.New("rejected")
				}
				return nil
			},
		})
	if updateErr != nil {
		return nil, updateErr
	}

	// Setup signal handler.
	signalActionsChan := workflow.GetSignalChannel(ctx, "do_actions_signal")
	retOrErrChan := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var sigActions kitchensink.DoSignal_DoSignalActions
			signalActionsChan.Receive(ctx, &sigActions)
			actionSet := sigActions.GetDoActionsInMain()
			if actionSet == nil {
				actionSet = sigActions.GetDoActions()
			}

			// Handle signal deduplication if signal ID is provided
			if receivedID := sigActions.GetSignalId(); receivedID != 0 {
				if isSignalAlreadyReceived(params, receivedID) {
					workflow.GetLogger(ctx).Debug("Signal already received, skipping", "signalID", receivedID)
					continue
				}
				handleSignalDeduplication(params, receivedID)
			}

			workflow.Go(ctx, func(ctx workflow.Context) {
				ret, err := state.handleActionSet(ctx, actionSet)
				if ret != nil || err != nil {
					retOrErrChan.Send(ctx, ReturnOrErr{ret, err})
				}
			})
		}
	})

	// Handle initial set.
	var initialRetOrErr *ReturnOrErr
	if params != nil && params.InitialActions != nil {
		for _, actionSet := range params.InitialActions {
			if ret, err := state.handleActionSet(ctx, actionSet); ret != nil || err != nil {
				workflow.GetLogger(ctx).Debug("Got return/error from initial actions", "ret", ret, "err", err, "actionSet", actionSet)
				// If there's an error, return immediately without waiting for signals
				if err != nil {
					return ret, err
				}
				// Otherwise, store the return value to use later
				initialRetOrErr = &ReturnOrErr{ret, err}
			}
		}
	}

	var retOrErr ReturnOrErr
	// If we got a return value from initial actions, use it
	if initialRetOrErr != nil {
		retOrErr = *initialRetOrErr
	} else {
		// Otherwise wait for return/error from signals
		for {
			retOrErrChan.Receive(ctx, &retOrErr)
			workflow.GetLogger(ctx).Info("Received return/error from signal", "retOrErr", retOrErr)
			if retOrErr.retme != nil || retOrErr.err != nil {
				break
			}
		}
	}

	// If there's an error, return immediately without waiting for signals
	if retOrErr.err != nil {
		return retOrErr.retme, retOrErr.err
	}

	// Only wait for signals if we're expecting any
	if params != nil && len(params.ExpectedSignalIds) > 0 {
		// Calculate timeout - use WorkflowExecutionTimeout if set, otherwise a reasonable default
		timeout := workflow.GetInfo(ctx).WorkflowExecutionTimeout - (10 * time.Second)
		if timeout <= 0 {
			// If no WorkflowExecutionTimeout or it's too short, use a default
			timeout = 1 * time.Minute
		}

		// Wait for all signals to arrive or an error indicating early termination
		var missingSignals []int32
		ok, err := workflow.AwaitWithTimeout(
			ctx,
			timeout,
			func() bool {
				missingSignals = validateAllSignalsReceived(params)
				return len(missingSignals) == 0
			})
		if err != nil || !ok {
			if !ok {
				err = fmt.Errorf("timeout waiting for all signals before deadline, missing signals: %v, err: %w", missingSignals, err)
			}
			return nil, fmt.Errorf("failed waiting for signals, missing signals: %v, err: %w", missingSignals, err)
		}
	}

	return retOrErr.retme, nil
}

func (ws *KSWorkflowState) handleActionSet(
	ctx workflow.Context,
	set *kitchensink.ActionSet,
) (returnValue *commonpb.Payload, err error) {
	// If these are non-concurrent, just execute and return if requested
	if !set.Concurrent {
		for _, action := range set.Actions {
			if returnValue, err = ws.handleAction(ctx, action); returnValue != nil || err != nil {
				return
			}
		}
		return
	}
	// With a concurrent set, we'll use a coroutine for each, only updating the
	// return values if we should return, then awaiting on that or completion
	var actionsCompleted int
	for _, action := range set.Actions {
		workflow.Go(ctx, func(ctx workflow.Context) {
			if maybeReturnValue, maybeErr := ws.handleAction(ctx, action); maybeReturnValue != nil || maybeErr != nil {
				returnValue, err = maybeReturnValue, maybeErr
			}
			actionsCompleted++
		})
	}
	awaitErr := workflow.Await(ctx, func() bool {
		return (returnValue != nil || err != nil) || actionsCompleted == len(set.Actions)
	})
	if awaitErr != nil {
		return nil, fmt.Errorf("failed waiting on actions: %w", err)
	}
	return
}

// validateAllSignalsReceived checks if all expected signals have been received
// and returns the list of missing signal IDs if any are missing.
func validateAllSignalsReceived(params *kitchensink.WorkflowInput) []int32 {
	if params != nil && len(params.ExpectedSignalIds) > 0 {
		return params.ExpectedSignalIds
	}
	return nil
}

// isSignalAlreadyReceived checks if a signal has already been received (for deduplication).
func isSignalAlreadyReceived(params *kitchensink.WorkflowInput, signalID int32) bool {
	if params == nil {
		return true
	}
	// If the signal ID is not in the expected list, it means we already processed it
	return !slices.Contains(params.ExpectedSignalIds, signalID)
}

// handleSignalDeduplication removes a received signal ID from the expected list.
func handleSignalDeduplication(params *kitchensink.WorkflowInput, signalID int32) {
	if params == nil {
		return
	}
	// Remove the signal ID from the expected slice
	for i, id := range params.ExpectedSignalIds {
		if id == signalID {
			// Remove by swapping with last element and truncating
			params.ExpectedSignalIds[i] = params.ExpectedSignalIds[len(params.ExpectedSignalIds)-1]
			params.ExpectedSignalIds = params.ExpectedSignalIds[:len(params.ExpectedSignalIds)-1]
			return
		}
	}
}

func (ws *KSWorkflowState) handleAction(
	ctx workflow.Context,
	action *kitchensink.Action,
) (*commonpb.Payload, error) {
	switch {
	case action.GetReturnResult() != nil:
		rr := action.GetReturnResult()
		return rr.ReturnThis, nil
	case action.GetReturnError() != nil:
		re := action.GetReturnError()
		return nil, temporal.NewApplicationError(re.Failure.Message, "")
	case action.GetContinueAsNew() != nil:
		can := action.GetContinueAsNew()
		return nil, workflow.NewContinueAsNewError(ctx, "kitchenSink", can.GetArguments()[0])
	case action.GetTimer() != nil:
		timer := action.GetTimer()
		return nil, withAwaitableChoice(ctx, ws, func(ctx workflow.Context) workflow.Future {
			fut, setter := workflow.NewFuture(ctx)
			workflow.Go(ctx, func(ctx workflow.Context) {
				_ = workflow.Sleep(ctx, time.Duration(timer.Milliseconds)*time.Millisecond)
				setter.Set(struct{}{}, nil)
			})
			return fut
		}, timer.AwaitableChoice)
	case action.GetExecActivity() != nil:
		return nil, launchActivity(ctx, ws, action.GetExecActivity())
	case action.GetExecChildWorkflow() != nil:
		return nil, ws.executeChildWorkflow(ctx, action.GetExecChildWorkflow())
	case action.GetSetPatchMarker() != nil:
		patch := action.GetSetPatchMarker()
		if workflow.GetVersion(ctx, patch.GetPatchId(), workflow.DefaultVersion, 1) == 1 {
			return ws.handleAction(ctx, patch.GetInnerAction())
		}
	case action.GetSetWorkflowState() != nil:
		ws.workflowState = action.GetSetWorkflowState()
	case action.GetAwaitWorkflowState() != nil:
		awaitState := action.GetAwaitWorkflowState()
		err := workflow.Await(ctx, func() bool {
			if val, ok := ws.workflowState.Kvs[awaitState.Key]; ok {
				return val == awaitState.Value
			}
			return false
		})
		return nil, err
	case action.GetUpsertMemo() != nil:
		upsertMemo := action.GetUpsertMemo()
		convertedMap := make(map[string]any, len(upsertMemo.GetUpsertedMemo().Fields))
		for k, v := range upsertMemo.GetUpsertedMemo().Fields {
			convertedMap[k] = v
		}
		err := workflow.UpsertMemo(ctx, convertedMap)
		return nil, err
	case action.GetUpsertSearchAttributes() != nil:
		upsertSA := action.GetUpsertSearchAttributes()
		convertedMap := make(map[string]any, len(upsertSA.GetSearchAttributes()))
		for k, v := range upsertSA.GetSearchAttributes() {
			convertedMap[k] = v
		}
		err := workflow.UpsertSearchAttributes(ctx, convertedMap) //nolint:staticcheck // Kitchensink exercises the legacy untyped action schema.
		return nil, err
	case action.GetNestedActionSet() != nil:
		return ws.handleActionSet(ctx, action.GetNestedActionSet())
	case action.GetNexusOperation() != nil:
		return nil, handleNexusOperation(ctx, action.GetNexusOperation(), ws)
	case action.GetAwaitPendingActions() != nil:
		return nil, handleAwaitPendingActions(ctx, ws)
	case action.GetSendSignal() != nil:
		return nil, handleSendSignal(ctx, ws, action.GetSendSignal())
	default:
		return nil, errors.New("unrecognized action")
	}
	return nil, nil
}

func (ws *KSWorkflowState) executeChildWorkflow(ctx workflow.Context, child *kitchensink.ExecuteChildWorkflowAction) error {
	// Use name if present, otherwise use this one
	childType := "kitchenSink"
	if child.WorkflowType != "" {
		childType = child.WorkflowType
	}
	var searchAttributes map[string]any
	if child.SearchAttributes != nil {
		searchAttributes = make(map[string]any)
		for key, value := range child.SearchAttributes {
			searchAttributes[key] = value
		}
	}
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:       child.WorkflowId,
		SearchAttributes: searchAttributes,
	})
	return withAwaitableChoiceCustom(ctx, ws, func(ctx workflow.Context) workflow.ChildWorkflowFuture {
		return workflow.ExecuteChildWorkflow(childCtx, childType, child.GetInput()[0])
	}, child.AwaitableChoice,
		func(ctx workflow.Context, future workflow.ChildWorkflowFuture) error {
			return future.GetChildWorkflowExecution().Get(ctx, nil)
		},
		func(ctx workflow.Context, future workflow.ChildWorkflowFuture) error {
			return future.Get(ctx, nil)
		},
	)
}

func launchActivity(ctx workflow.Context, ws *KSWorkflowState, act *kitchensink.ExecuteActivityAction) error {
	actType, args := kitchensink.ActivityNameAndArgs(act)
	if act.GetIsLocal() != nil {
		opts := workflow.LocalActivityOptions{
			ScheduleToCloseTimeout: act.ScheduleToCloseTimeout.AsDuration(),
			StartToCloseTimeout:    act.StartToCloseTimeout.AsDuration(),
			RetryPolicy:            kitchensink.ConvertFromPBRetryPolicy(act.GetRetryPolicy()),
		}
		actCtx := workflow.WithLocalActivityOptions(ctx, opts)

		return withAwaitableChoice(actCtx, ws, func(ctx workflow.Context) workflow.Future {
			return workflow.ExecuteLocalActivity(ctx, actType, args...)
		}, act.GetAwaitableChoice())
	}
	waitForCancel := false
	if remote := act.GetRemote(); remote != nil {
		if remote.GetCancellationType() == kitchensink.ActivityCancellationType_WAIT_CANCELLATION_COMPLETED {
			waitForCancel = true
		}
	}

	var priority temporal.Priority
	if prio := act.GetPriority(); prio != nil {
		priority.PriorityKey = int(prio.PriorityKey)
	}
	if fk := act.GetFairnessKey(); fk != "" {
		return errors.New("fairness key is not supported yet")
	}
	if fw := act.GetFairnessWeight(); fw > 0 {
		return errors.New("fairness weight is not supported yet")
	}

	opts := workflow.ActivityOptions{
		TaskQueue:              act.TaskQueue,
		ScheduleToCloseTimeout: act.ScheduleToCloseTimeout.AsDuration(),
		StartToCloseTimeout:    act.StartToCloseTimeout.AsDuration(),
		ScheduleToStartTimeout: act.ScheduleToStartTimeout.AsDuration(),
		WaitForCancellation:    waitForCancel,
		HeartbeatTimeout:       act.HeartbeatTimeout.AsDuration(),
		RetryPolicy:            kitchensink.ConvertFromPBRetryPolicy(act.GetRetryPolicy()),
		Priority:               priority,
	}
	actCtx := workflow.WithActivityOptions(ctx, opts)
	return withAwaitableChoice(actCtx, ws, func(ctx workflow.Context) workflow.Future {
		return workflow.ExecuteActivity(ctx, actType, args...)
	}, act.GetAwaitableChoice())
}

func withAwaitableChoice[F workflow.Future](
	ctx workflow.Context,
	ws *KSWorkflowState,
	starter func(workflow.Context) F,
	awaitChoice *kitchensink.AwaitableChoice,
) error {
	return withAwaitableChoiceCustom(ctx, ws, starter, awaitChoice,
		func(ctx workflow.Context, fut F) error {
			_ = workflow.Sleep(ctx, 1)
			return nil
		},
		func(ctx workflow.Context, fut F) error {
			return fut.Get(ctx, nil)
		})
}

func withAwaitableChoiceCustom[F workflow.Future](
	ctx workflow.Context,
	ws *KSWorkflowState,
	starter func(workflow.Context) F,
	awaitChoice *kitchensink.AwaitableChoice,
	afterStartedWaiter func(workflow.Context, F) error,
	afterCompletedWaiter func(workflow.Context, F) error,
) error {
	cancelCtx, cancel := workflow.WithCancel(ctx)
	fut := starter(cancelCtx)
	var err error
	didCancel := false
	switch {
	case awaitChoice.GetAbandon() != nil:
		return nil
	case awaitChoice.GetCancelBeforeStarted() != nil:
		cancel()
		didCancel = true
		err = fut.Get(ctx, nil)
	case awaitChoice.GetCancelAfterStarted() != nil:
		err = afterStartedWaiter(ctx, fut)
		if err != nil {
			return err
		}
		cancel()
		didCancel = true
		err = fut.Get(ctx, nil)
	case awaitChoice.GetCancelAfterCompleted() != nil:
		res := afterCompletedWaiter(ctx, fut)
		cancel()
		err = res
	case awaitChoice.GetWaitStarted() != nil:
		err = afterStartedWaiter(ctx, fut)
		if err == nil && ws != nil {
			ws.pendingActions = append(ws.pendingActions, fut)
		}
	default:
		err = fut.Get(ctx, nil)
	}

	// If we intentionally cancelled we want to swallow the cancel error to avoid bombing out the
	// whole workflow
	var canceledErr *temporal.CanceledError
	if didCancel && errors.As(err, &canceledErr) {
		err = nil
	}
	return err
}

func handleNexusOperation(ctx workflow.Context, nexusOp *kitchensink.ExecuteNexusOperation, state *KSWorkflowState) error {
	return withAwaitableChoiceCustom(ctx, state, func(ctx workflow.Context) workflow.NexusOperationFuture {
		client := workflow.NewNexusClient(nexusOp.Endpoint, KitchenSinkServiceName)
		nexusOptions := workflow.NexusOperationOptions{}
		input := &kitchensink.NexusHandlerInput{
			Input:                           nexusOp.Input,
			BeforeActions:                   nexusOp.BeforeActions,
			HandlerWorkflowId:               nexusOp.HandlerWorkflowId,
			HandlerWorkflowIdConflictPolicy: nexusOp.HandlerWorkflowIdConflictPolicy,
			WaitForSignal:                   nexusOp.WaitForSignal,
		}
		return client.ExecuteOperation(ctx, nexusOp.Operation, input, nexusOptions)
	}, nexusOp.AwaitableChoice,
		func(ctx workflow.Context, fut workflow.NexusOperationFuture) error {
			return fut.GetNexusOperationExecution().Get(ctx, nil)
		},
		func(ctx workflow.Context, fut workflow.NexusOperationFuture) error {
			if expOutput := nexusOp.GetExpectedOutput(); expOutput != "" {
				var result string
				if err := fut.Get(ctx, &result); err != nil {
					return err
				}
				if expOutput != result {
					return fmt.Errorf("expected output %q, got %q", expOutput, result)
				}
				return nil
			}
			return fut.Get(ctx, nil)
		})
}

func handleSendSignal(ctx workflow.Context, ws *KSWorkflowState, action *kitchensink.SendSignalAction) error {
	return withAwaitableChoiceCustom(ctx, ws, func(ctx workflow.Context) workflow.Future {
		return workflow.SignalExternalWorkflow(ctx, action.WorkflowId, action.RunId, action.SignalName, nil)
	}, action.AwaitableChoice,
		func(ctx workflow.Context, fut workflow.Future) error {
			return fut.Get(ctx, nil)
		},
		func(ctx workflow.Context, fut workflow.Future) error {
			return fut.Get(ctx, nil)
		})
}

func handleAwaitPendingActions(ctx workflow.Context, ws *KSWorkflowState) error {
	pending := ws.pendingActions
	ws.pendingActions = nil
	for i, fut := range pending {
		if err := fut.Get(ctx, nil); err != nil {
			return fmt.Errorf("pending action %d failed: %w", i, err)
		}
	}
	return nil
}

// Noop is used as a no-op activity.
func Noop(_ context.Context) error {
	return nil
}

func Payload(_ context.Context, inputData []byte, bytesToReturn int32) ([]byte, error) {
	output := make([]byte, bytesToReturn)
	// Random bytes keep the payload incompressible; cryptographic security is not required.
	if _, err := rand.Read(output); err != nil {
		return nil, err
	}
	return output, nil
}

// Delay runs for the provided delay period.
func Delay(ctx context.Context, delayFor time.Duration) error {
	timer := time.NewTimer(delayFor)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RetryableError throws retryable errors for N attempts, then succeeds
func RetryableError(ctx context.Context, config *kitchensink.ExecuteActivityAction_RetryableErrorActivity) error {
	info := activity.GetInfo(ctx)
	if info.Attempt <= config.FailAttempts {
		return temporal.NewApplicationError("retryable error", "RetryableError", nil)
	}
	return nil
}

// Timeout runs too long for N attempts (causing timeout), then completes quickly
func Timeout(ctx context.Context, config *kitchensink.ExecuteActivityAction_TimeoutActivity) error {
	info := activity.GetInfo(ctx)

	if info.Attempt <= config.FailAttempts {
		// Run until context is canceled (via StartToCloseTimeout)
		<-ctx.Done()
		return ctx.Err()
	}

	select {
	// Success case: run for configured duration
	case <-time.After(config.SuccessDuration.AsDuration()):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Heartbeat skips heartbeats for N attempts (causing heartbeat timeout), then sends them
func Heartbeat(ctx context.Context, config *kitchensink.ExecuteActivityAction_HeartbeatTimeoutActivity) error {
	info := activity.GetInfo(ctx)
	shouldSendHeartbeats := info.Attempt > config.FailAttempts
	// If we should not send heartbeats, run the activity until it is cancelled via heartbeat timeout.
	if !shouldSendHeartbeats {
		<-ctx.Done()
		return ctx.Err()
	}
	// Otherwise, run it for the configured success duration and heartbeat.
	<-time.After(config.SuccessDuration.AsDuration())
	activity.RecordHeartbeat(ctx)
	return nil
}

type ReturnOrErr struct {
	retme *commonpb.Payload
	err   error
}

func NexusHandlerWorkflow(ctx workflow.Context, input *kitchensink.NexusHandlerInput) (string, error) {
	state := KSWorkflowState{
		workflowState: &kitchensink.WorkflowState{},
	}
	for _, actionSet := range input.BeforeActions {
		if _, err := state.handleActionSet(ctx, actionSet); err != nil {
			return "", err
		}
	}
	if input.WaitForSignal {
		workflow.GetSignalChannel(ctx, "unblock").Receive(ctx, nil)
	}
	return input.Input, nil
}

// EchoSyncOperation returns the input synchronously without starting a workflow.
var EchoSyncOperation = nexus.NewSyncOperation("echo-sync", func(ctx context.Context, input *kitchensink.NexusHandlerInput, opts nexus.StartOperationOptions) (string, error) {
	if len(input.BeforeActions) > 0 {
		return "", nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "before_actions not supported in echo-sync")
	}
	return input.Input, nil
})

// EchoAsyncOperation starts a NexusHandlerWorkflow that runs before_actions and returns the input.
var EchoAsyncOperation = temporalnexus.NewWorkflowRunOperation("echo-async", NexusHandlerWorkflow, func(ctx context.Context, input *kitchensink.NexusHandlerInput, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
	if input.HandlerWorkflowId != "" {
		return client.StartWorkflowOptions{
			ID:                       input.HandlerWorkflowId,
			WorkflowIDConflictPolicy: input.HandlerWorkflowIdConflictPolicy,
			WorkflowExecutionTimeout: 60 * time.Minute,
		}, nil
	}
	return client.StartWorkflowOptions{
		ID: opts.RequestID,
	}, nil
})
