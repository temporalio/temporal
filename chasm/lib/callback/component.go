package callback

import (
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CompletionSource interface {
	GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)
}

// CompletionSourceFn allows a function value to be used as a CompletionSource instance.
type CompletionSourceFn func(chasm.Context, string) (nexusrpc.CompleteOperationOptions, error)

func (csFunc CompletionSourceFn) GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return csFunc(ctx, requestID)
}

var (
	_ chasm.Component                                = (*Callback)(nil)
	_ chasm.StateMachine[callbackspb.CallbackStatus] = (*Callback)(nil)

	// Capabilities only supported/used for standalone callbacks.
	_ chasm.RootComponent                      = (*Callback)(nil)
	_ chasm.VisibilityMemoProvider             = (*Callback)(nil)
	_ chasm.VisibilitySearchAttributesProvider = (*Callback)(nil)
)

var executionStatusSearchAttribute = chasm.NewSearchAttributeKeyword(
	"ExecutionStatus",
	chasm.SearchAttributeFieldLowCardinalityKeyword01,
)

// Callback represents a callback component in CHASM.
type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState

	// Failure from an external termination (timeout or terminate), stored separately because
	// of its potential size, and to not overload CallbackState::LastAttemptFailure.
	TerminalFailure chasm.Field[*failurepb.Failure]

	// For most callbacks, the completion result is obtained from the parent component.
	// e.g. the Workflow result to be delivered. However, for standalone callbacks, there
	// is no parent and the user-supplied SuppliedCompletion will be used instead.
	ParentCompletionSource chasm.ParentPtr[CompletionSource]
	SuppliedCompletion     chasm.Field[*callbackpb.CallbackExecutionCompletion]

	// Visibility sub-component for search attributes and memo indexing.
	Visibility chasm.Field[*chasm.Visibility]
}

// NewEmbeddedCallback returns a Callback component, which will deliver the completion from
// its parent CHASM component. The parent must implement CompletionSource.
func NewEmbeddedCallback(
	ctx chasm.MutableContext,
	requestID string,
	registrationTime *timestamppb.Timestamp,
	cb *callbackspb.Callback,
) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        requestID,
			RegistrationTime: registrationTime,
			Callback:         cb,
			Status:           callbackspb.CALLBACK_STATUS_STANDBY,
		},
		TerminalFailure: chasm.NewDataField[*failurepb.Failure](ctx, nil),
	}
}

type newStandaloneCallbackOpts struct {
	RequestID        string
	RegistrationTime *timestamppb.Timestamp
	Callback         *callbackspb.Callback

	CallbackID             string
	ScheduleToCloseTimeout *durationpb.Duration
	Completion             *callbackpb.CallbackExecutionCompletion
	SearchAttributes       map[string]*commonpb.Payload
}

// newStandaloneCallback returns a new Callback component which will deliver the supplied
// completion result.
func newStandaloneCallback(
	ctx chasm.MutableContext,
	opts newStandaloneCallbackOpts,
) *Callback {
	cb := NewEmbeddedCallback(ctx, opts.RequestID, opts.RegistrationTime, opts.Callback)

	// Add standalone-specific fields.
	cb.ScheduleToCloseTimeout = opts.ScheduleToCloseTimeout
	cb.SuppliedCompletion = chasm.NewDataField(ctx, opts.Completion)

	visibility := chasm.NewVisibilityWithData(ctx, opts.SearchAttributes, nil)
	cb.Visibility = chasm.NewComponentField(ctx, visibility)

	return cb
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED:
		// TODO: Use chasm.LifecycleStateTerminated when it's available (currently commented out
		// in chasm/component.go:70). For now, LifecycleStateFailed is functionally correct
		// as IsClosed() returns true for all states >= LifecycleStateCompleted.
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (c *Callback) StateMachineState() callbackspb.CallbackStatus {
	return c.Status
}

func (c *Callback) SetStateMachineState(status callbackspb.CallbackStatus) {
	c.Status = status
}

func (c *Callback) ContextMetadata(ctx chasm.Context) map[string]string {
	return map[string]string{
		"RequestID": c.RequestId,
		// Only set for standalone callbacks.
		"CallbackID": ctx.ExecutionKey().BusinessID,
	}
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider.
func (c *Callback) SearchAttributes(ctx chasm.Context) []chasm.SearchAttributeKeyValue {
	apiStatus := callbackStatusToAPIExecutionStatus(c.Status)
	return []chasm.SearchAttributeKeyValue{
		executionStatusSearchAttribute.Value(apiStatus.String()),
	}
}

// Memo implements chasm.VisibilityMemoProvider. Returns the CallbackExecutionListInfo
// as the memo for visibility queries.
func (c *Callback) Memo(ctx chasm.Context) proto.Message {
	return &callbackpb.CallbackExecutionListInfo{
		CallbackId: ctx.ExecutionKey().BusinessID,
		Status:     callbackStatusToAPIExecutionStatus(c.Status),
		CreateTime: c.RegistrationTime,
		CloseTime:  c.CloseTime,
	}
}

// Terminate forcefully terminates the callback execution.
//
// If already terminated with the same request ID, this is a no-op.
// If already terminated with a different request ID, returns FailedPrecondition.
func (c *Callback) Terminate(
	ctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if c.LifecycleState(ctx).IsClosed() {
		if c.TerminateRequestId == "" {
			// Completed organically (succeeded/failed/timed out), not via Terminate.
			err := serviceerror.NewFailedPreconditionf("callback execution already in terminal state %v", c.Status)
			return chasm.TerminateComponentResponse{}, err
		}
		if c.TerminateRequestId != req.RequestID {
			err := serviceerror.NewFailedPreconditionf("already terminated with request ID %s", c.TerminateRequestId)
			return chasm.TerminateComponentResponse{}, err
		}
		return chasm.TerminateComponentResponse{}, nil
	}
	if err := TransitionTerminated.Apply(c, ctx, EventTerminated{Reason: req.Reason}); err != nil {
		return chasm.TerminateComponentResponse{}, fmt.Errorf("failed to terminate callback: %w", err)
	}

	c.TerminateRequestId = req.RequestID
	// c.TerminalFailure is set in the transition handler.
	return chasm.TerminateComponentResponse{}, nil
}

func (c *Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}

//nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function parameter
func (c *Callback) loadInvocationArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (invocable, error) {
	// Get the completion result to be delivered.
	completionSource := c.completionSource(ctx)
	completion, err := completionSource.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
	}

	callback := c.GetCallback().GetNexus()
	if callback == nil {
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %v", callback),
		)
	}

	// Setup the completion's headers.
	completion.Header = callback.Header
	if callback.GetToken() != "" {
		if completion.Header == nil {
			completion.Header = nexus.Header{}
		}
		completion.Header.Set(commonnexus.CallbackTokenHeader, callback.GetToken())
	}

	if callback.Url == chasm.NexusCompletionHandlerURL {
		return invocableInternal{
			callback:   callback,
			attempt:    c.Attempt,
			completion: completion,
			requestID:  c.RequestId,
		}, nil
	}
	return invocableOutbound{
		callback:   callback,
		completion: completion,
		workflowID: ctx.ExecutionKey().BusinessID,
		runID:      ctx.ExecutionKey().RunID,
		attempt:    c.Attempt,
	}, nil
}

type saveResultInput struct {
	result      invocationResult
	retryPolicy backoff.RetryPolicy
}

func (c *Callback) saveResult(
	ctx chasm.MutableContext,
	input saveResultInput,
) (chasm.NoValue, error) {
	// If the callback was terminated while the invocation was in-flight,
	// the result is no longer relevant. We'll just drop it silently.
	//
	// This shouldn't happen outside of tests, since the Nexus machinery
	// would prevent an invalid transition anyways. (e.g. terminating
	// an already terminated Callback.)
	if c.LifecycleState(ctx).IsClosed() {
		return nil, nil
	}

	switch r := input.result.(type) {
	case invocationResultOK:
		err := TransitionSucceeded.Apply(c, ctx, EventSucceeded{Time: ctx.Now(c)})
		return nil, err
	case invocationResultRetry:
		err := TransitionAttemptFailed.Apply(c, ctx, EventAttemptFailed{
			Time:        ctx.Now(c),
			Err:         r.err,
			RetryPolicy: input.retryPolicy,
		})
		return nil, err
	case invocationResultFail:
		err := TransitionFailed.Apply(c, ctx, EventFailed{
			Time: ctx.Now(c),
			Err:  r.err,
		})
		return nil, err
	default:
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unrecognized callback result %v", input.result),
		)
	}
}

// ToAPICallback converts a CHASM callback to API callback proto.
func (c *Callback) ToAPICallback() (*commonpb.Callback, error) {
	// Convert CHASM callback proto to API callback proto
	chasmCB := c.GetCallback()
	res := &commonpb.Callback{
		Links: chasmCB.GetLinks(),
	}

	// CHASM currently only supports Nexus callbacks
	if variant, ok := chasmCB.Variant.(*callbackspb.Callback_Nexus_); ok {
		res.Variant = &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: variant.Nexus.GetHeader(),
			},
		}
		return res, nil
	}

	// This should not happen as CHASM only supports Nexus callbacks currently
	return nil, serviceerror.NewInternal("unsupported CHASM callback type")
}

// ScheduleStandbyCallbacks transitions all STANDBY callbacks to SCHEDULED state,
// triggering their invocation. Used by both workflows and standalone activities
// when the execution reaches a terminal state.
func ScheduleStandbyCallbacks(ctx chasm.MutableContext, callbacks chasm.Map[string, *Callback]) error {
	for _, field := range callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
			continue
		}
		if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
			return err
		}
	}
	return nil
}

func callbackCompletionToNexusCompleteOperationOpts(
	cb *Callback,
	completion *callbackpb.CallbackExecutionCompletion) (nexusrpc.CompleteOperationOptions, error) {

	nexusCompletion := nexusrpc.CompleteOperationOptions{
		StartTime: cb.GetRegistrationTime().AsTime(),
		CloseTime: cb.CloseTime.AsTime(),
	}

	switch completion.Result.(type) {
	case *callbackpb.CallbackExecutionCompletion_Success:
		nexusCompletion.Result = completion.GetSuccess()
		return nexusCompletion, nil

	case *callbackpb.CallbackExecutionCompletion_Failure:
		f, err := commonnexus.TemporalFailureToNexusFailure(completion.GetFailure())
		if err != nil {
			wrappedErr := fmt.Errorf("failed to convert failure: %w", err)
			return nexusrpc.CompleteOperationOptions{}, wrappedErr
		}
		opErr := &nexus.OperationError{
			State:   nexus.OperationStateFailed,
			Message: "operation failed",
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			wrappedErr := fmt.Errorf("failed to mark wrapper error: %w", err)
			return nexusrpc.CompleteOperationOptions{}, wrappedErr
		}
		nexusCompletion.Error = opErr
		return nexusCompletion, nil

	default:
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInvalidArgument("no completion result provided")
	}
}

// completionSource returns the completionSource from the callback, which depends on whether it
// is embedded or is running in standalone mode.
func (c *Callback) completionSource(ctx chasm.Context) CompletionSource {
	// Embedded callbacks use their parent component as a CompletionSource.
	source, ok := c.ParentCompletionSource.TryGet(ctx)
	if ok {
		return source
	}

	// For standalone completions, get the user-supplied value and convert it
	// into the Nexus API type.
	suppliedCompletion, ok := c.SuppliedCompletion.TryGet(ctx)
	if !ok {
		return CompletionSourceFn(func(_ chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
			return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternal("no completion available")
		})
	}

	convertOutcomeProtoFn := func(_ chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
		return callbackCompletionToNexusCompleteOperationOpts(c, suppliedCompletion)
	}
	return CompletionSourceFn(convertOutcomeProtoFn)
}

// describe returns the CallbackExecutionInfo for the describe RPC. Only applies to standalone callbacks.
func (c *Callback) describe(ctx chasm.Context) (*callbackpb.CallbackExecutionInfo, error) {
	apiCb, err := c.ToAPICallback()
	if err != nil {
		return nil, err
	}

	exInfo := ctx.ExecutionInfo()
	info := &callbackpb.CallbackExecutionInfo{
		CallbackId:              ctx.ExecutionKey().BusinessID,
		RunId:                   ctx.ExecutionKey().RunID,
		Callback:                apiCb,
		Status:                  callbackStatusToAPIExecutionStatus(c.Status),
		State:                   callbackStatusToAPIState(c.Status),
		Attempt:                 c.Attempt,
		CreateTime:              c.RegistrationTime,
		LastAttemptCompleteTime: c.LastAttemptCompleteTime,
		LastAttemptFailure:      c.LastAttemptFailure,
		NextAttemptScheduleTime: c.NextAttemptScheduleTime,
		CloseTime:               c.CloseTime,
		ScheduleToCloseTimeout:  c.ScheduleToCloseTimeout,
		StateTransitionCount:    exInfo.StateTransitionCount,
	}
	return info, nil
}

// outcome returns the callback execution outcome if the execution is in a terminal state. (Otherwise, nil.)
//
// IMPORTANT: This is specific to the callback delivery, and not the actual completion. The outcome will be
// a success even if it was to deliver a failed completion result.
func (c *Callback) outcome(ctx chasm.Context) *callbackpb.CallbackExecutionOutcome {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		val := &callbackpb.CallbackExecutionOutcome_Success{}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}

	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED:
		val := &callbackpb.CallbackExecutionOutcome_Failure{
			Failure: c.TerminalFailure.Get(ctx),
		}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}

	default:
		return nil
	}
}

// callbackStatusToAPIExecutionStatus maps internal CallbackStatus to public API CallbackExecutionStatus.
func callbackStatusToAPIExecutionStatus(status callbackspb.CallbackStatus) enumspb.CallbackExecutionStatus {
	switch status {
	case callbackspb.CALLBACK_STATUS_STANDBY,
		callbackspb.CALLBACK_STATUS_SCHEDULED,
		callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_EXECUTION_STATUS_RUNNING
	case callbackspb.CALLBACK_STATUS_FAILED:
		return enumspb.CALLBACK_EXECUTION_STATUS_FAILED
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_EXECUTION_STATUS_SUCCEEDED
	case callbackspb.CALLBACK_STATUS_TERMINATED:
		return enumspb.CALLBACK_EXECUTION_STATUS_TERMINATED
	default:
		return enumspb.CALLBACK_EXECUTION_STATUS_UNSPECIFIED
	}
}

// callbackStatusToAPIState maps internal CallbackStatus to public API CallbackState.
func callbackStatusToAPIState(status callbackspb.CallbackStatus) enumspb.CallbackState {
	switch status {
	case callbackspb.CALLBACK_STATUS_STANDBY:
		return enumspb.CALLBACK_STATE_STANDBY
	case callbackspb.CALLBACK_STATUS_SCHEDULED:
		return enumspb.CALLBACK_STATE_SCHEDULED
	case callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_STATE_BACKING_OFF
	case callbackspb.CALLBACK_STATUS_FAILED:
		return enumspb.CALLBACK_STATE_FAILED
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_STATE_SUCCEEDED
	case callbackspb.CALLBACK_STATUS_TERMINATED:
		return enumspb.CALLBACK_STATE_TERMINATED
	default:
		return enumspb.CALLBACK_STATE_UNSPECIFIED
	}
}
