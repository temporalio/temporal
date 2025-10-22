package callback

import (
	"context"
	"fmt"
	"net/http"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)
type HTTPCallerProvider func(queues.NamespaceIDAndDestination) HTTPCaller

func NewInvocationTaskExecutor(opts InvocationTaskExecutorOptions) *InvocationTaskExecutor {
	return &InvocationTaskExecutor{
		InvocationTaskExecutorOptions: opts,
	}
}

type InvocationTaskExecutorOptions struct {
	fx.In

	Config             *Config
	NamespaceRegistry  namespace.Registry
	MetricsHandler     metrics.Handler
	Logger             log.Logger
	HTTPCallerProvider HTTPCallerProvider
	HTTPTraceProvider  commonnexus.HTTPClientTraceProvider
	HistoryClient      resource.HistoryClient
	ChasmEngine        chasm.Engine
}

type InvocationTaskExecutor struct {
	InvocationTaskExecutorOptions
}

func (e InvocationTaskExecutor) Execute(ctx context.Context, ref chasm.ComponentRef, attrs chasm.TaskAttributes, task InvocationTask) error {
	return e.executeInvocationTask(ctx, ref, attrs, task)
}

func (e InvocationTaskExecutor) Validate(ctx chasm.Context, cb Callback, attrs chasm.TaskAttributes, task InvocationTask) (bool, error) {
	return cb.Attempt == task.attempt && cb.Status == callbackspb.CALLBACK_STATUS_SCHEDULED, nil
}

// invocationResult is a marker for the callbackInvokable.Invoke result to indicate to the executor how to handle the
// invocation outcome.
type invocationResult interface {
	// A marker for all possible implementations.
	mustImplementInvocationResult()
	error() error
}

// invocationResultFail marks an invocation as successful.
type invocationResultOK struct{}

func (invocationResultOK) mustImplementInvocationResult() {}

func (invocationResultOK) error() error {
	return nil
}

// invocationResultFail marks an invocation as permanently failed.
type invocationResultFail struct {
	err error
}

func (invocationResultFail) mustImplementInvocationResult() {}

func (r invocationResultFail) error() error {
	return r.err
}

// invocationResultRetry marks an invocation as failed with the intent to retry.
type invocationResultRetry struct {
	err error
}

func (invocationResultRetry) mustImplementInvocationResult() {}

func (r invocationResultRetry) error() error {
	return r.err
}

type callbackInvokable interface {
	// Invoke executes the callback logic and returns the invocation result.
	Invoke(ctx context.Context, ns *namespace.Namespace, e InvocationTaskExecutor, task InvocationTask) invocationResult
	// WrapError provides each variant the opportunity to wrap the error returned by the task executor for, e.g. to
	// trigger the circuit breaker.
	WrapError(result invocationResult, err error) error
}

func (e InvocationTaskExecutor) executeInvocationTask(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task InvocationTask,
) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}

	invokable, err := e.loadInvocationArgs(ctx, ref)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.destination),
	)
	defer cancel()

	result := invokable.Invoke(callCtx, ns, e, task)
	saveErr := e.saveResult(ctx, ref, result)
	return invokable.WrapError(result, saveErr)
}

func (e InvocationTaskExecutor) loadInvocationArgs(
	ctx context.Context,
	ref chasm.ComponentRef,
) (invokable callbackInvokable, err error) {
	return chasm.ReadComponent(
		ctx,
		ref,
		func(component *Callback, chasmCtx chasm.Context, _ any) (callbackInvokable, error) {
			target, err := component.CanGetNexusCompletion.Get(chasmCtx)
			if err != nil {
				return nil, err
			}

			completion, err := target.GetNexusCompletion(ctx, component.RequestId)
			if err != nil {
				return nil, err
			}

			switch variant := component.GetCallback().GetVariant().(type) {
			case *callbackspb.Callback_Nexus:
				if variant.Nexus.Url == chasm.NexusCompletionHandlerURL {
					return chasmInvocation{
						nexus:      variant.Nexus,
						attempt:    component.Attempt,
						completion: completion,
						requestID:  component.RequestId,
					}, nil
				} else {
					return nexusInvocation{
						nexus:      variant.Nexus,
						completion: completion,
						workflowID: component.WorkflowId,
						runID:      component.RunId,
						attempt:    component.Attempt,
					}, nil
				}
			default:
				return nil, queues.NewUnprocessableTaskError(
					fmt.Sprintf("unprocessable callback variant: %v", variant),
				)
			}
		},
		nil,
	)
}

func (e InvocationTaskExecutor) saveResult(
	ctx context.Context,
	ref chasm.ComponentRef,
	result invocationResult,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(component *Callback, ctx chasm.MutableContext, _ any) (struct{}, error) {
			switch result.(type) {
			case invocationResultOK:
				component.Status = callbackspb.CALLBACK_STATUS_SCHEDULED
				component.LastAttemptCompleteTime = timestamppb.New(ctx.Now(component))
			case invocationResultRetry:
				component.Status = callbackspb.CALLBACK_STATUS_BACKING_OFF
				component.LastAttemptCompleteTime = timestamppb.New(ctx.Now(component))
				// TODO (seankane): Calculate backoff and set NextAttemptScheduleTime
				// TODO (seankane): Add backoff task
			case invocationResultFail:
				component.Status = callbackspb.CALLBACK_STATUS_FAILED
				component.LastAttemptCompleteTime = timestamppb.New(ctx.Now(component))
			default:
				return struct{}{}, queues.NewUnprocessableTaskError(
					fmt.Sprintf("unrecognized callback result %v", result),
				)
			}

			return struct{}{}, nil
		},
		nil,
	)
	return err
}

type BackoffTaskExecutor struct {
	BackoffTaskExecutorOptions
}

type BackoffTaskExecutorOptions struct {
	fx.In

	Config         *Config
	MetricsHandler metrics.Handler
	Logger         log.Logger
}

func NewBackoffTaskExecutor(opts BackoffTaskExecutorOptions) *BackoffTaskExecutor {
	return &BackoffTaskExecutor{
		BackoffTaskExecutorOptions: opts,
	}
}

// Execute transitions the callback from BACKING_OFF to SCHEDULED state
// and generates an InvocationTask for the next attempt.
func (e *BackoffTaskExecutor) Execute(
	ctx chasm.MutableContext,
	callback *Callback,
	taskAttrs chasm.TaskAttributes,
	task *callbackspb.BackoffTask,
) error {
	// Create a taskExecutor wrapper with the same options
	executor := InvocationTaskExecutor{
		InvocationTaskExecutorOptions: InvocationTaskExecutorOptions{
			Config:         e.Config,
			MetricsHandler: e.MetricsHandler,
			Logger:         e.Logger,
		},
	}

	// Convert the CHASM task to the internal BackoffTask type
	// Note: BackoffTask proto is empty, deadline comes from NextAttemptScheduleTime in callback
	backoffTask := BackoffTask{
		deadline: callback.NextAttemptScheduleTime.AsTime(),
	}

	// Delegate to the taskExecutor implementation
	return executor.executeBackoffTask(ctx, callback, taskAttrs, backoffTask)
}

func (e *BackoffTaskExecutor) Validate(
	ctx chasm.Context,
	callback *Callback,
	_ chasm.TaskAttributes,
	_ *callbackspb.BackoffTask,
) (bool, error) {
	// Validate that the callback is in BACKING_OFF state
	return callback.Status == callbackspb.CALLBACK_STATUS_BACKING_OFF, nil
}

func (e InvocationTaskExecutor) executeBackoffTask(
	ctx chasm.MutableContext,
	callback *Callback,
	attrs chasm.TaskAttributes,
	task BackoffTask,
) error {
	callback.Status = callbackspb.CALLBACK_STATUS_SCHEDULED
	callback.NextAttemptScheduleTime = nil

	invocationTask := InvocationTask{destination: attrs.Destination}
	chasmAttrs := chasm.TaskAttributes{
		ScheduledTime: chasm.TaskScheduledTimeImmediate,
		Destination:   attrs.Destination,
	}
	ctx.AddTask(callback, chasmAttrs, invocationTask)
	return nil
}
