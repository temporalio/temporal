// Package callback provides CHASM-based callback execution infrastructure.
//
// This package is a port of the HSM-based callback execution system from
// components/callbacks/executors.go.
//
// HSM Pattern (Old):
//   - Used hsm.Registry with RegisterImmediateExecutor and RegisterTimerExecutor
//   - Task execution through env.Access() with hsm.AccessRead/Write
//   - State transitions via hsm.MachineTransition with explicit transition types
//   - Results returned as invocationResult interface types
//   - LoadInvocationArgs and saveResult were separate reusable methods
//
// CHASM Pattern (New):
//   - Explicit executor structs with Execute() and Validate() methods
//   - Component access through chasm.ReadComponent and chasm.UpdateComponent
//   - Direct state manipulation by setting fields (Status, NextAttemptScheduleTime)
//   - Results returned as CallbackStatus enum values
//   - Logic inlined in Execute() methods for clarity
//
// The core HTTP invocation logic remains unchanged - only the state management
// framework differs.
package callback

import (
	"context"
	"fmt"
	"net/http"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
)

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)
type HTTPCallerProvider func(queues.NamespaceIDAndDestination) HTTPCaller

// InvocationTaskExecutor is responsible for the invocation of a callback.
// For Nexus callbacks this will be an HTTP call, for other callbacks this
// could be any CHASM invocation.
// This is the CHASM port of the HSM taskExecutor.executeInvocationTask functionality
// from components/callbacks/executors.go.
type InvocationTaskExecutor struct {
	InvocationTaskExecutorOptions
}

type InvocationTaskExecutorOptions struct {
	fx.In

	Config             *Config
	MetricsHandler     metrics.Handler
	Logger             log.Logger
	NamespaceRegistry  namespace.Registry
	HTTPCallerProvider HTTPCallerProvider
	HTTPTraceProvider  commonnexus.HTTPClientTraceProvider
	HistoryClient      resource.HistoryClient
}

func NewInvocationTaskExecutor(opts InvocationTaskExecutorOptions) *InvocationTaskExecutor {
	return &InvocationTaskExecutor{
		InvocationTaskExecutorOptions: opts,
	}
}

func (e *InvocationTaskExecutor) Execute(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
	taskAttributes chasm.TaskAttributes,
	task *callbackspb.InvocationTask,
) error {
	var ns *namespace.Namespace
	var invoker *Invoker
	var callback *Callback

	// Read the invoker component and load invocation arguments.
	// This replaces the HSM pattern of env.Access(ctx, ref, hsm.AccessRead, ...) with
	// CHASM's chasm.ReadComponent(). We extract the callback data and prepare the
	// nexusInvocation struct fields (completion, attempt, nexus variant) similar to
	// HSM's loadInvocationArgs() in executors.go:132-194.
	_, err := chasm.ReadComponent(
		ctx,
		invokerRef,
		func(i *Invoker, ctx chasm.Context, _ any) (struct{}, error) {
			invoker = &Invoker{
				InvokerState: common.CloneProto(i.InvokerState),
			}
			nexusCompletion, err := i.CanGetNexusCompletion.Get(ctx)
			if err != nil {
				return struct{}{}, err
			}

			c, err := i.Callback.Get(ctx)
			if err != nil {
				return struct{}{}, err
			}

			invoker.completion, err = nexusCompletion.GetNexusCompletion(
				context.Background(),
				c.RequestId,
			)
			// TODO seankane: where do we get these?
			// invoker.workflowID = c.WorkflowID
			// invoker.runID = c.RunID
			invoker.attempt = c.Attempt
			invoker.nexus = c.Callback.GetNexus()
			if err != nil {
				return struct{}{}, err
			}

			callback = &Callback{
				CallbackState: common.CloneProto(c.CallbackState),
				NamespaceID:   namespace.ID(invoker.NamespaceId),
			}
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read component: %w", err)
	}

	callCtx, cancel := context.WithTimeout(
		context.Background(),
		e.Config.RequestTimeout(invoker.NamespaceId, taskAttributes.Destination),
	)
	defer cancel()

	result := invoker.Invoke(callCtx, ns, e, taskAttributes, task)

	// Save the invocation result back to the callback component.
	// Replaces the pattern of env.Access(ctx, ref, hsm.AccessWrite, ...) and
	// hsm.MachineTransition() from executors.go:197-226. In HSM, the result was mapped
	// to TransitionSucceeded/TransitionAttemptFailed/TransitionFailed. In CHASM, we
	// directly update the callback's Status field to SUCCEEDED/BACKING_OFF/FAILED.
	_, _, err = chasm.UpdateComponent(
		ctx,
		invokerRef,
		func(i *Invoker, ctx chasm.MutableContext, _ any) (struct{}, error) {
			c, err := i.Callback.Get(ctx)
			if err != nil {
				return struct{}{}, err
			}
			c.Status = result

			c.CallbackState = callback.CallbackState

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update component: %w", err)
	}

	return nil
}

func (e *InvocationTaskExecutor) Validate(
	ctx chasm.Context,
	callback *Callback,
	_ chasm.TaskAttributes,
	_ *callbackspb.InvocationTask,
) (bool, error) {
	return callback.Status == callbackspb.CALLBACK_STATUS_SCHEDULED, nil
}

// BackoffTaskExecutor is responsible for the retry scheduling after failed
// attempts. This is a timer executor that will fire when the backoff period
// expires and the callback should be retried.
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
// and generates an InvocationTask for the next attempt. Directly 
// manipulate the state schedule the next invocation attempt.
func (e *BackoffTaskExecutor) Execute(
	ctx chasm.MutableContext,
	callback *Callback,
	_ chasm.TaskAttributes,
	_ *callbackspb.BackoffTask,
) error {
	// Clear the next attempt schedule time and transition to scheduled
	callback.NextAttemptScheduleTime = nil
	callback.Status = callbackspb.CALLBACK_STATUS_SCHEDULED

	// Generate an invocation task for the next attempt
	invocationTask, err := e.generateInvocationTask(callback)
	if err != nil {
		return fmt.Errorf("failed to generate invocation task: %w", err)
	}

	// Add the task to be executed
	ctx.AddTask(callback, chasm.TaskAttributes{}, invocationTask)

	return nil
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

// generateInvocationTask creates an InvocationTask based on the callback variant
func (e *BackoffTaskExecutor) generateInvocationTask(callback *Callback) (*callbackspb.InvocationTask, error) {
	switch variant := callback.Callback.GetVariant().(type) {
	case *callbackspb.Callback_Nexus_:
		// For Nexus callbacks, extract the destination from the URL
		// This matches the HSM behavior of extracting scheme + host
		url := variant.Nexus.Url
		// For now, we'll use the full URL as destination
		// TODO seankane: Extract just scheme + host like HSM does
		return &callbackspb.InvocationTask{
			Url: url,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported callback variant: %v", variant)
	}
}
