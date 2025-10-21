package callback

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

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

	// Read the invoker component and load invocation arguments.
	callback, err := chasm.ReadComponent(
		ctx,
		invokerRef,
		func(c *Callback, ctx chasm.Context, _ any) (*Callback, error) {
			callback := &Callback{
				CallbackState: common.CloneProto(c.CallbackState),
			}

			nexusCompletion, err := c.CanGetNexusCompletion.Get(ctx)
			if err != nil {
				return nil, err
			}

			c.completion, err = nexusCompletion.GetNexusCompletion(
				context.Background(),
				c.RequestId,
			)
			callback.WorkflowId = c.WorkflowId
			callback.RunId = c.RunId
			callback.Attempt = c.Attempt
			callback.Callback = c.Callback
			callback.NamespaceId = c.NamespaceId
			if err != nil {
				return nil, err
			}

			return callback, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read component: %w", err)
	}

	callCtx, cancel := context.WithTimeout(
		context.Background(),
		e.Config.RequestTimeout(callback.NamespaceId, taskAttributes.Destination),
	)
	defer cancel()

	result := callback.invoke(callCtx, ns, e, taskAttributes, task)

	// Save the invocation result back to the callback component.
	// Replaces the pattern of env.Access(ctx, ref, hsm.AccessWrite, ...) and
	// hsm.MachineTransition() from executors.go:197-226. In HSM, the result was mapped
	// to TransitionSucceeded/TransitionAttemptFailed/TransitionFailed. In CHASM, we
	// directly update the callback's Status field to SUCCEEDED/BACKING_OFF/FAILED.
	_, _, err = chasm.UpdateComponent(
		ctx,
		invokerRef,
		func(c *Callback, ctx chasm.MutableContext, _ any) (struct{}, error) {
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
	taskAttributes chasm.TaskAttributes,
	invocationTask *callbackspb.InvocationTask,
) (bool, error) {
	return callback.Attempt == invocationTask.Attempt &&
		callback.Status == callbackspb.CALLBACK_STATUS_SCHEDULED, nil
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

	// Generate an invocation task and task attributes for the next attempt
	invocationTask, taskAttrs, err := e.generateInvocationTask(callback)
	if err != nil {
		return fmt.Errorf("failed to generate invocation task: %w", err)
	}

	// Add the task to be executed
	ctx.AddTask(callback, taskAttrs, invocationTask)

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

// generateInvocationTask creates an InvocationTask and TaskAttributes based on the callback variant.
// This is the CHASM port of HSM's RegenerateTasks logic from statemachine.go:79-100.
// The destination (scheme + host) is extracted from the URL and used for task routing,
// matching HSM's pattern where InvocationTask stores destination as u.Scheme + "://" + u.Host.
func (e *BackoffTaskExecutor) generateInvocationTask(callback *Callback) (*callbackspb.InvocationTask, chasm.TaskAttributes, error) {
	switch variant := callback.Callback.GetVariant().(type) {
	case *callbackspb.Callback_Nexus:
		// Parse URL to extract scheme and host, matching HSM's behavior
		// from statemachine.go:86-90
		u, err := url.Parse(variant.Nexus.Url)
		if err != nil {
			return nil, chasm.TaskAttributes{}, fmt.Errorf("failed to parse URL: %w", err)
		}

		// Extract destination (scheme + host) for task routing
		destination := u.Scheme + "://" + u.Host

		return &callbackspb.InvocationTask{
				Attempt: callback.Attempt,
				Url:     variant.Nexus.Url,
			},
			chasm.TaskAttributes{
				Destination: destination,
			},
			nil
	default:
		return nil, chasm.TaskAttributes{}, fmt.Errorf("unsupported callback variant: %v", variant)
	}
}
