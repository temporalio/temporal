package callback

import (
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
)

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)
type HTTPCallerProvider func(queues.NamespaceIDAndDestination) HTTPCaller

// InvocationTaskExecutor is responsible for the invocation of a callback.
// For Nexus callbacks this will be an HTTP call, for other callbacks this
// could be any CHASM invocation. (?)
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
	ctx chasm.MutableContext,
	callback *Callback,
	_ chasm.TaskAttributes,
	_ *callbackspb.InvocationTask,
) error {
	return fmt.Errorf("not implemented")
}

// BackoffTaskExecutor is responsible for the retry scheduling after failed
// attempts. This is a timer executor that will fire if a response is
// not received in time. (?)
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
// and generates an InvocationTask for the next attempt
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
		// TODO: Extract just scheme + host like HSM does
		return &callbackspb.InvocationTask{
			Url: url,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported callback variant: %v", variant)
	}
}
