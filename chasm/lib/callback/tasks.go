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
	"go.temporal.io/server/service/history/queues/common"
	"go.uber.org/fx"
)

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)

// HTTPCallerProvider is a method that can be used to retrieve an HTTPCaller for a given namespace and destination.
type HTTPCallerProvider func(common.NamespaceIDAndDestination) HTTPCaller

// invocationResult is a marker for the callbackInvokable.Invoke result to indicate to the handler how to handle the
// invocation outcome.
type invocationResult interface {
	// A marker for all possible implementations.
	mustImplementInvocationResult()
	error() error
}

// invocationResultOK marks an invocation as successful.
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

type invocable interface {
	// Invoke executes the callback logic and returns the invocation result.
	Invoke(ctx context.Context, ns *namespace.Namespace, h *invocationTaskHandler, task *callbackspb.InvocationTask, taskAttr chasm.TaskAttributes) invocationResult
	// WrapError provides each variant the opportunity to wrap the error returned by the task handler for, e.g. to
	// trigger the circuit breaker.
	WrapError(result invocationResult, err error) error
}

type invocationTaskHandlerOptions struct {
	fx.In

	Config             *Config
	NamespaceRegistry  namespace.Registry
	MetricsHandler     metrics.Handler
	Logger             log.Logger
	HTTPCallerProvider HTTPCallerProvider
	HTTPTraceProvider  commonnexus.HTTPClientTraceProvider
	HistoryClient      resource.HistoryClient
}

type invocationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*callbackspb.InvocationTask]
	config             *Config
	namespaceRegistry  namespace.Registry
	metricsHandler     metrics.Handler
	logger             log.Logger
	httpCallerProvider HTTPCallerProvider
	httpTraceProvider  commonnexus.HTTPClientTraceProvider
	historyClient      resource.HistoryClient
}

func newInvocationTaskHandler(opts invocationTaskHandlerOptions) *invocationTaskHandler {
	return &invocationTaskHandler{
		config:             opts.Config,
		namespaceRegistry:  opts.NamespaceRegistry,
		metricsHandler:     opts.MetricsHandler,
		logger:             opts.Logger,
		httpCallerProvider: opts.HTTPCallerProvider,
		httpTraceProvider:  opts.HTTPTraceProvider,
		historyClient:      opts.HistoryClient,
	}
}

func (h *invocationTaskHandler) Validate(ctx chasm.Context, cb *Callback, attrs chasm.TaskAttributes, task *callbackspb.InvocationTask) (bool, error) {
	return cb.Attempt == task.Attempt && cb.Status == callbackspb.CALLBACK_STATUS_SCHEDULED, nil
}

func (h *invocationTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	taskAttr chasm.TaskAttributes,
	task *callbackspb.InvocationTask,
) error {
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}

	invokable, err := chasm.ReadComponent(
		ctx,
		ref,
		(*Callback).loadInvocationArgs,
		nil,
	)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		h.config.RequestTimeout(ns.Name().String(), taskAttr.Destination),
	)
	defer cancel()

	result := invokable.Invoke(callCtx, ns, h, task, taskAttr)
	_, _, saveErr := chasm.UpdateComponent(
		ctx,
		ref,
		(*Callback).saveResult,
		saveResultInput{
			result:      result,
			retryPolicy: h.config.RetryPolicy(),
		},
	)
	return invokable.WrapError(result, saveErr)
}

type backoffTaskHandler struct {
	chasm.PureTaskHandlerBase
}

type backoffTaskHandlerOptions struct {
	fx.In
}

func newBackoffTaskHandler(opts backoffTaskHandlerOptions) *backoffTaskHandler {
	return &backoffTaskHandler{}
}

// Execute toggles the callback status from BACKING_OFF to SCHEDULED to trigger a new invocation attempt.
func (h *backoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	callback *Callback,
	taskAttrs chasm.TaskAttributes,
	task *callbackspb.BackoffTask,
) error {
	return TransitionRescheduled.Apply(callback, ctx, EventRescheduled{})
}

// Validate validates that the callback is in BACKING_OFF state and that the attempt number matches before allowing the
// backoff task to execute.
func (h *backoffTaskHandler) Validate(
	ctx chasm.Context,
	callback *Callback,
	taskAttr chasm.TaskAttributes,
	task *callbackspb.BackoffTask,
) (bool, error) {
	return callback.Status == callbackspb.CALLBACK_STATUS_BACKING_OFF && callback.Attempt == task.Attempt, nil
}
