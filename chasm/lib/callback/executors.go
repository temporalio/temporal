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
type HTTPCallerProvider func(common.NamespaceIDAndDestination) HTTPCaller

func NewInvocationTaskExecutor(opts InvocationTaskExecutorOptions) *InvocationTaskExecutor {
	return &InvocationTaskExecutor{
		config:             opts.Config,
		namespaceRegistry:  opts.NamespaceRegistry,
		metricsHandler:     opts.MetricsHandler,
		logger:             opts.Logger,
		httpCallerProvider: opts.HTTPCallerProvider,
		httpTraceProvider:  opts.HTTPTraceProvider,
		historyClient:      opts.HistoryClient,
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
}

type InvocationTaskExecutor struct {
	config             *Config
	namespaceRegistry  namespace.Registry
	metricsHandler     metrics.Handler
	logger             log.Logger
	httpCallerProvider HTTPCallerProvider
	httpTraceProvider  commonnexus.HTTPClientTraceProvider
	historyClient      resource.HistoryClient
}

func (e InvocationTaskExecutor) Execute(ctx context.Context, ref chasm.ComponentRef, attrs chasm.TaskAttributes, task *callbackspb.InvocationTask) error {
	return e.Invoke(ctx, ref, attrs, task)
}

func (e InvocationTaskExecutor) Validate(ctx chasm.Context, cb *Callback, attrs chasm.TaskAttributes, task *callbackspb.InvocationTask) (bool, error) {
	return cb.Attempt == task.Attempt && cb.Status == callbackspb.CALLBACK_STATUS_SCHEDULED, nil
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
	Invoke(ctx context.Context, ns *namespace.Namespace, e InvocationTaskExecutor, task *callbackspb.InvocationTask, taskAttr chasm.TaskAttributes) invocationResult
	// WrapError provides each variant the opportunity to wrap the error returned by the task executor for, e.g. to
	// trigger the circuit breaker.
	WrapError(result invocationResult, err error) error
}

func (e InvocationTaskExecutor) Invoke(
	ctx context.Context,
	ref chasm.ComponentRef,
	taskAttr chasm.TaskAttributes,
	task *callbackspb.InvocationTask,
) error {
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
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
		e.config.RequestTimeout(ns.Name().String(), taskAttr.Destination),
	)
	defer cancel()

	result := invokable.Invoke(callCtx, ns, e, task, taskAttr)
	_, _, saveErr := chasm.UpdateComponent(
		ctx,
		ref,
		(*Callback).saveResult,
		saveResultInput{
			result:      result,
			retryPolicy: e.config.RetryPolicy(),
		},
	)
	return invokable.WrapError(result, saveErr)
}

type BackoffTaskExecutor struct {
	config         *Config
	metricsHandler metrics.Handler
	logger         log.Logger
}

type BackoffTaskExecutorOptions struct {
	fx.In

	Config         *Config
	MetricsHandler metrics.Handler
	Logger         log.Logger
}

func NewBackoffTaskExecutor(opts BackoffTaskExecutorOptions) *BackoffTaskExecutor {
	return &BackoffTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
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
	return TransitionRescheduled.Apply(callback, ctx, EventRescheduled{})
}

func (e *BackoffTaskExecutor) Validate(
	ctx chasm.Context,
	callback *Callback,
	taskAttr chasm.TaskAttributes,
	task *callbackspb.BackoffTask,
) (bool, error) {
	// Validate that the callback is in BACKING_OFF state
	return callback.Status == callbackspb.CALLBACK_STATUS_BACKING_OFF && callback.Attempt == task.Attempt, nil
}
