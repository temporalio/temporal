// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package callbacks

import (
	"context"
	"fmt"
	"net/http"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
)

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)
type HTTPCallerProvider func(queues.NamespaceIDAndDestination) HTTPCaller

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
) error {
	exec := taskExecutor{executorOptions}
	if err := hsm.RegisterImmediateExecutor(
		registry,
		exec.executeInvocationTask,
	); err != nil {
		return err
	}
	return hsm.RegisterTimerExecutor(
		registry,
		exec.executeBackoffTask,
	)
}

type TaskExecutorOptions struct {
	fx.In

	Config             *Config
	NamespaceRegistry  namespace.Registry
	MetricsHandler     metrics.Handler
	Logger             log.Logger
	HTTPCallerProvider HTTPCallerProvider
	HistoryClient      resource.HistoryClient
}

type taskExecutor struct {
	TaskExecutorOptions
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
	Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult
	// WrapError provides each variant the opportunity to wrap the error returned by the task executor for, e.g. to
	// trigger the circuit breaker.
	WrapError(result invocationResult, err error) error
}

func (e taskExecutor) executeInvocationTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task InvocationTask,
) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}

	invokable, err := e.loadInvocationArgs(ctx, env, ref)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.Destination()),
	)
	defer cancel()

	result := invokable.Invoke(callCtx, ns, e, task)
	saveErr := e.saveResult(callCtx, env, ref, result)
	return invokable.WrapError(result, saveErr)
}

func (e taskExecutor) loadInvocationArgs(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
) (invokable callbackInvokable, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		callback, err := hsm.MachineData[Callback](node)
		if err != nil {
			return err
		}

		switch variant := callback.GetCallback().GetVariant().(type) {
		case *persistencespb.Callback_Nexus_:
			target, err := hsm.MachineData[CanGetNexusCompletion](node.Parent)
			if err != nil {
				return err
			}
			// variant struct is immutable and ok to reference without copying
			nexusInvokable := nexusInvocation{}
			nexusInvokable.nexus = variant.Nexus
			nexusInvokable.completion, err = target.GetNexusCompletion(ctx)
			invokable = nexusInvokable
			if err != nil {
				return err
			}
		case *persistencespb.Callback_Hsm:
			target, err := hsm.MachineData[CanGetHSMCompletionCallbackArg](node.Parent)
			if err != nil {
				return err
			}
			// variant struct is immutable and ok to reference without copying
			hsmInvokable := hsmInvocation{}
			hsmInvokable.hsm = variant.Hsm
			hsmInvokable.callbackArg, err = target.GetHSMCompletionCallbackArg(ctx)
			if err != nil {
				return err
			}
			invokable = hsmInvokable
		default:
			return queues.NewUnprocessableTaskError(
				fmt.Sprintf("unprocessable callback variant: %v", variant),
			)
		}
		return nil
	})
	return
}

func (e taskExecutor) saveResult(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	result invocationResult,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(callback Callback) (hsm.TransitionOutput, error) {
			switch result.(type) {
			case invocationResultOK:
				return TransitionSucceeded.Apply(callback, EventSucceeded{
					Time: env.Now(),
				})
			case invocationResultRetry:
				return TransitionAttemptFailed.Apply(callback, EventAttemptFailed{
					Time:        env.Now(),
					Err:         result.error(),
					RetryPolicy: e.Config.RetryPolicy(),
				})
			case invocationResultFail:
				return TransitionFailed.Apply(callback, EventFailed{
					Time: env.Now(),
					Err:  result.error(),
				})
			default:
				return hsm.TransitionOutput{}, queues.NewUnprocessableTaskError(fmt.Sprintf("unrecognized callback result %v", result))
			}
		})
	})
}

func (e taskExecutor) executeBackoffTask(
	env hsm.Environment,
	node *hsm.Node,
	task BackoffTask,
) error {
	return hsm.MachineTransition(node, func(callback Callback) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(callback, EventRescheduled{})
	})
}
