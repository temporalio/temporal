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

type (
	TaskExecutorOptions struct {
		fx.In

		Config             *Config
		NamespaceRegistry  namespace.Registry
		MetricsHandler     metrics.Handler
		Logger             log.Logger
		HTTPCallerProvider HTTPCallerProvider
		HistoryClient      resource.HistoryClient
	}

	taskExecutor struct {
		TaskExecutorOptions
	}

	invocationResult int

	callbackInvokable interface {
		// Invoke executes the callback logic and returns a result, and the error to be logged in the state machine.
		Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) (invocationResult, error)
		// WrapError provides each variant the opportunity to return a different error up the call stack than the one logged.
		WrapError(result invocationResult, err error) error
	}
)

const (
	ok invocationResult = iota
	retry
	failed
)

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
		e.Config.RequestTimeout(ns.Name().String(), task.Destination),
	)
	defer cancel()

	result, err := invokable.Invoke(callCtx, ns, e, task)

	saveErr := e.saveResult(callCtx, env, ref, result, err)
	if saveErr != nil {
		return saveErr
	}
	return invokable.WrapError(result, err)
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
			if err != nil {
				return err
			}
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
	callErr error,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(callback Callback) (hsm.TransitionOutput, error) {
			switch result {
			case ok:
				return TransitionSucceeded.Apply(callback, EventSucceeded{
					Time: env.Now(),
				})
			case retry:
				return TransitionAttemptFailed.Apply(callback, EventAttemptFailed{
					Time:        env.Now(),
					Err:         callErr,
					RetryPolicy: e.Config.RetryPolicy(),
				})
			case failed:
				return TransitionFailed.Apply(callback, EventFailed{
					Time: env.Now(),
					Err:  callErr,
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
