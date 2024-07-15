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

	"go.uber.org/fx"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
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

		Config            *Config
		NamespaceRegistry namespace.Registry
		MetricsHandler    metrics.Handler
		Logger            log.Logger
		// TODO(Tianyu): Is there any way to move this off to Nexus specific code?
		CallerProvider HTTPCallerProvider
		HistoryClient  resource.HistoryClient
	}

	taskExecutor struct {
		TaskExecutorOptions
	}

	invocationResult int

	callbackInvokable interface {
		Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) (invocationResult, error)
	}
)

const (
	Ok invocationResult = iota
	Retry
	Failed
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

	// If the request permanently failed there is no need to raise the error
	if result == Failed {
		return nil
	}
	return err
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
			args := nexusInvocation{}
			args.nexus = variant.Nexus
			args.completion, err = target.GetNexusCompletion(ctx)
			invokable = args
			if err != nil {
				return err
			}
		case *persistencespb.Callback_Hsm_:
			target, err := hsm.MachineData[CanGetCompletionEvent](node.Parent)
			if err != nil {
				return err
			}
			args := hsmInvocation{}
			args.hsm = variant.Hsm
			args.completionEvent, err = target.GetCompletionEvent(ctx)
			if err != nil {
				return err
			}
			invokable = args
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
			case Ok:
				return TransitionSucceeded.Apply(callback, EventSucceeded{})
			case Retry:
				return TransitionAttemptFailed.Apply(callback, EventAttemptFailed{
					Time:        env.Now(),
					Err:         callErr,
					RetryPolicy: e.Config.RetryPolicy(),
				})
			case Failed:
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
