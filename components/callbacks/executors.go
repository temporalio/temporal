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
	historypb "go.temporal.io/api/history/v1"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.uber.org/fx"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type CanGetNexusCompletion interface {
	GetNexusCompletion(ctx context.Context) (nexus.OperationCompletion, error)
}

type CanGetCompletionEvent interface {
	GetCompletionEvent(ctx context.Context) (*historypb.HistoryEvent, error)
}

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
		CallerProvider    HTTPCallerProvider
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
	
	return e.saveResult(callCtx, env, ref, result, err)
}

type nexusInvocationArgs struct {
	url        string
	header     map[string]string
	completion nexus.OperationCompletion
}

func (args nexusInvocationArgs) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) (invocationResult, error) {
	request, err := nexus.NewCompletionHTTPRequest(ctx, args.url, args.completion)
	if err != nil {
		return Failed, queues.NewUnprocessableTaskError(
			fmt.Sprintf("failed to construct Nexus request: %v", err),
		)
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	for k, v := range args.header {
		request.Header.Set(k, v)
	}

	caller := e.CallerProvider(queues.NamespaceIDAndDestination{
		NamespaceID: ns.ID().String(),
		Destination: task.Destination,
	})
	// Make the call and record metrics.
	startTime := time.Now()
	response, callErr := caller(request)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(task.Destination)
	statusCodeTag := metrics.NexusOutcomeTag(outcomeTag(ctx, response, callErr))
	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if callErr == nil {
		// Body is not read but should be discarded to keep the underlying TCP connection alive.
		// Just in case something unexpected happens while discarding or closing the body,
		// propagate errors to the machine.
		if _, callErr = io.Copy(io.Discard, response.Body); callErr == nil {
			callErr = response.Body.Close()
			if isRetryableHTTPResponse(response) {
				return Retry, queues.NewDestinationDownError(
					fmt.Sprintf("response returned retryable status code %d", response.StatusCode),
					err,
				)
			}
			return Ok, nil
		}
	}
	status := response.Status
	retryable := isRetryableHTTPResponse(response)
	e.Logger.Error("Callback request failed", tag.Error(callErr), tag.NewStringTag("status", status), tag.NewBoolTag("retryable", retryable))
	return Failed, queues.NewDestinationDownError(callErr.Error(), err)
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
			args := nexusInvocationArgs{
				url:    variant.Nexus.GetUrl(),
				header: variant.Nexus.GetHeader(),
			}
			args.completion, err = target.GetNexusCompletion(ctx)
			invokable = args
			if err != nil {
				return err
			}
		case *persistencespb.Callback_SchedulerCallback_:

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
				return hsm.TransitionOutput{}, fmt.Errorf("unrecognized callback result %v", result)
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

func isRetryableHTTPResponse(response *http.Response) bool {
	return response.StatusCode >= 500 || slices.Contains(retryable4xxErrorTypes, response.StatusCode)
}

func outcomeTag(callCtx context.Context, response *http.Response, callErr error) string {
	if callErr != nil {
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		return "unknown-error"
	}
	return fmt.Sprintf("status:%d", response.StatusCode)
}
