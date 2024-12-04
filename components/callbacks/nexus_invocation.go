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
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/queues"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type CanGetNexusCompletion interface {
	GetNexusCompletion(ctx context.Context) (nexus.OperationCompletion, error)
}

type nexusInvocation struct {
	nexus      *persistencepb.Callback_Nexus
	completion nexus.OperationCompletion
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

func (n nexusInvocation) WrapError(result invocationResult, err error) error {
	if failure, ok := result.(invocationResultRetry); ok {
		return queues.NewDestinationDownError(failure.err.Error(), err)
	}
	return err
}

func (n nexusInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	request, err := nexus.NewCompletionHTTPRequest(ctx, n.nexus.Url, n.completion)
	if err != nil {
		return invocationResultFail{queues.NewUnprocessableTaskError(
			fmt.Sprintf("failed to construct Nexus request: %v", err),
		)}
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	for k, v := range n.nexus.Header {
		request.Header.Set(k, v)
	}

	caller := e.HTTPCallerProvider(queues.NamespaceIDAndDestination{
		NamespaceID: ns.ID().String(),
		Destination: task.Destination(),
	})
	// Make the call and record metrics.
	startTime := time.Now()
	response, err := caller(request)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(task.Destination())
	statusCodeTag := metrics.OutcomeTag(outcomeTag(ctx, response, err))
	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if err == nil {
		// Body is not read but should be discarded to keep the underlying TCP connection alive.
		// Just in case something unexpected happens while discarding or closing the body,
		// propagate errors to the machine.
		if _, err = io.Copy(io.Discard, response.Body); err == nil {
			err = response.Body.Close()
		}
	}

	if err != nil {
		e.Logger.Error("Callback request failed with error", tag.Error(err))
		return invocationResultRetry{err}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return invocationResultOK{}
	}

	retryable := isRetryableHTTPResponse(response)
	err = fmt.Errorf("request failed with: %v", response.Status)
	e.Logger.Error("Callback request failed", tag.Error(err), tag.NewStringTag("status", response.Status), tag.NewBoolTag("retryable", retryable))
	if retryable {
		return invocationResultRetry{err}
	}
	return invocationResultFail{err}
}
