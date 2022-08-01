// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package admin

import (
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ adminservice.AdminServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          adminservice.AdminServiceClient
	metricsClient   metrics.Client
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of adminservice.AdminServiceClient that emits metrics
func NewMetricClient(
	client adminservice.AdminServiceClient,
	metricsClient metrics.Client,
	throttledLogger log.Logger,
) adminservice.AdminServiceClient {
	return &metricClient{
		client:          client,
		metricsClient:   metricsClient,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) startMetricsRecording(
	metricScope int,
) (metrics.Scope, metrics.Stopwatch) {
	scope := c.metricsClient.Scope(metricScope)
	scope.IncCounter(metrics.ClientRequests)
	stopwatch := scope.StartTimer(metrics.ClientLatency)
	return scope, stopwatch
}

func (c *metricClient) finishMetricsRecording(
	scope metrics.Scope,
	stopwatch metrics.Stopwatch,
	err error,
) {
	if err != nil {
		switch err.(type) {
		case *serviceerror.Canceled,
			*serviceerror.DeadlineExceeded,
			*serviceerror.NotFound,
			*serviceerror.QueryFailed,
			*serviceerror.NamespaceNotFound,
			*serviceerror.WorkflowNotReady,
			*serviceerror.WorkflowExecutionAlreadyStarted:
			// noop - not interest and too many logs
		default:
			c.throttledLogger.Info("admin client encountered error", tag.Error(err), tag.ErrorType(err))
		}
		scope.Tagged(metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.ClientFailures)
	}
	stopwatch.Stop()
}
