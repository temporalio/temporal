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

package history

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ historyservice.HistoryServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          historyservice.HistoryServiceClient
	metricsHandler  metrics.MetricsHandler
	logger          log.Logger
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of historyservice.HistoryServiceClient that emits metrics
func NewMetricClient(
	client historyservice.HistoryServiceClient,
	metricsHandler metrics.MetricsHandler,
	logger log.Logger,
	throttledLogger log.Logger,
) historyservice.HistoryServiceClient {
	return &metricClient{
		client:          client,
		metricsHandler:  metricsHandler,
		logger:          logger,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) startMetricsRecording(
	ctx context.Context,
	operation string,
) (metrics.MetricsHandler, time.Time) {
	caller := headers.GetCallerInfo(ctx).CallerName
	handler := c.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller), metrics.ServiceRoleTag(metrics.HistoryRoleTagValue))
	handler.Counter(metrics.ClientRequests.GetMetricName()).Record(1)
	return handler, time.Now().UTC()
}

func (c *metricClient) finishMetricsRecording(
	handler metrics.MetricsHandler,
	startTime time.Time,
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
			c.throttledLogger.Info("history client encountered error", tag.Error(err), tag.ErrorType(err))
		}
		handler.Counter(metrics.ClientFailures.GetMetricName()).Record(1, metrics.ServiceErrorTypeTag(err))
	}
	handler.Timer(metrics.ClientLatency.GetMetricName()).Record(time.Since(startTime))
}
