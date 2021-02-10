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

package matching

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

type handlerContext struct {
	context.Context
	scope  metrics.Scope
	logger log.Logger
}

var stickyTaskQueueMetricTag = metrics.TaskQueueTag("__sticky__")

func newHandlerContext(
	ctx context.Context,
	namespace string,
	taskQueue *taskqueuepb.TaskQueue,
	metricsClient metrics.Client,
	metricsScope int,
	logger log.Logger,
) *handlerContext {
	return &handlerContext{
		Context: ctx,
		scope: newPerTaskQueueScope(
			namespace,
			taskQueue.GetName(),
			taskQueue.GetKind(),
			metricsClient,
			metricsScope,
		),
		logger: logger,
	}
}

func newPerTaskQueueScope(
	namespace string,
	taskQueueName string,
	taskQueueKind enumspb.TaskQueueKind,
	client metrics.Client,
	scopeIdx int,
) metrics.Scope {
	namespaceTag := metrics.NamespaceUnknownTag()
	taskQueueTag := metrics.TaskQueueUnknownTag()
	if namespace != "" {
		namespaceTag = metrics.NamespaceTag(namespace)
	}
	if taskQueueName != "" && taskQueueKind != enumspb.TASK_QUEUE_KIND_STICKY {
		taskQueueTag = metrics.TaskQueueTag(taskQueueName)
	}
	if taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		taskQueueTag = stickyTaskQueueMetricTag
	}
	return client.Scope(scopeIdx, namespaceTag, taskQueueTag)
}
