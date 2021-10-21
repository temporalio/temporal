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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

type handlerContext struct {
	context.Context
	scope  metrics.Scope
	logger log.Logger
}

func newHandlerContext(
	ctx context.Context,
	namespace namespace.Name,
	taskQueue *taskqueuepb.TaskQueue,
	metricsClient metrics.Client,
	metricsScope int,
	logger log.Logger,
) *handlerContext {
	return &handlerContext{
		Context: ctx,
		scope: metrics.GetPerTaskQueueScope(
			metricsClient.Scope(metricsScope),
			namespace.String(),
			taskQueue.GetName(),
			taskQueue.GetKind(),
		),
		logger: logger,
	}
}
