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
	"sync"

	enumspb "go.temporal.io/temporal-proto/enums/v1"

	"github.com/temporalio/temporal/common/metrics"

	"go.temporal.io/temporal-proto/serviceerror"
	tasklistpb "go.temporal.io/temporal-proto/tasklist/v1"
)

type handlerContext struct {
	context.Context
	scope metrics.Scope
}

var stickyTaskListMetricTag = metrics.TaskListTag("__sticky__")

func newHandlerContext(
	ctx context.Context,
	namespace string,
	taskList *tasklistpb.TaskList,
	metricsClient metrics.Client,
	metricsScope int,
) *handlerContext {
	return &handlerContext{
		Context: ctx,
		scope:   newPerTaskListScope(namespace, taskList.GetName(), taskList.GetKind(), metricsClient, metricsScope),
	}
}

func newPerTaskListScope(
	namespace string,
	taskListName string,
	taskListKind enumspb.TaskListKind,
	client metrics.Client,
	scopeIdx int,
) metrics.Scope {
	namespaceTag := metrics.NamespaceUnknownTag()
	taskListTag := metrics.TaskListUnknownTag()
	if namespace != "" {
		namespaceTag = metrics.NamespaceTag(namespace)
	}
	if taskListName != "" && taskListKind != enumspb.TASK_LIST_KIND_STICKY {
		taskListTag = metrics.TaskListTag(taskListName)
	}
	if taskListKind == enumspb.TASK_LIST_KIND_STICKY {
		taskListTag = stickyTaskListMetricTag
	}
	return client.Scope(scopeIdx, namespaceTag, taskListTag)
}

// startProfiling initiates recording of request metrics
func (reqCtx *handlerContext) startProfiling(wg *sync.WaitGroup) metrics.Stopwatch {
	wg.Wait()
	sw := reqCtx.scope.StartTimer(metrics.ServiceLatencyPerTaskList)
	reqCtx.scope.IncCounter(metrics.ServiceRequestsPerTaskList)
	return sw
}

func (reqCtx *handlerContext) handleErr(err error) error {
	if err == nil {
		return nil
	}

	scope := reqCtx.scope

	switch err.(type) {
	case *serviceerror.Internal:
		scope.IncCounter(metrics.ServiceFailuresPerTaskList)
		return err
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentPerTaskListCounter)
		return err
	case *serviceerror.NotFound:
		scope.IncCounter(metrics.ServiceErrNotFoundPerTaskListCounter)
		return err
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		scope.IncCounter(metrics.ServiceErrExecutionAlreadyStartedPerTaskListCounter)
		return err
	case *serviceerror.NamespaceAlreadyExists:
		scope.IncCounter(metrics.ServiceErrNamespaceAlreadyExistsPerTaskListCounter)
		return err
	case *serviceerror.QueryFailed:
		scope.IncCounter(metrics.ServiceErrQueryFailedPerTaskListCounter)
		return err
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.ServiceErrResourceExhaustedPerTaskListCounter)
		return err
	case *serviceerror.NamespaceNotActive:
		scope.IncCounter(metrics.ServiceErrNamespaceNotActivePerTaskListCounter)
		return err
	default:
		scope.IncCounter(metrics.ServiceFailuresPerTaskList)
		return serviceerror.NewInternal(err.Error())
	}
}
