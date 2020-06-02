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

	"github.com/uber/cadence/common/metrics"

	gen "github.com/uber/cadence/.gen/go/shared"
)

type handlerContext struct {
	context.Context
	scope metrics.Scope
}

var stickyTaskListMetricTag = metrics.TaskListTag("__sticky__")

func newHandlerContext(
	ctx context.Context,
	domain string,
	taskList *gen.TaskList,
	metricsClient metrics.Client,
	metricsScope int,
) *handlerContext {
	return &handlerContext{
		Context: ctx,
		scope:   newPerTaskListScope(domain, taskList.GetName(), taskList.GetKind(), metricsClient, metricsScope),
	}
}

func newPerTaskListScope(
	domain string,
	taskListName string,
	taskListKind gen.TaskListKind,
	client metrics.Client,
	scopeIdx int,
) metrics.Scope {
	domainTag := metrics.DomainUnknownTag()
	taskListTag := metrics.TaskListUnknownTag()
	if domain != "" {
		domainTag = metrics.DomainTag(domain)
	}
	if taskListName != "" && taskListKind != gen.TaskListKindSticky {
		taskListTag = metrics.TaskListTag(taskListName)
	}
	if taskListKind == gen.TaskListKindSticky {
		taskListTag = stickyTaskListMetricTag
	}
	return client.Scope(scopeIdx, domainTag, taskListTag)
}

// startProfiling initiates recording of request metrics
func (reqCtx *handlerContext) startProfiling(wg *sync.WaitGroup) metrics.Stopwatch {
	wg.Wait()
	sw := reqCtx.scope.StartTimer(metrics.CadenceLatencyPerTaskList)
	reqCtx.scope.IncCounter(metrics.CadenceRequestsPerTaskList)
	return sw
}

func (reqCtx *handlerContext) handleErr(err error) error {
	if err == nil {
		return nil
	}

	scope := reqCtx.scope

	switch err.(type) {
	case *gen.InternalServiceError:
		scope.IncCounter(metrics.CadenceFailuresPerTaskList)
		return err
	case *gen.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestPerTaskListCounter)
		return err
	case *gen.EntityNotExistsError:
		scope.IncCounter(metrics.CadenceErrEntityNotExistsPerTaskListCounter)
		return err
	case *gen.WorkflowExecutionAlreadyStartedError:
		scope.IncCounter(metrics.CadenceErrExecutionAlreadyStartedPerTaskListCounter)
		return err
	case *gen.DomainAlreadyExistsError:
		scope.IncCounter(metrics.CadenceErrDomainAlreadyExistsPerTaskListCounter)
		return err
	case *gen.QueryFailedError:
		scope.IncCounter(metrics.CadenceErrQueryFailedPerTaskListCounter)
		return err
	case *gen.LimitExceededError:
		scope.IncCounter(metrics.CadenceErrLimitExceededPerTaskListCounter)
		return err
	case *gen.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyPerTaskListCounter)
		return err
	case *gen.DomainNotActiveError:
		scope.IncCounter(metrics.CadenceErrDomainNotActivePerTaskListCounter)
		return err
	case *gen.RemoteSyncMatchedError:
		scope.IncCounter(metrics.CadenceErrRemoteSyncMatchFailedPerTaskListCounter)
		return err
	default:
		scope.IncCounter(metrics.CadenceFailuresPerTaskList)
		return &gen.InternalServiceError{Message: err.Error()}
	}
}
