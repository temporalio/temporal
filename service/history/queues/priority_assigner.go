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

package queues

import (
	"sync"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/tasks"
)

type (
	// PriorityAssigner assigns priority to task executables
	PriorityAssigner interface {
		Assign(Executable) error
	}

	PriorityAssignerOptions struct {
		HighPriorityRPS       dynamicconfig.IntPropertyFnWithNamespaceFilter
		CriticalRetryAttempts dynamicconfig.IntPropertyFn
	}

	priorityAssignerImpl struct {
		currentClusterName string
		namespaceRegistry  namespace.Registry
		metricsProvider    metrics.MetricsHandler
		options            PriorityAssignerOptions

		sync.RWMutex
		rateLimiters map[string]quotas.RateLimiter
	}
)

func NewPriorityAssigner(
	currentClusterName string,
	namespaceRegistry namespace.Registry,
	options PriorityAssignerOptions,
	metricsProvider metrics.MetricsHandler,
) PriorityAssigner {
	return &priorityAssignerImpl{
		currentClusterName: currentClusterName,
		namespaceRegistry:  namespaceRegistry,
		metricsProvider:    metricsProvider.WithTags(metrics.OperationTag(OperationTaskPriorityAssigner)),
		options:            options,
		rateLimiters:       make(map[string]quotas.RateLimiter),
	}
}

func (a *priorityAssignerImpl) Assign(executable Executable) error {
	/*
		Summary:
		- High priority: active tasks from active queue processor and no-op tasks (currently ignoring overrides)
		- Default priority: throttled tasks and selected task types (e.g. delete history events)
		- Low priority: standby tasks and tasks keep retrying

		Only candidates for high priority will consume the token in the rate limiter for high priority tasks and
		potentially be throttled.
	*/

	if executable.Attempt() > a.options.CriticalRetryAttempts() {
		executable.SetPriority(tasks.PriorityLow)
		return nil
	}

	namespaceEntry, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(executable.GetNamespaceID()))
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceNotFound); ok {
			executable.SetPriority(tasks.PriorityLow)
			return nil
		}

		return err
	}

	namespaceName := namespaceEntry.Name().String()
	namespaceActive := namespaceEntry.ActiveInCluster(a.currentClusterName)
	// TODO: remove QueueType() and the special logic for assgining high priority to no-op tasks
	// after merging active/standby queue processor or performing task filtering before submitting
	// tasks to worker pool
	var taskActive bool
	switch executable.QueueType() {
	case QueueTypeActiveTransfer, QueueTypeActiveTimer:
		taskActive = true
	case QueueTypeStandbyTransfer, QueueTypeStandbyTimer:
		taskActive = false
	default:
		taskActive = namespaceActive
	}

	if !taskActive && !namespaceActive {
		// standby tasks
		executable.SetPriority(tasks.PriorityLow)
		return nil
	}

	if (taskActive && !namespaceActive) || (!taskActive && namespaceActive) {
		// no-op tasks, set to high priority to ack them as soon as possible
		// don't consume rps limit
		// ignoring overrides for some no-op standby tasks for now
		executable.SetPriority(tasks.PriorityHigh)
		return nil
	}

	// active tasks for active namespaces
	switch executable.GetType() {
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		// add more task types here if we believe it's ok to delay those tasks
		// and assign them the same priority as throttled tasks
		executable.SetPriority(tasks.PriorityMedium)
		return nil
	}

	ratelimiter := a.getOrCreateRateLimiter(executable.GetNamespaceID())
	if !ratelimiter.Allow() {
		executable.SetPriority(tasks.PriorityMedium)

		a.metricsProvider.Counter(TaskThrottledCounter).Record(
			1,
			metrics.NamespaceTag(namespaceName),
			metrics.TaskTypeTag(executable.GetType().String()),
		)

		return nil
	}

	executable.SetPriority(tasks.PriorityHigh)
	return nil
}

func (a *priorityAssignerImpl) getOrCreateRateLimiter(
	namespaceName string,
) quotas.RateLimiter {
	a.RLock()
	rateLimiter, ok := a.rateLimiters[namespaceName]
	a.RUnlock()
	if ok {
		return rateLimiter
	}

	newRateLimiter := quotas.NewDefaultIncomingRateLimiter(
		func() float64 { return float64(a.options.HighPriorityRPS(namespaceName)) },
	)

	a.Lock()
	defer a.Unlock()

	rateLimiter, ok = a.rateLimiters[namespaceName]
	if ok {
		return rateLimiter
	}
	a.rateLimiters[namespaceName] = newRateLimiter
	return newRateLimiter
}
