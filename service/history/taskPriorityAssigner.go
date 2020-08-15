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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination taskPriorityAssigner_mock.go -self_package go.temporal.io/server/service/history

package history

import (
	"strconv"
	"sync"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type (
	taskPriorityAssigner interface {
		Assign(queueTask) error
	}

	taskPriorityAssignerImpl struct {
		sync.RWMutex

		currentClusterName string
		namespaceCache     cache.NamespaceCache
		config             *Config
		logger             log.Logger
		scope              metrics.Scope
		rateLimiters       map[string]quotas.Limiter
	}
)

var _ taskPriorityAssigner = (*taskPriorityAssignerImpl)(nil)

const (
	numBitsPerLevel = 3
)

const (
	taskHighPriorityClass = iota << numBitsPerLevel
	taskDefaultPriorityClass
	taskLowPriorityClass
)

const (
	taskHighPrioritySubclass = iota
	taskDefaultPrioritySubclass
	taskLowPrioritySubclass
)

var defaultTaskPriorityWeight = map[int]int{
	getTaskPriority(taskHighPriorityClass, taskDefaultPrioritySubclass):    200,
	getTaskPriority(taskDefaultPriorityClass, taskDefaultPrioritySubclass): 100,
	getTaskPriority(taskLowPriorityClass, taskDefaultPrioritySubclass):     50,
}

func newTaskPriorityAssigner(
	currentClusterName string,
	namespaceCache cache.NamespaceCache,
	logger log.Logger,
	metricClient metrics.Client,
	config *Config,
) *taskPriorityAssignerImpl {
	return &taskPriorityAssignerImpl{
		currentClusterName: currentClusterName,
		namespaceCache:     namespaceCache,
		config:             config,
		logger:             logger,
		scope:              metricClient.Scope(metrics.TaskPriorityAssignerScope),
		rateLimiters:       make(map[string]quotas.Limiter),
	}
}

func (a *taskPriorityAssignerImpl) Assign(
	task queueTask,
) error {
	if task.GetQueueType() == replicationQueueType {
		task.SetPriority(getTaskPriority(taskLowPriorityClass, taskDefaultPrioritySubclass))
		return nil
	}

	// timer of transfer task, first check if namespace is active or not
	namespace, active, err := a.getNamespaceInfo(task.GetNamespaceId())
	if err != nil {
		return err
	}

	if !active {
		task.SetPriority(getTaskPriority(taskLowPriorityClass, taskDefaultPrioritySubclass))
		return nil
	}

	if !a.getRateLimiter(namespace).Allow() {
		task.SetPriority(getTaskPriority(taskDefaultPriorityClass, taskDefaultPrioritySubclass))
		taggedScope := a.scope.Tagged(metrics.NamespaceTag(namespace))
		if task.GetQueueType() == transferQueueType {
			taggedScope.IncCounter(metrics.TransferTaskThrottledCounter)
		} else {
			taggedScope.IncCounter(metrics.TimerTaskThrottledCounter)
		}
		return nil
	}

	task.SetPriority(getTaskPriority(taskHighPriorityClass, taskDefaultPrioritySubclass))
	return nil
}

// getNamespaceInfo returns three pieces of information:
//  1. namespace name
//  2. if namespace is active
//  3. error, if any
func (a *taskPriorityAssignerImpl) getNamespaceInfo(
	namespaceID string,
) (string, bool, error) {
	namespaceEntry, err := a.namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			a.logger.Warn("Cannot find namespace", tag.WorkflowNamespaceID(namespaceID))
			return "", false, err
		}
		// it is possible that the namespace is deleted
		// we should treat that namespace as active
		a.logger.Warn("Cannot find namespace, treat as active task.", tag.WorkflowNamespaceID(namespaceID))
		return "", true, nil
	}

	if namespaceEntry.IsGlobalNamespace() && a.currentClusterName != namespaceEntry.GetReplicationConfig().ActiveClusterName {
		return namespaceEntry.GetInfo().Name, false, nil
	}
	return namespaceEntry.GetInfo().Name, true, nil
}

func (a *taskPriorityAssignerImpl) getRateLimiter(
	namespace string,
) quotas.Limiter {
	a.RLock()
	if limiter, ok := a.rateLimiters[namespace]; ok {
		a.RUnlock()
		return limiter
	}
	a.RUnlock()

	limiter := quotas.NewDynamicRateLimiter(
		func() float64 {
			return float64(a.config.TaskProcessRPS(namespace))
		},
	)

	a.Lock()
	defer a.Unlock()
	if existingLimiter, ok := a.rateLimiters[namespace]; ok {
		return existingLimiter
	}

	a.rateLimiters[namespace] = limiter
	return limiter
}

func getTaskPriority(
	class, subClass int,
) int {
	return class | subClass
}

func convertWeightsToDynamicConfigValue(
	weights map[int]int,
) map[string]interface{} {
	weightsForDC := make(map[string]interface{})
	for priority, weight := range weights {
		weightsForDC[strconv.Itoa(priority)] = weight
	}
	return weightsForDC
}
