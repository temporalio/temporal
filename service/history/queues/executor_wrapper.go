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
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
)

type (
	executorWrapper struct {
		currentClusterName string
		registry           namespace.Registry
		activeExecutor     Executor
		standbyExecutor    Executor
		logger             log.Logger
	}
)

func NewExecutorWrapper(
	currentClusterName string,
	registry namespace.Registry,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
) Executor {
	return &executorWrapper{
		currentClusterName: currentClusterName,
		registry:           registry,
		activeExecutor:     activeExecutor,
		standbyExecutor:    standbyExecutor,
		logger:             logger,
	}
}

func (e *executorWrapper) Execute(
	ctx context.Context,
	executable Executable,
) ([]metrics.Tag, bool, error) {

	namespaceID := executable.GetNamespaceID()
	entry, err := e.registry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		e.logger.Error("Unable to find namespace. Process task as active.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
		return e.activeExecutor.Execute(ctx, executable)
	}

	if !entry.IsOnCluster(e.currentClusterName) {
		e.logger.Debug("Dropping task as namespace is not on current cluster", tag.WorkflowNamespaceID(executable.GetNamespaceID()), tag.Value(executable.GetTask()))
		metricsTags := []metrics.Tag{
			metrics.NamespaceTag(entry.Name().String()),
			metrics.TaskTypeTag(executable.GetType().String()),
		}
		return metricsTags, false, consts.ErrTaskDiscarded
	}

	if e.isActiveTask(executable, entry) {
		return e.activeExecutor.Execute(ctx, executable)
	}

	return e.standbyExecutor.Execute(ctx, executable)
}

func (e *executorWrapper) isActiveTask(
	executable Executable,
	namespaceEntry *namespace.Namespace,
) bool {
	// Following is the existing task allocator logic for verifying active task
	namespaceID := namespaceEntry.ID().String()
	if !namespaceEntry.ActiveInCluster(e.currentClusterName) {
		e.logger.Debug("Process task as standby.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
		return false
	}

	e.logger.Debug("Process task as active.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
	return true
}
