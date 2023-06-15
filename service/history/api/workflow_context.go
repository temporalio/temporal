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

package api

import (
	"context"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
)

type WorkflowContext interface {
	GetContext() workflow.Context
	GetMutableState() workflow.MutableState
	GetReleaseFn() wcache.ReleaseCacheFunc

	GetNamespaceEntry() *namespace.Namespace
	GetWorkflowKey() definition.WorkflowKey
	GetUpdateRegistry(context.Context) update.Registry
}

type WorkflowContextImpl struct {
	context      workflow.Context
	mutableState workflow.MutableState
	releaseFn    wcache.ReleaseCacheFunc
}

type UpdateWorkflowAction struct {
	Noop               bool
	CreateWorkflowTask bool
}

var (
	UpdateWorkflowWithNewWorkflowTask = &UpdateWorkflowAction{
		CreateWorkflowTask: true,
	}
	UpdateWorkflowWithoutWorkflowTask = &UpdateWorkflowAction{
		CreateWorkflowTask: false,
	}
)

type UpdateWorkflowActionFunc func(WorkflowContext) (*UpdateWorkflowAction, error)

var _ WorkflowContext = (*WorkflowContextImpl)(nil)

func NewWorkflowContext(
	context workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	mutableState workflow.MutableState,
) *WorkflowContextImpl {

	return &WorkflowContextImpl{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func (w *WorkflowContextImpl) GetContext() workflow.Context {
	return w.context
}

func (w *WorkflowContextImpl) GetMutableState() workflow.MutableState {
	return w.mutableState
}

func (w *WorkflowContextImpl) GetReleaseFn() wcache.ReleaseCacheFunc {
	return w.releaseFn
}

func (w *WorkflowContextImpl) GetNamespaceEntry() *namespace.Namespace {
	return w.mutableState.GetNamespaceEntry()
}

func (w *WorkflowContextImpl) GetWorkflowKey() definition.WorkflowKey {
	return w.context.GetWorkflowKey()
}

func (w *WorkflowContextImpl) GetUpdateRegistry(ctx context.Context) update.Registry {
	return w.context.UpdateRegistry(ctx)
}
