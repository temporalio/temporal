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
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type WorkflowLease interface {
	GetContext() workflow.Context
	GetMutableState() workflow.MutableState
	GetReleaseFn() wcache.ReleaseCacheFunc
}

type workflowLease struct {
	context      workflow.Context
	mutableState workflow.MutableState
	releaseFn    wcache.ReleaseCacheFunc
}

type UpdateWorkflowAction struct {
	Noop               bool
	CreateWorkflowTask bool
	// Abort all "Workflow Updates" (not persistence updates) after persistence operation is succeeded but WF lock is not released.
	AbortUpdates bool
}

var (
	UpdateWorkflowWithNewWorkflowTask = &UpdateWorkflowAction{
		CreateWorkflowTask: true,
	}
	UpdateWorkflowWithoutWorkflowTask = &UpdateWorkflowAction{
		CreateWorkflowTask: false,
	}
	UpdateWorkflowTerminate = &UpdateWorkflowAction{
		CreateWorkflowTask: false,
		AbortUpdates:       true,
	}
)

type UpdateWorkflowActionFunc func(WorkflowLease) (*UpdateWorkflowAction, error)

var _ WorkflowLease = (*workflowLease)(nil)

func NewWorkflowLease(
	context workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	mutableState workflow.MutableState,
) WorkflowLease {
	return &workflowLease{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func (w *workflowLease) GetContext() workflow.Context {
	return w.context
}

func (w *workflowLease) GetMutableState() workflow.MutableState {
	return w.mutableState
}

func (w *workflowLease) GetReleaseFn() wcache.ReleaseCacheFunc {
	return w.releaseFn
}
