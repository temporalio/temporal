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
	historyi "go.temporal.io/server/service/history/interfaces"
)

type WorkflowLease interface {
	GetContext() historyi.WorkflowContext
	GetMutableState() historyi.MutableState
	GetReleaseFn() historyi.ReleaseWorkflowContextFunc
}

type workflowLease struct {
	context      historyi.WorkflowContext
	mutableState historyi.MutableState
	releaseFn    historyi.ReleaseWorkflowContextFunc
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
	wfContext historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	mutableState historyi.MutableState,
) WorkflowLease {
	return &workflowLease{
		context:      wfContext,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func (w *workflowLease) GetContext() historyi.WorkflowContext {
	return w.context
}

func (w *workflowLease) GetMutableState() historyi.MutableState {
	return w.mutableState
}

func (w *workflowLease) GetReleaseFn() historyi.ReleaseWorkflowContextFunc {
	return w.releaseFn
}
