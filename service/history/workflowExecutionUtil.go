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

package history

import (
	"go.temporal.io/server/service/history/workflow"
)

type workflowContext interface {
	getContext() workflow.Context
	getMutableState() workflow.MutableState
	reloadMutableState() (workflow.MutableState, error)
	getReleaseFn() workflow.ReleaseCacheFunc
	getWorkflowID() string
	getRunID() string
}

type workflowContextImpl struct {
	context      workflow.Context
	mutableState workflow.MutableState
	releaseFn    workflow.ReleaseCacheFunc
}

type updateWorkflowAction struct {
	noop               bool
	createWorkflowTask bool
}

var (
	updateWorkflowWithNewWorkflowTask = &updateWorkflowAction{
		createWorkflowTask: true,
	}
	updateWorkflowWithoutWorkflowTask = &updateWorkflowAction{
		createWorkflowTask: false,
	}
)

type updateWorkflowActionFunc func(workflow.Context, workflow.MutableState) (*updateWorkflowAction, error)

func (w *workflowContextImpl) getContext() workflow.Context {
	return w.context
}

func (w *workflowContextImpl) getMutableState() workflow.MutableState {
	return w.mutableState
}

func (w *workflowContextImpl) reloadMutableState() (workflow.MutableState, error) {
	mutableState, err := w.getContext().LoadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	w.mutableState = mutableState
	return mutableState, nil
}

func (w *workflowContextImpl) getReleaseFn() workflow.ReleaseCacheFunc {
	return w.releaseFn
}

func (w *workflowContextImpl) getWorkflowID() string {
	return w.getContext().GetExecution().GetWorkflowId()
}

func (w *workflowContextImpl) getRunID() string {
	return w.getContext().GetExecution().GetRunId()
}

func newWorkflowContext(
	context workflow.Context,
	releaseFn workflow.ReleaseCacheFunc,
	mutableState workflow.MutableState,
) *workflowContextImpl {

	return &workflowContextImpl{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}
