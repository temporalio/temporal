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
	"github.com/uber/cadence/service/history/execution"
)

type workflowContext interface {
	getContext() execution.Context
	getMutableState() execution.MutableState
	reloadMutableState() (execution.MutableState, error)
	getReleaseFn() execution.ReleaseFunc
	getWorkflowID() string
	getRunID() string
}

type workflowContextImpl struct {
	context      execution.Context
	mutableState execution.MutableState
	releaseFn    execution.ReleaseFunc
}

type updateWorkflowAction struct {
	noop           bool
	createDecision bool
}

var (
	updateWorkflowWithNewDecision = &updateWorkflowAction{
		createDecision: true,
	}
	updateWorkflowWithoutDecision = &updateWorkflowAction{
		createDecision: false,
	}
)

type updateWorkflowActionFunc func(execution.Context, execution.MutableState) (*updateWorkflowAction, error)

func (w *workflowContextImpl) getContext() execution.Context {
	return w.context
}

func (w *workflowContextImpl) getMutableState() execution.MutableState {
	return w.mutableState
}

func (w *workflowContextImpl) reloadMutableState() (execution.MutableState, error) {
	mutableState, err := w.getContext().LoadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	w.mutableState = mutableState
	return mutableState, nil
}

func (w *workflowContextImpl) getReleaseFn() execution.ReleaseFunc {
	return w.releaseFn
}

func (w *workflowContextImpl) getWorkflowID() string {
	return w.getContext().GetExecution().GetWorkflowId()
}

func (w *workflowContextImpl) getRunID() string {
	return w.getContext().GetExecution().GetRunId()
}

func newWorkflowContext(
	context execution.Context,
	releaseFn execution.ReleaseFunc,
	mutableState execution.MutableState,
) workflowContext {

	return &workflowContextImpl{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}
