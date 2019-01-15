// Copyright (c) 2017 Uber Technologies, Inc.
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

import "github.com/stretchr/testify/mock"

import "context"

import h "github.com/uber/cadence/.gen/go/history"
import workflow "github.com/uber/cadence/.gen/go/shared"

type mockWorkflowResetor struct {
	mock.Mock
}

var _ workflowResetor = (*mockWorkflowResetor)(nil)

// ResetWorkflowExecution provides a mock function with given fields: ctx, resetRequest
func (_m *mockWorkflowResetor) ResetWorkflowExecution(ctx context.Context, resetRequest *h.ResetWorkflowExecutionRequest) (*workflow.ResetWorkflowExecutionResponse, error) {
	ret := _m.Called(ctx, resetRequest)

	var r0 *workflow.ResetWorkflowExecutionResponse
	if rf, ok := ret.Get(0).(func(context.Context, *h.ResetWorkflowExecutionRequest) *workflow.ResetWorkflowExecutionResponse); ok {
		r0 = rf(ctx, resetRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*workflow.ResetWorkflowExecutionResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *h.ResetWorkflowExecutionRequest) error); ok {
		r1 = rf(ctx, resetRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ApplyResetEvent provides a mock function with given fields: ctx, request, domainID, workflowID, currentRunID
func (_m *mockWorkflowResetor) ApplyResetEvent(ctx context.Context, request *h.ReplicateEventsRequest, domainID string, workflowID string, currentRunID string) error {
	ret := _m.Called(ctx, request, domainID, workflowID, currentRunID)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *h.ReplicateEventsRequest, string, string, string) error); ok {
		r0 = rf(ctx, request, domainID, workflowID, currentRunID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
