// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

// Code generated by MockGen. DO NOT EDIT.
// Source: nDCStateRebuilder.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	definition "github.com/temporalio/temporal/common/definition"
	reflect "reflect"
	time "time"
)

// MocknDCStateRebuilder is a mock of nDCStateRebuilder interface.
type MocknDCStateRebuilder struct {
	ctrl     *gomock.Controller
	recorder *MocknDCStateRebuilderMockRecorder
}

// MocknDCStateRebuilderMockRecorder is the mock recorder for MocknDCStateRebuilder.
type MocknDCStateRebuilderMockRecorder struct {
	mock *MocknDCStateRebuilder
}

// NewMocknDCStateRebuilder creates a new mock instance.
func NewMocknDCStateRebuilder(ctrl *gomock.Controller) *MocknDCStateRebuilder {
	mock := &MocknDCStateRebuilder{ctrl: ctrl}
	mock.recorder = &MocknDCStateRebuilderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocknDCStateRebuilder) EXPECT() *MocknDCStateRebuilderMockRecorder {
	return m.recorder
}

// rebuild mocks base method.
func (m *MocknDCStateRebuilder) rebuild(ctx context.Context, now time.Time, baseWorkflowIdentifier definition.WorkflowIdentifier, baseBranchToken []byte, baseLastEventID, baseLastEventVersion int64, targetWorkflowIdentifier definition.WorkflowIdentifier, targetBranchToken []byte, requestID string) (mutableState, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "rebuild", ctx, now, baseWorkflowIdentifier, baseBranchToken, baseLastEventID, baseLastEventVersion, targetWorkflowIdentifier, targetBranchToken, requestID)
	ret0, _ := ret[0].(mutableState)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// rebuild indicates an expected call of rebuild.
func (mr *MocknDCStateRebuilderMockRecorder) rebuild(ctx, now, baseWorkflowIdentifier, baseBranchToken, baseLastEventID, baseLastEventVersion, targetWorkflowIdentifier, targetBranchToken, requestID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "rebuild", reflect.TypeOf((*MocknDCStateRebuilder)(nil).rebuild), ctx, now, baseWorkflowIdentifier, baseBranchToken, baseLastEventID, baseLastEventVersion, targetWorkflowIdentifier, targetBranchToken, requestID)
}
