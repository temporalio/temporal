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

import (
	"github.com/stretchr/testify/mock"
	"github.com/uber/cadence/common/persistence"
)

// mockConflictResolver is used as mock implementation for conflictResolver
type mockConflictResolver struct {
	mock.Mock
}

var _ conflictResolver = (*mockConflictResolver)(nil)

// reset is mock implementation for reset of conflictResolver
func (_m *mockConflictResolver) reset(_a0 string, _a1 int64, _a2 int, _a3 string, _a4 int64, _a5 *persistence.WorkflowExecutionInfo, _a6 int64) (mutableState, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4, _a5, _a6)

	var r0 mutableState
	if rf, ok := ret.Get(0).(func(string, int64, int, string, int64, *persistence.WorkflowExecutionInfo, int64) mutableState); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4, _a5, _a6)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(mutableState)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int64, int, string, int64, *persistence.WorkflowExecutionInfo, int64) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3, _a4, _a5, _a6)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
