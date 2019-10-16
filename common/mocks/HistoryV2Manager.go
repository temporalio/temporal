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

package mocks

import "github.com/uber/cadence/common/persistence"
import "github.com/stretchr/testify/mock"

// HistoryManager mock implementation
type HistoryV2Manager struct {
	mock.Mock
}

// GetName provides a mock function with given fields:
func (_m *HistoryV2Manager) GetName() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// AppendHistoryNodes provides a mock function with given fields: request
func (_m *HistoryV2Manager) AppendHistoryNodes(request *persistence.AppendHistoryNodesRequest) (*persistence.AppendHistoryNodesResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.AppendHistoryNodesResponse
	if rf, ok := ret.Get(0).(func(*persistence.AppendHistoryNodesRequest) *persistence.AppendHistoryNodesResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.AppendHistoryNodesResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.AppendHistoryNodesRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// ReadHistoryBranch provides a mock function with given fields: request
func (_m *HistoryV2Manager) ReadHistoryBranch(request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.ReadHistoryBranchResponse
	if rf, ok := ret.Get(0).(func(*persistence.ReadHistoryBranchRequest) *persistence.ReadHistoryBranchResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.ReadHistoryBranchResponse)
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.ReadHistoryBranchRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// ReadHistoryBranchByBatch provides a mock function with given fields: request
func (_m *HistoryV2Manager) ReadHistoryBranchByBatch(request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchByBatchResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.ReadHistoryBranchByBatchResponse
	if rf, ok := ret.Get(0).(func(*persistence.ReadHistoryBranchRequest) *persistence.ReadHistoryBranchByBatchResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.ReadHistoryBranchByBatchResponse)
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.ReadHistoryBranchRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// ReadRawHistoryBranch provides a mock function with given fields: request
func (_m *HistoryV2Manager) ReadRawHistoryBranch(request *persistence.ReadHistoryBranchRequest) (*persistence.ReadRawHistoryBranchResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.ReadRawHistoryBranchResponse
	if rf, ok := ret.Get(0).(func(*persistence.ReadHistoryBranchRequest) *persistence.ReadRawHistoryBranchResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.ReadRawHistoryBranchResponse)
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.ReadHistoryBranchRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// ForkHistoryBranch provides a mock function with given fields: request
func (_m *HistoryV2Manager) ForkHistoryBranch(request *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.ForkHistoryBranchResponse
	if rf, ok := ret.Get(0).(func(*persistence.ForkHistoryBranchRequest) *persistence.ForkHistoryBranchResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.ForkHistoryBranchResponse)
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.ForkHistoryBranchRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// DeleteHistoryBranch provides a mock function with given fields: request
func (_m *HistoryV2Manager) DeleteHistoryBranch(request *persistence.DeleteHistoryBranchRequest) error {
	ret := _m.Called(request)
	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.DeleteHistoryBranchRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// CompleteForkBranch provides a mock function with given fields: request
func (_m *HistoryV2Manager) CompleteForkBranch(request *persistence.CompleteForkBranchRequest) error {
	ret := _m.Called(request)
	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.CompleteForkBranchRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// GetHistoryTree provides a mock function with given fields: request
func (_m *HistoryV2Manager) GetHistoryTree(request *persistence.GetHistoryTreeRequest) (*persistence.GetHistoryTreeResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.GetHistoryTreeResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetHistoryTreeRequest) *persistence.GetHistoryTreeResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetHistoryTreeResponse)
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetHistoryTreeRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

func (_m *HistoryV2Manager) GetAllHistoryTreeBranches(request *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
	ret := _m.Called(request)
	var r0 *persistence.GetAllHistoryTreeBranchesResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetAllHistoryTreeBranchesRequest) *persistence.GetAllHistoryTreeBranchesResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetAllHistoryTreeBranchesResponse)
		}
	}
	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetAllHistoryTreeBranchesRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// Close provides a mock function with given fields:
func (_m *HistoryV2Manager) Close() {
	_m.Called()
}

var _ persistence.HistoryManager = (*HistoryV2Manager)(nil)
