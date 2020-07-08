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

package mocks

import (
	mock "github.com/stretchr/testify/mock"

	persistence "go.temporal.io/server/common/persistence"
)

// ShardManager is an autogenerated mock type for the ShardManager type
type ShardManager struct {
	mock.Mock
}

// GetName provides a mock function with given fields:
func (_m *ShardManager) GetName() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *ShardManager) Close() {
	_m.Called()
}

// CreateShard provides a mock function with given fields: request
func (_m *ShardManager) CreateShard(request *persistence.CreateShardRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.CreateShardRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetShard provides a mock function with given fields: request
func (_m *ShardManager) GetShard(request *persistence.GetShardRequest) (*persistence.GetShardResponse, error) {
	ret := _m.Called(request)

	var r0 *persistence.GetShardResponse
	if rf, ok := ret.Get(0).(func(*persistence.GetShardRequest) *persistence.GetShardResponse); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*persistence.GetShardResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*persistence.GetShardRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateShard provides a mock function with given fields: request
func (_m *ShardManager) UpdateShard(request *persistence.UpdateShardRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*persistence.UpdateShardRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

var _ persistence.ShardManager = (*ShardManager)(nil)
