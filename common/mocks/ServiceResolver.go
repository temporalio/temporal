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

import mock "github.com/stretchr/testify/mock"
import membership "github.com/uber/cadence/common/membership"

// ServiceResolver is an mock implementation
type ServiceResolver struct {
	mock.Mock
}

// Lookup is am mock implementation
func (_m *ServiceResolver) Lookup(key string) (*membership.HostInfo, error) {
	ret := _m.Called(key)

	var r0 *membership.HostInfo
	if rf, ok := ret.Get(0).(func(string) *membership.HostInfo); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*membership.HostInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddListener is am mock implementation
func (_m *ServiceResolver) AddListener(name string, notifyChannel chan<- *membership.ChangedEvent) error {
	ret := _m.Called(name, notifyChannel)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, chan<- *membership.ChangedEvent) error); ok {
		r0 = rf(name, notifyChannel)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveListener is am mock implementation
func (_m *ServiceResolver) RemoveListener(name string) error {
	ret := _m.Called(name)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(name)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MemberCount is am mock implementation
func (_m *ServiceResolver) MemberCount() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// Members is am mock implementation
func (_m *ServiceResolver) Members() []*membership.HostInfo {
	ret := _m.Called()

	var r0 []*membership.HostInfo
	if rf, ok := ret.Get(0).(func() []*membership.HostInfo); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).([]*membership.HostInfo)
	}

	return r0
}

var _ membership.ServiceResolver = (*ServiceResolver)(nil)
