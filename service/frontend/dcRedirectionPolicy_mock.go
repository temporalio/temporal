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

package frontend

import (
	"context"

	mock "github.com/stretchr/testify/mock"
)

// MockDCRedirectionPolicy is an autogenerated mock type for the DCRedirectionPolicy type
type MockDCRedirectionPolicy struct {
	mock.Mock
}

// WithNamespaceIDRedirect provides a mock function with given fields: namespaceID, apiName, call
func (_m *MockDCRedirectionPolicy) WithNamespaceIDRedirect(ctx context.Context, namespaceID string, apiName string, call func(string) error) error {
	ret := _m.Called(namespaceID, apiName, call)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, func(string) error) error); ok {
		r0 = rf(namespaceID, apiName, call)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WithNamespaceRedirect provides a mock function with given fields: namespace, apiName, call
func (_m *MockDCRedirectionPolicy) WithNamespaceRedirect(ctx context.Context, namespace string, apiName string, call func(string) error) error {
	ret := _m.Called(namespace, apiName, call)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, func(string) error) error); ok {
		r0 = rf(namespace, apiName, call)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
