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

package log

import (
	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/common/log/tag"
)

// MockLogger returns a mock for Logger interface
type MockLogger struct {
	mock.Mock
}

// Debug provides a mock function with given fields: msg, tags
func (_m *MockLogger) Debug(msg string, tags ...tag.Tag) {
	_m.Called(msg, tags)
}

// Info provides a mock function with given fields: msg, tags
func (_m *MockLogger) Info(msg string, tags ...tag.Tag) {
	_m.Called(msg, tags)
}

// Warn provides a mock function with given fields: msg, tags
func (_m *MockLogger) Warn(msg string, tags ...tag.Tag) {
	_m.Called(msg, tags)
}

// Error provides a mock function with given fields: msg, tags
func (_m *MockLogger) Error(msg string, tags ...tag.Tag) {
	_m.Called(msg, tags)
}

// Fatal provides a mock function with given fields: msg, tags
func (_m *MockLogger) Fatal(msg string, tags ...tag.Tag) {
	_m.Called(msg, tags)
}

// WithTags provides a mock function with given fields: tags
func (_m *MockLogger) WithTags(tags ...tag.Tag) Logger {
	ret := _m.Called(tags)

	var r0 Logger
	if rf, ok := ret.Get(0).(func(...tag.Tag) Logger); ok {
		r0 = rf(tags...)
	} else {
		r0 = ret.Get(0).(Logger)
	}

	return r0
}
