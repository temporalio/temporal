package log

import (
	"github.com/stretchr/testify/mock"

	"github.com/temporalio/temporal/common/log/tag"
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
