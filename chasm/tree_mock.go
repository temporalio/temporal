// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
//

// Code generated by MockGen. DO NOT EDIT.
// Source: tree.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../LICENSE -package chasm -source tree.go -destination tree_mock.go
//

// Package chasm is a generated GoMock package.
package chasm

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockNodeBackend is a mock of NodeBackend interface.
type MockNodeBackend struct {
	ctrl     *gomock.Controller
	recorder *MockNodeBackendMockRecorder
}

// MockNodeBackendMockRecorder is the mock recorder for MockNodeBackend.
type MockNodeBackendMockRecorder struct {
	mock *MockNodeBackend
}

// NewMockNodeBackend creates a new mock instance.
func NewMockNodeBackend(ctrl *gomock.Controller) *MockNodeBackend {
	mock := &MockNodeBackend{ctrl: ctrl}
	mock.recorder = &MockNodeBackendMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNodeBackend) EXPECT() *MockNodeBackendMockRecorder {
	return m.recorder
}

// MockNodePathEncoder is a mock of NodePathEncoder interface.
type MockNodePathEncoder struct {
	ctrl     *gomock.Controller
	recorder *MockNodePathEncoderMockRecorder
}

// MockNodePathEncoderMockRecorder is the mock recorder for MockNodePathEncoder.
type MockNodePathEncoderMockRecorder struct {
	mock *MockNodePathEncoder
}

// NewMockNodePathEncoder creates a new mock instance.
func NewMockNodePathEncoder(ctrl *gomock.Controller) *MockNodePathEncoder {
	mock := &MockNodePathEncoder{ctrl: ctrl}
	mock.recorder = &MockNodePathEncoderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNodePathEncoder) EXPECT() *MockNodePathEncoderMockRecorder {
	return m.recorder
}

// Decode mocks base method.
func (m *MockNodePathEncoder) Decode(encodedPath string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decode", encodedPath)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Decode indicates an expected call of Decode.
func (mr *MockNodePathEncoderMockRecorder) Decode(encodedPath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decode", reflect.TypeOf((*MockNodePathEncoder)(nil).Decode), encodedPath)
}

// Encode mocks base method.
func (m *MockNodePathEncoder) Encode(node *Node, path []string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encode", node, path)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Encode indicates an expected call of Encode.
func (mr *MockNodePathEncoderMockRecorder) Encode(node, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encode", reflect.TypeOf((*MockNodePathEncoder)(nil).Encode), node, path)
}
