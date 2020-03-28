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
// Source: queryParser.go

// Package filestore is a generated GoMock package.
package filestore

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockQueryParser is a mock of QueryParser interface.
type MockQueryParser struct {
	ctrl     *gomock.Controller
	recorder *MockQueryParserMockRecorder
}

// MockQueryParserMockRecorder is the mock recorder for MockQueryParser.
type MockQueryParserMockRecorder struct {
	mock *MockQueryParser
}

// NewMockQueryParser creates a new mock instance.
func NewMockQueryParser(ctrl *gomock.Controller) *MockQueryParser {
	mock := &MockQueryParser{ctrl: ctrl}
	mock.recorder = &MockQueryParserMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockQueryParser) EXPECT() *MockQueryParserMockRecorder {
	return m.recorder
}

// Parse mocks base method.
func (m *MockQueryParser) Parse(query string) (*parsedQuery, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Parse", query)
	ret0, _ := ret[0].(*parsedQuery)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Parse indicates an expected call of Parse.
func (mr *MockQueryParserMockRecorder) Parse(query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Parse", reflect.TypeOf((*MockQueryParser)(nil).Parse), query)
}
