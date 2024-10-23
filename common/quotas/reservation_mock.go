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

// Code generated by MockGen. DO NOT EDIT.
// Source: reservation.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package quotas -source reservation.go -destination reservation_mock.go
//

// Package quotas is a generated GoMock package.
package quotas

import (
	reflect "reflect"
	time "time"

	gomock "go.uber.org/mock/gomock"
)

// MockReservation is a mock of Reservation interface.
type MockReservation struct {
	ctrl     *gomock.Controller
	recorder *MockReservationMockRecorder
}

// MockReservationMockRecorder is the mock recorder for MockReservation.
type MockReservationMockRecorder struct {
	mock *MockReservation
}

// NewMockReservation creates a new mock instance.
func NewMockReservation(ctrl *gomock.Controller) *MockReservation {
	mock := &MockReservation{ctrl: ctrl}
	mock.recorder = &MockReservationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReservation) EXPECT() *MockReservationMockRecorder {
	return m.recorder
}

// Cancel mocks base method.
func (m *MockReservation) Cancel() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Cancel")
}

// Cancel indicates an expected call of Cancel.
func (mr *MockReservationMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockReservation)(nil).Cancel))
}

// CancelAt mocks base method.
func (m *MockReservation) CancelAt(now time.Time) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CancelAt", now)
}

// CancelAt indicates an expected call of CancelAt.
func (mr *MockReservationMockRecorder) CancelAt(now any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelAt", reflect.TypeOf((*MockReservation)(nil).CancelAt), now)
}

// Delay mocks base method.
func (m *MockReservation) Delay() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delay")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// Delay indicates an expected call of Delay.
func (mr *MockReservationMockRecorder) Delay() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delay", reflect.TypeOf((*MockReservation)(nil).Delay))
}

// DelayFrom mocks base method.
func (m *MockReservation) DelayFrom(now time.Time) time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DelayFrom", now)
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// DelayFrom indicates an expected call of DelayFrom.
func (mr *MockReservationMockRecorder) DelayFrom(now any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DelayFrom", reflect.TypeOf((*MockReservation)(nil).DelayFrom), now)
}

// OK mocks base method.
func (m *MockReservation) OK() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OK")
	ret0, _ := ret[0].(bool)
	return ret0
}

// OK indicates an expected call of OK.
func (mr *MockReservationMockRecorder) OK() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OK", reflect.TypeOf((*MockReservation)(nil).OK))
}
