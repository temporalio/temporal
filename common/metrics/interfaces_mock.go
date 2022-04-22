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
// Source: interfaces.go

// Package metrics is a generated GoMock package.
package metrics

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	log "go.temporal.io/server/common/log"
)

// MockStopwatch is a mock of Stopwatch interface.
type MockStopwatch struct {
	ctrl     *gomock.Controller
	recorder *MockStopwatchMockRecorder
}

// MockStopwatchMockRecorder is the mock recorder for MockStopwatch.
type MockStopwatchMockRecorder struct {
	mock *MockStopwatch
}

// NewMockStopwatch creates a new mock instance.
func NewMockStopwatch(ctrl *gomock.Controller) *MockStopwatch {
	mock := &MockStopwatch{ctrl: ctrl}
	mock.recorder = &MockStopwatchMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStopwatch) EXPECT() *MockStopwatchMockRecorder {
	return m.recorder
}

// Stop mocks base method.
func (m *MockStopwatch) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockStopwatchMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockStopwatch)(nil).Stop))
}

// Subtract mocks base method.
func (m *MockStopwatch) Subtract(d time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Subtract", d)
}

// Subtract indicates an expected call of Subtract.
func (mr *MockStopwatchMockRecorder) Subtract(d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subtract", reflect.TypeOf((*MockStopwatch)(nil).Subtract), d)
}

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// AddCounter mocks base method.
func (m *MockClient) AddCounter(scope, counter int, delta int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddCounter", scope, counter, delta)
}

// AddCounter indicates an expected call of AddCounter.
func (mr *MockClientMockRecorder) AddCounter(scope, counter, delta interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddCounter", reflect.TypeOf((*MockClient)(nil).AddCounter), scope, counter, delta)
}

// IncCounter mocks base method.
func (m *MockClient) IncCounter(scope, counter int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "IncCounter", scope, counter)
}

// IncCounter indicates an expected call of IncCounter.
func (mr *MockClientMockRecorder) IncCounter(scope, counter interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IncCounter", reflect.TypeOf((*MockClient)(nil).IncCounter), scope, counter)
}

// RecordDistribution mocks base method.
func (m *MockClient) RecordDistribution(scope, timer, d int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordDistribution", scope, timer, d)
}

// RecordDistribution indicates an expected call of RecordDistribution.
func (mr *MockClientMockRecorder) RecordDistribution(scope, timer, d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordDistribution", reflect.TypeOf((*MockClient)(nil).RecordDistribution), scope, timer, d)
}

// RecordTimer mocks base method.
func (m *MockClient) RecordTimer(scope, timer int, d time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordTimer", scope, timer, d)
}

// RecordTimer indicates an expected call of RecordTimer.
func (mr *MockClientMockRecorder) RecordTimer(scope, timer, d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordTimer", reflect.TypeOf((*MockClient)(nil).RecordTimer), scope, timer, d)
}

// Scope mocks base method.
func (m *MockClient) Scope(scope int, tags ...Tag) Scope {
	m.ctrl.T.Helper()
	varargs := []interface{}{scope}
	for _, a := range tags {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Scope", varargs...)
	ret0, _ := ret[0].(Scope)
	return ret0
}

// Scope indicates an expected call of Scope.
func (mr *MockClientMockRecorder) Scope(scope interface{}, tags ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{scope}, tags...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Scope", reflect.TypeOf((*MockClient)(nil).Scope), varargs...)
}

// StartTimer mocks base method.
func (m *MockClient) StartTimer(scope, timer int) Stopwatch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartTimer", scope, timer)
	ret0, _ := ret[0].(Stopwatch)
	return ret0
}

// StartTimer indicates an expected call of StartTimer.
func (mr *MockClientMockRecorder) StartTimer(scope, timer interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartTimer", reflect.TypeOf((*MockClient)(nil).StartTimer), scope, timer)
}

// UpdateGauge mocks base method.
func (m *MockClient) UpdateGauge(scope, gauge int, value float64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UpdateGauge", scope, gauge, value)
}

// UpdateGauge indicates an expected call of UpdateGauge.
func (mr *MockClientMockRecorder) UpdateGauge(scope, gauge, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateGauge", reflect.TypeOf((*MockClient)(nil).UpdateGauge), scope, gauge, value)
}

// UserScope mocks base method.
func (m *MockClient) UserScope() UserScope {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserScope")
	ret0, _ := ret[0].(UserScope)
	return ret0
}

// UserScope indicates an expected call of UserScope.
func (mr *MockClientMockRecorder) UserScope() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserScope", reflect.TypeOf((*MockClient)(nil).UserScope))
}

// MockScope is a mock of Scope interface.
type MockScope struct {
	ctrl     *gomock.Controller
	recorder *MockScopeMockRecorder
}

// MockScopeMockRecorder is the mock recorder for MockScope.
type MockScopeMockRecorder struct {
	mock *MockScope
}

// NewMockScope creates a new mock instance.
func NewMockScope(ctrl *gomock.Controller) *MockScope {
	mock := &MockScope{ctrl: ctrl}
	mock.recorder = &MockScopeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockScope) EXPECT() *MockScopeMockRecorder {
	return m.recorder
}

// AddCounter mocks base method.
func (m *MockScope) AddCounter(counter int, delta int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddCounter", counter, delta)
}

// AddCounter indicates an expected call of AddCounter.
func (mr *MockScopeMockRecorder) AddCounter(counter, delta interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddCounter", reflect.TypeOf((*MockScope)(nil).AddCounter), counter, delta)
}

// IncCounter mocks base method.
func (m *MockScope) IncCounter(counter int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "IncCounter", counter)
}

// IncCounter indicates an expected call of IncCounter.
func (mr *MockScopeMockRecorder) IncCounter(counter interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IncCounter", reflect.TypeOf((*MockScope)(nil).IncCounter), counter)
}

// RecordDistribution mocks base method.
func (m *MockScope) RecordDistribution(id, d int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordDistribution", id, d)
}

// RecordDistribution indicates an expected call of RecordDistribution.
func (mr *MockScopeMockRecorder) RecordDistribution(id, d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordDistribution", reflect.TypeOf((*MockScope)(nil).RecordDistribution), id, d)
}

// RecordTimer mocks base method.
func (m *MockScope) RecordTimer(timer int, d time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordTimer", timer, d)
}

// RecordTimer indicates an expected call of RecordTimer.
func (mr *MockScopeMockRecorder) RecordTimer(timer, d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordTimer", reflect.TypeOf((*MockScope)(nil).RecordTimer), timer, d)
}

// StartTimer mocks base method.
func (m *MockScope) StartTimer(timer int) Stopwatch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartTimer", timer)
	ret0, _ := ret[0].(Stopwatch)
	return ret0
}

// StartTimer indicates an expected call of StartTimer.
func (mr *MockScopeMockRecorder) StartTimer(timer interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartTimer", reflect.TypeOf((*MockScope)(nil).StartTimer), timer)
}

// Tagged mocks base method.
func (m *MockScope) Tagged(tags ...Tag) Scope {
	m.ctrl.T.Helper()
	varargs := []interface{}{}
	for _, a := range tags {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Tagged", varargs...)
	ret0, _ := ret[0].(Scope)
	return ret0
}

// Tagged indicates an expected call of Tagged.
func (mr *MockScopeMockRecorder) Tagged(tags ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tagged", reflect.TypeOf((*MockScope)(nil).Tagged), tags...)
}

// UpdateGauge mocks base method.
func (m *MockScope) UpdateGauge(gauge int, value float64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UpdateGauge", gauge, value)
}

// UpdateGauge indicates an expected call of UpdateGauge.
func (mr *MockScopeMockRecorder) UpdateGauge(gauge, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateGauge", reflect.TypeOf((*MockScope)(nil).UpdateGauge), gauge, value)
}

// MockUserScope is a mock of UserScope interface.
type MockUserScope struct {
	ctrl     *gomock.Controller
	recorder *MockUserScopeMockRecorder
}

// MockUserScopeMockRecorder is the mock recorder for MockUserScope.
type MockUserScopeMockRecorder struct {
	mock *MockUserScope
}

// NewMockUserScope creates a new mock instance.
func NewMockUserScope(ctrl *gomock.Controller) *MockUserScope {
	mock := &MockUserScope{ctrl: ctrl}
	mock.recorder = &MockUserScopeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockUserScope) EXPECT() *MockUserScopeMockRecorder {
	return m.recorder
}

// AddCounter mocks base method.
func (m *MockUserScope) AddCounter(counter string, delta int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddCounter", counter, delta)
}

// AddCounter indicates an expected call of AddCounter.
func (mr *MockUserScopeMockRecorder) AddCounter(counter, delta interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddCounter", reflect.TypeOf((*MockUserScope)(nil).AddCounter), counter, delta)
}

// IncCounter mocks base method.
func (m *MockUserScope) IncCounter(counter string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "IncCounter", counter)
}

// IncCounter indicates an expected call of IncCounter.
func (mr *MockUserScopeMockRecorder) IncCounter(counter interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IncCounter", reflect.TypeOf((*MockUserScope)(nil).IncCounter), counter)
}

// RecordDistribution mocks base method.
func (m *MockUserScope) RecordDistribution(id string, unit MetricUnit, d int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordDistribution", id, unit, d)
}

// RecordDistribution indicates an expected call of RecordDistribution.
func (mr *MockUserScopeMockRecorder) RecordDistribution(id, unit, d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordDistribution", reflect.TypeOf((*MockUserScope)(nil).RecordDistribution), id, unit, d)
}

// RecordTimer mocks base method.
func (m *MockUserScope) RecordTimer(timer string, d time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordTimer", timer, d)
}

// RecordTimer indicates an expected call of RecordTimer.
func (mr *MockUserScopeMockRecorder) RecordTimer(timer, d interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordTimer", reflect.TypeOf((*MockUserScope)(nil).RecordTimer), timer, d)
}

// StartTimer mocks base method.
func (m *MockUserScope) StartTimer(timer string) Stopwatch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartTimer", timer)
	ret0, _ := ret[0].(Stopwatch)
	return ret0
}

// StartTimer indicates an expected call of StartTimer.
func (mr *MockUserScopeMockRecorder) StartTimer(timer interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartTimer", reflect.TypeOf((*MockUserScope)(nil).StartTimer), timer)
}

// Tagged mocks base method.
func (m *MockUserScope) Tagged(tags map[string]string) UserScope {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tagged", tags)
	ret0, _ := ret[0].(UserScope)
	return ret0
}

// Tagged indicates an expected call of Tagged.
func (mr *MockUserScopeMockRecorder) Tagged(tags interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tagged", reflect.TypeOf((*MockUserScope)(nil).Tagged), tags)
}

// UpdateGauge mocks base method.
func (m *MockUserScope) UpdateGauge(gauge string, value float64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UpdateGauge", gauge, value)
}

// UpdateGauge indicates an expected call of UpdateGauge.
func (mr *MockUserScopeMockRecorder) UpdateGauge(gauge, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateGauge", reflect.TypeOf((*MockUserScope)(nil).UpdateGauge), gauge, value)
}

// MockReporter is a mock of Reporter interface.
type MockReporter struct {
	ctrl     *gomock.Controller
	recorder *MockReporterMockRecorder
}

// MockReporterMockRecorder is the mock recorder for MockReporter.
type MockReporterMockRecorder struct {
	mock *MockReporter
}

// NewMockReporter creates a new mock instance.
func NewMockReporter(ctrl *gomock.Controller) *MockReporter {
	mock := &MockReporter{ctrl: ctrl}
	mock.recorder = &MockReporterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReporter) EXPECT() *MockReporterMockRecorder {
	return m.recorder
}

// NewClient mocks base method.
func (m *MockReporter) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewClient", logger, serviceIdx)
	ret0, _ := ret[0].(Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewClient indicates an expected call of NewClient.
func (mr *MockReporterMockRecorder) NewClient(logger, serviceIdx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewClient", reflect.TypeOf((*MockReporter)(nil).NewClient), logger, serviceIdx)
}

// Stop mocks base method.
func (m *MockReporter) Stop(logger log.Logger) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop", logger)
}

// Stop indicates an expected call of Stop.
func (mr *MockReporterMockRecorder) Stop(logger interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockReporter)(nil).Stop), logger)
}

// UserScope mocks base method.
func (m *MockReporter) UserScope() UserScope {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserScope")
	ret0, _ := ret[0].(UserScope)
	return ret0
}

// UserScope indicates an expected call of UserScope.
func (mr *MockReporterMockRecorder) UserScope() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserScope", reflect.TypeOf((*MockReporter)(nil).UserScope))
}
