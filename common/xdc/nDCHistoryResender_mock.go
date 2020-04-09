// Code generated by MockGen. DO NOT EDIT.
// Source: nDCHistoryResender.go

// Package xdc is a generated GoMock package.
package xdc

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockNDCHistoryResender is a mock of NDCHistoryResender interface.
type MockNDCHistoryResender struct {
	ctrl     *gomock.Controller
	recorder *MockNDCHistoryResenderMockRecorder
}

// MockNDCHistoryResenderMockRecorder is the mock recorder for MockNDCHistoryResender.
type MockNDCHistoryResenderMockRecorder struct {
	mock *MockNDCHistoryResender
}

// NewMockNDCHistoryResender creates a new mock instance.
func NewMockNDCHistoryResender(ctrl *gomock.Controller) *MockNDCHistoryResender {
	mock := &MockNDCHistoryResender{ctrl: ctrl}
	mock.recorder = &MockNDCHistoryResenderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNDCHistoryResender) EXPECT() *MockNDCHistoryResenderMockRecorder {
	return m.recorder
}

// SendSingleWorkflowHistory mocks base method.
func (m *MockNDCHistoryResender) SendSingleWorkflowHistory(namespaceID, workflowID, runID string, startEventID, startEventVersion, endEventID, endEventVersion int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendSingleWorkflowHistory", namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendSingleWorkflowHistory indicates an expected call of SendSingleWorkflowHistory.
func (mr *MockNDCHistoryResenderMockRecorder) SendSingleWorkflowHistory(namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendSingleWorkflowHistory", reflect.TypeOf((*MockNDCHistoryResender)(nil).SendSingleWorkflowHistory), namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion)
}
