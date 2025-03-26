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
// Source: namespace_replication_queue.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package persistence -source namespace_replication_queue.go -destination namespace_replication_queue_mock.go
//

// Package persistence is a generated GoMock package.
package persistence

import (
	context "context"
	reflect "reflect"

	repication "go.temporal.io/server/api/replication/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockNamespaceReplicationQueue is a mock of NamespaceReplicationQueue interface.
type MockNamespaceReplicationQueue struct {
	ctrl     *gomock.Controller
	recorder *MockNamespaceReplicationQueueMockRecorder
	isgomock struct{}
}

// MockNamespaceReplicationQueueMockRecorder is the mock recorder for MockNamespaceReplicationQueue.
type MockNamespaceReplicationQueueMockRecorder struct {
	mock *MockNamespaceReplicationQueue
}

// NewMockNamespaceReplicationQueue creates a new mock instance.
func NewMockNamespaceReplicationQueue(ctrl *gomock.Controller) *MockNamespaceReplicationQueue {
	mock := &MockNamespaceReplicationQueue{ctrl: ctrl}
	mock.recorder = &MockNamespaceReplicationQueueMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNamespaceReplicationQueue) EXPECT() *MockNamespaceReplicationQueueMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockNamespaceReplicationQueue) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockNamespaceReplicationQueueMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).Close))
}

// DeleteMessageFromDLQ mocks base method.
func (m *MockNamespaceReplicationQueue) DeleteMessageFromDLQ(ctx context.Context, messageID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteMessageFromDLQ", ctx, messageID)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteMessageFromDLQ indicates an expected call of DeleteMessageFromDLQ.
func (mr *MockNamespaceReplicationQueueMockRecorder) DeleteMessageFromDLQ(ctx, messageID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteMessageFromDLQ", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).DeleteMessageFromDLQ), ctx, messageID)
}

// DeleteMessagesBefore mocks base method.
func (m *MockNamespaceReplicationQueue) DeleteMessagesBefore(ctx context.Context, exclusiveMessageID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteMessagesBefore", ctx, exclusiveMessageID)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteMessagesBefore indicates an expected call of DeleteMessagesBefore.
func (mr *MockNamespaceReplicationQueueMockRecorder) DeleteMessagesBefore(ctx, exclusiveMessageID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteMessagesBefore", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).DeleteMessagesBefore), ctx, exclusiveMessageID)
}

// GetAckLevels mocks base method.
func (m *MockNamespaceReplicationQueue) GetAckLevels(ctx context.Context) (map[string]int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAckLevels", ctx)
	ret0, _ := ret[0].(map[string]int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAckLevels indicates an expected call of GetAckLevels.
func (mr *MockNamespaceReplicationQueueMockRecorder) GetAckLevels(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAckLevels", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).GetAckLevels), ctx)
}

// GetDLQAckLevel mocks base method.
func (m *MockNamespaceReplicationQueue) GetDLQAckLevel(ctx context.Context) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDLQAckLevel", ctx)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDLQAckLevel indicates an expected call of GetDLQAckLevel.
func (mr *MockNamespaceReplicationQueueMockRecorder) GetDLQAckLevel(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDLQAckLevel", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).GetDLQAckLevel), ctx)
}

// GetMessagesFromDLQ mocks base method.
func (m *MockNamespaceReplicationQueue) GetMessagesFromDLQ(ctx context.Context, firstMessageID, lastMessageID int64, pageSize int, pageToken []byte) ([]*repication.ReplicationTask, []byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMessagesFromDLQ", ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	ret0, _ := ret[0].([]*repication.ReplicationTask)
	ret1, _ := ret[1].([]byte)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetMessagesFromDLQ indicates an expected call of GetMessagesFromDLQ.
func (mr *MockNamespaceReplicationQueueMockRecorder) GetMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMessagesFromDLQ", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).GetMessagesFromDLQ), ctx, firstMessageID, lastMessageID, pageSize, pageToken)
}

// GetReplicationMessages mocks base method.
func (m *MockNamespaceReplicationQueue) GetReplicationMessages(ctx context.Context, lastMessageID int64, maxCount int) ([]*repication.ReplicationTask, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReplicationMessages", ctx, lastMessageID, maxCount)
	ret0, _ := ret[0].([]*repication.ReplicationTask)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetReplicationMessages indicates an expected call of GetReplicationMessages.
func (mr *MockNamespaceReplicationQueueMockRecorder) GetReplicationMessages(ctx, lastMessageID, maxCount any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReplicationMessages", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).GetReplicationMessages), ctx, lastMessageID, maxCount)
}

// Publish mocks base method.
func (m *MockNamespaceReplicationQueue) Publish(ctx context.Context, task *repication.ReplicationTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Publish", ctx, task)
	ret0, _ := ret[0].(error)
	return ret0
}

// Publish indicates an expected call of Publish.
func (mr *MockNamespaceReplicationQueueMockRecorder) Publish(ctx, task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).Publish), ctx, task)
}

// PublishToDLQ mocks base method.
func (m *MockNamespaceReplicationQueue) PublishToDLQ(ctx context.Context, task *repication.ReplicationTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PublishToDLQ", ctx, task)
	ret0, _ := ret[0].(error)
	return ret0
}

// PublishToDLQ indicates an expected call of PublishToDLQ.
func (mr *MockNamespaceReplicationQueueMockRecorder) PublishToDLQ(ctx, task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PublishToDLQ", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).PublishToDLQ), ctx, task)
}

// RangeDeleteMessagesFromDLQ mocks base method.
func (m *MockNamespaceReplicationQueue) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID, lastMessageID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RangeDeleteMessagesFromDLQ", ctx, firstMessageID, lastMessageID)
	ret0, _ := ret[0].(error)
	return ret0
}

// RangeDeleteMessagesFromDLQ indicates an expected call of RangeDeleteMessagesFromDLQ.
func (mr *MockNamespaceReplicationQueueMockRecorder) RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RangeDeleteMessagesFromDLQ", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).RangeDeleteMessagesFromDLQ), ctx, firstMessageID, lastMessageID)
}

// UpdateAckLevel mocks base method.
func (m *MockNamespaceReplicationQueue) UpdateAckLevel(ctx context.Context, lastProcessedMessageID int64, clusterName string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateAckLevel", ctx, lastProcessedMessageID, clusterName)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateAckLevel indicates an expected call of UpdateAckLevel.
func (mr *MockNamespaceReplicationQueueMockRecorder) UpdateAckLevel(ctx, lastProcessedMessageID, clusterName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateAckLevel", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).UpdateAckLevel), ctx, lastProcessedMessageID, clusterName)
}

// UpdateDLQAckLevel mocks base method.
func (m *MockNamespaceReplicationQueue) UpdateDLQAckLevel(ctx context.Context, lastProcessedMessageID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateDLQAckLevel", ctx, lastProcessedMessageID)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateDLQAckLevel indicates an expected call of UpdateDLQAckLevel.
func (mr *MockNamespaceReplicationQueueMockRecorder) UpdateDLQAckLevel(ctx, lastProcessedMessageID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateDLQAckLevel", reflect.TypeOf((*MockNamespaceReplicationQueue)(nil).UpdateDLQAckLevel), ctx, lastProcessedMessageID)
}
