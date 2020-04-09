// Code generated by MockGen. DO NOT EDIT.
// Source: interface.go

// Package task is a generated GoMock package.
package task

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockProcessor is a mock of Processor interface.
type MockProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockProcessorMockRecorder
}

// MockProcessorMockRecorder is the mock recorder for MockProcessor.
type MockProcessorMockRecorder struct {
	mock *MockProcessor
}

// NewMockProcessor creates a new mock instance.
func NewMockProcessor(ctrl *gomock.Controller) *MockProcessor {
	mock := &MockProcessor{ctrl: ctrl}
	mock.recorder = &MockProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProcessor) EXPECT() *MockProcessorMockRecorder {
	return m.recorder
}

// Start mocks base method.
func (m *MockProcessor) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockProcessorMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockProcessor)(nil).Start))
}

// Stop mocks base method.
func (m *MockProcessor) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockProcessorMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockProcessor)(nil).Stop))
}

// Submit mocks base method.
func (m *MockProcessor) Submit(task Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Submit", task)
	ret0, _ := ret[0].(error)
	return ret0
}

// Submit indicates an expected call of Submit.
func (mr *MockProcessorMockRecorder) Submit(task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockProcessor)(nil).Submit), task)
}

// MockScheduler is a mock of Scheduler interface.
type MockScheduler struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerMockRecorder
}

// MockSchedulerMockRecorder is the mock recorder for MockScheduler.
type MockSchedulerMockRecorder struct {
	mock *MockScheduler
}

// NewMockScheduler creates a new mock instance.
func NewMockScheduler(ctrl *gomock.Controller) *MockScheduler {
	mock := &MockScheduler{ctrl: ctrl}
	mock.recorder = &MockSchedulerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockScheduler) EXPECT() *MockSchedulerMockRecorder {
	return m.recorder
}

// Start mocks base method.
func (m *MockScheduler) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockSchedulerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockScheduler)(nil).Start))
}

// Stop mocks base method.
func (m *MockScheduler) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockSchedulerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockScheduler)(nil).Stop))
}

// Submit mocks base method.
func (m *MockScheduler) Submit(task PriorityTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Submit", task)
	ret0, _ := ret[0].(error)
	return ret0
}

// Submit indicates an expected call of Submit.
func (mr *MockSchedulerMockRecorder) Submit(task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockScheduler)(nil).Submit), task)
}

// TrySubmit mocks base method.
func (m *MockScheduler) TrySubmit(task PriorityTask) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TrySubmit", task)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TrySubmit indicates an expected call of TrySubmit.
func (mr *MockSchedulerMockRecorder) TrySubmit(task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TrySubmit", reflect.TypeOf((*MockScheduler)(nil).TrySubmit), task)
}

// MockTask is a mock of Task interface.
type MockTask struct {
	ctrl     *gomock.Controller
	recorder *MockTaskMockRecorder
}

// MockTaskMockRecorder is the mock recorder for MockTask.
type MockTaskMockRecorder struct {
	mock *MockTask
}

// NewMockTask creates a new mock instance.
func NewMockTask(ctrl *gomock.Controller) *MockTask {
	mock := &MockTask{ctrl: ctrl}
	mock.recorder = &MockTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTask) EXPECT() *MockTaskMockRecorder {
	return m.recorder
}

// Execute mocks base method.
func (m *MockTask) Execute() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute")
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute.
func (mr *MockTaskMockRecorder) Execute() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockTask)(nil).Execute))
}

// HandleErr mocks base method.
func (m *MockTask) HandleErr(err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleErr", err)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleErr indicates an expected call of HandleErr.
func (mr *MockTaskMockRecorder) HandleErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleErr", reflect.TypeOf((*MockTask)(nil).HandleErr), err)
}

// RetryErr mocks base method.
func (m *MockTask) RetryErr(err error) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetryErr", err)
	ret0, _ := ret[0].(bool)
	return ret0
}

// RetryErr indicates an expected call of RetryErr.
func (mr *MockTaskMockRecorder) RetryErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryErr", reflect.TypeOf((*MockTask)(nil).RetryErr), err)
}

// Ack mocks base method.
func (m *MockTask) Ack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Ack")
}

// Ack indicates an expected call of Ack.
func (mr *MockTaskMockRecorder) Ack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockTask)(nil).Ack))
}

// Nack mocks base method.
func (m *MockTask) Nack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Nack")
}

// Nack indicates an expected call of Nack.
func (mr *MockTaskMockRecorder) Nack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nack", reflect.TypeOf((*MockTask)(nil).Nack))
}

// State mocks base method.
func (m *MockTask) State() State {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "State")
	ret0, _ := ret[0].(State)
	return ret0
}

// State indicates an expected call of State.
func (mr *MockTaskMockRecorder) State() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "State", reflect.TypeOf((*MockTask)(nil).State))
}

// MockPriorityTask is a mock of PriorityTask interface.
type MockPriorityTask struct {
	ctrl     *gomock.Controller
	recorder *MockPriorityTaskMockRecorder
}

// MockPriorityTaskMockRecorder is the mock recorder for MockPriorityTask.
type MockPriorityTaskMockRecorder struct {
	mock *MockPriorityTask
}

// NewMockPriorityTask creates a new mock instance.
func NewMockPriorityTask(ctrl *gomock.Controller) *MockPriorityTask {
	mock := &MockPriorityTask{ctrl: ctrl}
	mock.recorder = &MockPriorityTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPriorityTask) EXPECT() *MockPriorityTaskMockRecorder {
	return m.recorder
}

// Execute mocks base method.
func (m *MockPriorityTask) Execute() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute")
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute.
func (mr *MockPriorityTaskMockRecorder) Execute() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockPriorityTask)(nil).Execute))
}

// HandleErr mocks base method.
func (m *MockPriorityTask) HandleErr(err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleErr", err)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleErr indicates an expected call of HandleErr.
func (mr *MockPriorityTaskMockRecorder) HandleErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleErr", reflect.TypeOf((*MockPriorityTask)(nil).HandleErr), err)
}

// RetryErr mocks base method.
func (m *MockPriorityTask) RetryErr(err error) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetryErr", err)
	ret0, _ := ret[0].(bool)
	return ret0
}

// RetryErr indicates an expected call of RetryErr.
func (mr *MockPriorityTaskMockRecorder) RetryErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryErr", reflect.TypeOf((*MockPriorityTask)(nil).RetryErr), err)
}

// Ack mocks base method.
func (m *MockPriorityTask) Ack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Ack")
}

// Ack indicates an expected call of Ack.
func (mr *MockPriorityTaskMockRecorder) Ack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockPriorityTask)(nil).Ack))
}

// Nack mocks base method.
func (m *MockPriorityTask) Nack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Nack")
}

// Nack indicates an expected call of Nack.
func (mr *MockPriorityTaskMockRecorder) Nack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nack", reflect.TypeOf((*MockPriorityTask)(nil).Nack))
}

// State mocks base method.
func (m *MockPriorityTask) State() State {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "State")
	ret0, _ := ret[0].(State)
	return ret0
}

// State indicates an expected call of State.
func (mr *MockPriorityTaskMockRecorder) State() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "State", reflect.TypeOf((*MockPriorityTask)(nil).State))
}

// Priority mocks base method.
func (m *MockPriorityTask) Priority() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Priority")
	ret0, _ := ret[0].(int)
	return ret0
}

// Priority indicates an expected call of Priority.
func (mr *MockPriorityTaskMockRecorder) Priority() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Priority", reflect.TypeOf((*MockPriorityTask)(nil).Priority))
}

// SetPriority mocks base method.
func (m *MockPriorityTask) SetPriority(arg0 int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetPriority", arg0)
}

// SetPriority indicates an expected call of SetPriority.
func (mr *MockPriorityTaskMockRecorder) SetPriority(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetPriority", reflect.TypeOf((*MockPriorityTask)(nil).SetPriority), arg0)
}

// MockSequentialTaskQueue is a mock of SequentialTaskQueue interface.
type MockSequentialTaskQueue struct {
	ctrl     *gomock.Controller
	recorder *MockSequentialTaskQueueMockRecorder
}

// MockSequentialTaskQueueMockRecorder is the mock recorder for MockSequentialTaskQueue.
type MockSequentialTaskQueueMockRecorder struct {
	mock *MockSequentialTaskQueue
}

// NewMockSequentialTaskQueue creates a new mock instance.
func NewMockSequentialTaskQueue(ctrl *gomock.Controller) *MockSequentialTaskQueue {
	mock := &MockSequentialTaskQueue{ctrl: ctrl}
	mock.recorder = &MockSequentialTaskQueueMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSequentialTaskQueue) EXPECT() *MockSequentialTaskQueueMockRecorder {
	return m.recorder
}

// QueueID mocks base method.
func (m *MockSequentialTaskQueue) QueueID() interface{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueueID")
	ret0, _ := ret[0].(interface{})
	return ret0
}

// QueueID indicates an expected call of QueueID.
func (mr *MockSequentialTaskQueueMockRecorder) QueueID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueueID", reflect.TypeOf((*MockSequentialTaskQueue)(nil).QueueID))
}

// Add mocks base method.
func (m *MockSequentialTaskQueue) Add(task Task) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Add", task)
}

// Add indicates an expected call of Add.
func (mr *MockSequentialTaskQueueMockRecorder) Add(task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockSequentialTaskQueue)(nil).Add), task)
}

// Remove mocks base method.
func (m *MockSequentialTaskQueue) Remove() Task {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Remove")
	ret0, _ := ret[0].(Task)
	return ret0
}

// Remove indicates an expected call of Remove.
func (mr *MockSequentialTaskQueueMockRecorder) Remove() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Remove", reflect.TypeOf((*MockSequentialTaskQueue)(nil).Remove))
}

// IsEmpty mocks base method.
func (m *MockSequentialTaskQueue) IsEmpty() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsEmpty")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsEmpty indicates an expected call of IsEmpty.
func (mr *MockSequentialTaskQueueMockRecorder) IsEmpty() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsEmpty", reflect.TypeOf((*MockSequentialTaskQueue)(nil).IsEmpty))
}

// Len mocks base method.
func (m *MockSequentialTaskQueue) Len() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Len")
	ret0, _ := ret[0].(int)
	return ret0
}

// Len indicates an expected call of Len.
func (mr *MockSequentialTaskQueueMockRecorder) Len() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Len", reflect.TypeOf((*MockSequentialTaskQueue)(nil).Len))
}
