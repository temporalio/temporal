package chasm

import (
	"context"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/tasks"
)

// MockNodeBackend is a lightweight manual mock for the NodeBackend interface.
// Methods may be stubbed by assigning the corresponding Handle fields. Update call history is recorded in the struct
// fields (thread-safe).
type MockNodeBackend struct {
	// Optional function overrides. If nil, methods return zero-values.
	HandleGetExecutionState          func() *persistencespb.WorkflowExecutionState
	HandleGetExecutionInfo           func() *persistencespb.WorkflowExecutionInfo
	HandleGetCurrentVersion          func() int64
	HandleNextTransitionCount        func() int64
	HandleCurrentVersionedTransition func() *persistencespb.VersionedTransition
	HandleGetWorkflowKey             func() definition.WorkflowKey
	HandleUpdateWorkflowStateStatus  func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error)
	HandleIsWorkflow                 func() bool
	HandleGetNexusCompletion         func(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error)

	// Recorded calls (protected by mu).
	mu                  sync.Mutex
	TasksByCategory     map[tasks.Category][]tasks.Task
	DeletePureTaskCalls []time.Time
	UpdateCalls         []struct {
		State  enumsspb.WorkflowExecutionState
		Status enumspb.WorkflowExecutionStatus
	}
}

func (m *MockNodeBackend) GetExecutionState() *persistencespb.WorkflowExecutionState {
	if m.HandleGetExecutionState != nil {
		return m.HandleGetExecutionState()
	}
	return nil
}

func (m *MockNodeBackend) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	if m.HandleGetExecutionInfo != nil {
		return m.HandleGetExecutionInfo()
	}
	return nil
}

func (m *MockNodeBackend) GetCurrentVersion() int64 {
	if m.HandleGetCurrentVersion != nil {
		return m.HandleGetCurrentVersion()
	}
	return 0
}

func (m *MockNodeBackend) NextTransitionCount() int64 {
	if m.HandleNextTransitionCount != nil {
		return m.HandleNextTransitionCount()
	}
	return 0
}

func (m *MockNodeBackend) CurrentVersionedTransition() *persistencespb.VersionedTransition {
	if m.HandleCurrentVersionedTransition != nil {
		return m.HandleCurrentVersionedTransition()
	}
	return nil
}

func (m *MockNodeBackend) GetWorkflowKey() definition.WorkflowKey {
	if m.HandleGetWorkflowKey != nil {
		return m.HandleGetWorkflowKey()
	}
	return definition.WorkflowKey{}
}

func (m *MockNodeBackend) AddTasks(ts ...tasks.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.TasksByCategory == nil {
		m.TasksByCategory = make(map[tasks.Category][]tasks.Task, 1)
	}
	for _, task := range ts {
		category := task.GetCategory()
		m.TasksByCategory[category] = append(m.TasksByCategory[category], task)
	}
}

func (m *MockNodeBackend) DeleteCHASMPureTasks(maxScheduledTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeletePureTaskCalls = append(m.DeletePureTaskCalls, maxScheduledTime)
}

func (m *MockNodeBackend) LastDeletePureTaskCall() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.DeletePureTaskCalls) == 0 {
		return time.Time{}
	}
	return m.DeletePureTaskCalls[len(m.DeletePureTaskCalls)-1]
}

func (m *MockNodeBackend) UpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) (bool, error) {
	if m.HandleUpdateWorkflowStateStatus != nil {
		ok, err := m.HandleUpdateWorkflowStateStatus(state, status)

		m.mu.Lock()
		m.UpdateCalls = append(m.UpdateCalls, struct {
			State  enumsspb.WorkflowExecutionState
			Status enumspb.WorkflowExecutionStatus
		}{State: state, Status: status})
		m.mu.Unlock()

		return ok, err
	}

	m.mu.Lock()
	m.UpdateCalls = append(m.UpdateCalls, struct {
		State  enumsspb.WorkflowExecutionState
		Status enumspb.WorkflowExecutionStatus
	}{State: state, Status: status})
	m.mu.Unlock()

	return false, nil
}

func (m *MockNodeBackend) LastUpdateWorkflowState() enumsspb.WorkflowExecutionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.UpdateCalls) == 0 {
		return enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED
	}
	return m.UpdateCalls[len(m.UpdateCalls)-1].State
}

func (m *MockNodeBackend) LastUpdateWorkflowStatus() enumspb.WorkflowExecutionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.UpdateCalls) == 0 {
		return enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
	}
	return m.UpdateCalls[len(m.UpdateCalls)-1].Status
}

func (m *MockNodeBackend) IsWorkflow() bool {
	if m.HandleIsWorkflow != nil {
		return m.HandleIsWorkflow()
	}
	return false
}

func (m *MockNodeBackend) GetNexusCompletion(
	ctx context.Context,
	requestID string,
) (nexusrpc.OperationCompletion, error) {
	if m.HandleGetNexusCompletion != nil {
		return m.HandleGetNexusCompletion(ctx, requestID)
	}
	return nil, nil
}

func (m *MockNodeBackend) NumTasksAdded() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, ts := range m.TasksByCategory {
		count += len(ts)
	}
	return count
}
