package chasm

import (
	"context"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
)

// MockContext is a mock implementation of [Context].
type MockContext struct {
	HandleExecutionKey         func() ExecutionKey
	HandleNow                  func(component Component) time.Time
	HandleRef                  func(component Component) ([]byte, error)
	HandleExecutionCloseTime   func() time.Time
	HandleStateTransitionCount func() int64
	HandleLibrary              func(name string) (Library, bool)
	HandleGetNamespaceEntry    func() *namespace.Namespace
}

func (c *MockContext) GetContext() context.Context {
	return nil
}

func (c *MockContext) Now(cmp Component) time.Time {
	if c.HandleNow != nil {
		return c.HandleNow(cmp)
	}
	return time.Now()
}

func (c *MockContext) Ref(cmp Component) ([]byte, error) {
	if c.HandleRef != nil {
		return c.HandleRef(cmp)
	}
	return nil, nil
}

func (c *MockContext) structuredRef(cmp Component) (ComponentRef, error) {
	return ComponentRef{}, nil
}

func (c *MockContext) ExecutionKey() ExecutionKey {
	if c.HandleExecutionKey != nil {
		return c.HandleExecutionKey()
	}
	return ExecutionKey{}
}

func (c *MockContext) ExecutionCloseTime() time.Time {
	if c.HandleExecutionCloseTime != nil {
		return c.HandleExecutionCloseTime()
	}
	return time.Time{}
}

func (c *MockContext) StateTransitionCount() int64 {
	if c.HandleStateTransitionCount != nil {
		return c.HandleStateTransitionCount()
	}
	return 0
}

func (c *MockContext) GetNamespaceEntry() *namespace.Namespace {
	if c.HandleGetNamespaceEntry != nil {
		return c.HandleGetNamespaceEntry()
	}
	return nil
}

func (c *MockContext) Logger() log.Logger {
	executionKey := c.ExecutionKey()
	return log.NewTestLogger().With(
		tag.WorkflowNamespaceID(executionKey.NamespaceID),
		tag.WorkflowID(executionKey.BusinessID),
		tag.WorkflowRunID(executionKey.RunID),
	)
}

// MockMutableContext is a mock implementation of [MutableContext] that records added tasks for inspection in
// tests.
type MockMutableContext struct {
	MockContext

	HandleAddHistoryEvent func(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent

	mu    sync.Mutex
	Tasks []MockTask
}

func (c *MockMutableContext) AddTask(component Component, attributes TaskAttributes, payload any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Tasks = append(c.Tasks, MockTask{component, attributes, payload})
}

func (c *MockMutableContext) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	if c.HandleAddHistoryEvent != nil {
		return c.HandleAddHistoryEvent(t, setAttributes)
	}
	return nil
}

type MockTask struct {
	Component  Component
	Attributes TaskAttributes
	Payload    any
}
