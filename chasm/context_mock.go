package chasm

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ Context = (*MockContext)(nil)
var _ MutableContext = (*MockMutableContext)(nil)

// MockContext is a mock implementation of [Context].
type MockContext struct {
	HandleExecutionKey         func() ExecutionKey
	HandleNow                  func(component Component) time.Time
	HandleRef                  func(component Component) ([]byte, error)
	HandleExecutionCloseTime   func() time.Time
	HandleStateTransitionCount func() int64
	HandleMetricsHandler       func() metrics.Handler

	ctx context.Context
}

func (c *MockContext) goContext() context.Context {
	if c.ctx == nil {
		c.ctx = context.Background()
	}
	return c.ctx
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

func (c *MockContext) Logger() log.Logger {
	executionKey := c.ExecutionKey()
	return log.NewTestLogger().With(
		tag.WorkflowNamespaceID(executionKey.NamespaceID),
		tag.WorkflowID(executionKey.BusinessID),
		tag.WorkflowRunID(executionKey.RunID),
	)
}

func (c *MockContext) MetricsHandler() metrics.Handler {
	if c.HandleMetricsHandler != nil {
		return c.HandleMetricsHandler()
	}
	return metrics.NoopMetricsHandler
}

func (c *MockContext) Value(key any) any {
	return c.goContext().Value(key)
}

func (c *MockContext) withValue(key any, value any) Context {
	return &MockContext{
		HandleExecutionKey:         c.HandleExecutionKey,
		HandleNow:                  c.HandleNow,
		HandleRef:                  c.HandleRef,
		HandleExecutionCloseTime:   c.HandleExecutionCloseTime,
		HandleStateTransitionCount: c.HandleStateTransitionCount,
		HandleMetricsHandler:       c.HandleMetricsHandler,
		ctx:                        context.WithValue(c.goContext(), key, value),
	}
}

// MockMutableContext is a mock implementation of [MutableContext] that records added tasks for inspection in
// tests.
type MockMutableContext struct {
	MockContext

	mu    sync.Mutex
	Tasks []MockTask
}

func (c *MockMutableContext) AddTask(component Component, attributes TaskAttributes, payload any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Tasks = append(c.Tasks, MockTask{component, attributes, payload})
}

func (c *MockMutableContext) withValue(key any, value any) Context {
	return &MockMutableContext{
		MockContext: *ContextWithValue(&c.MockContext, key, value),
		Tasks:       slices.Clone(c.Tasks),
	}
}

type MockTask struct {
	Component  Component
	Attributes TaskAttributes
	Payload    any
}
