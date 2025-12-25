package chasm

import (
	"context"
	"sync"
	"time"
)

// MockContext is a mock implementation of [Context].
type MockContext struct {
	HandleExecutionKey       func() ExecutionKey
	HandleNow                func(component Component) time.Time
	HandleRef                func(component Component) ([]byte, error)
	HandleExecutionCloseTime func() time.Time
}

func (c *MockContext) getContext() context.Context {
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

type MockTask struct {
	Component  Component
	Attributes TaskAttributes
	Payload    any
}
