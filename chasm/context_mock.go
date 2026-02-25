package chasm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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
	HandleExecutionInfo        func() ExecutionInfo
	HandleMetricsHandler       func() metrics.Handler
	HandleLibrary              func(name string) (Library, bool)
	HandleNamespaceEntry       func() *namespace.Namespace
	HandleEndpointByName       func(string) (*persistencespb.NexusEndpointEntry, error)
	HandleMetricsHandler       func() metrics.Handler

	// GoCtx is the underlying context.Context used for context value lookups.
	// Any values set on it will be available via the CHASM mock context's Value method,
	// and take precedence over any registered context values.
	// Defaults to context.Background() if nil.
	GoCtx context.Context

	registeredContextValues map[any]any
}

func (c *MockContext) RegisterComponentContextValues(
	keyValues map[any]any,
) {
	if c.registeredContextValues == nil {
		c.registeredContextValues = make(map[any]any)
	}
	for k, v := range keyValues {
		if _, exists := c.registeredContextValues[k]; exists {
			// nolint:forbidigo
			panic(fmt.Sprintf("context value key already registered: %v", k))
		}
		c.registeredContextValues[k] = v
	}
}

func (c *MockContext) goContext() context.Context {
	if c.GoCtx == nil {
		c.GoCtx = context.Background()
	}
	return c.GoCtx
}

func (c *MockContext) EndpointByName(name string) (*persistencespb.NexusEndpointEntry, error) {
	if c.HandleEndpointByName != nil {
		return c.HandleEndpointByName(name)
	}
	return nil, errors.New("endpoint registry not available")
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

func (c *MockContext) ExecutionInfo() ExecutionInfo {
	if c.HandleExecutionInfo != nil {
		return c.HandleExecutionInfo()
	}
	return ExecutionInfo{}
}

func (c *MockContext) NamespaceEntry() *namespace.Namespace {
	if c.HandleNamespaceEntry != nil {
		return c.HandleNamespaceEntry()
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
		HandleExecutionKey:   c.HandleExecutionKey,
		HandleNow:            c.HandleNow,
		HandleRef:            c.HandleRef,
		HandleExecutionInfo:  c.HandleExecutionInfo,
		HandleMetricsHandler: c.HandleMetricsHandler,
		GoCtx:                context.WithValue(c.goContext(), key, value),
		HandleNamespaceEntry: c.HandleNamespaceEntry,
		HandleEndpointByName: c.HandleEndpointByName,
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
