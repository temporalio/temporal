package chasm

import (
	"context"
	"sync"

	"go.temporal.io/server/common/metrics"
)

// MockNodePureTask is a lightweight manual mock for the NodePureTask interface.
// Methods may be stubbed by assigning the corresponding Handle fields. Call history
// is recorded in the struct fields (thread-safe).
type MockNodePureTask struct {
	HandleExecutePureTask  func(baseCtx context.Context, metricsHandler metrics.Handler, taskAttributes TaskAttributes, taskInstance any) (bool, error)
	HandleValidatePureTask func(baseCtx context.Context, taskAttributes TaskAttributes, taskInstance any) (bool, error)

	mu           sync.Mutex
	ExecuteCalls []struct {
		BaseCtx        context.Context
		MetricsHandler metrics.Handler
		Attributes     TaskAttributes
		Task           any
	}
	ValidateCalls []struct {
		BaseCtx    context.Context
		Attributes TaskAttributes
		Task       any
	}
}

func (m *MockNodePureTask) ExecutePureTask(
	baseCtx context.Context,
	metricsHandler metrics.Handler,
	taskAttributes TaskAttributes,
	taskInstance any,
) (bool, error) {
	if m.HandleExecutePureTask != nil {
		ok, err := m.HandleExecutePureTask(baseCtx, metricsHandler, taskAttributes, taskInstance)

		m.mu.Lock()
		m.ExecuteCalls = append(m.ExecuteCalls, struct {
			BaseCtx        context.Context
			MetricsHandler metrics.Handler
			Attributes     TaskAttributes
			Task           any
		}{BaseCtx: baseCtx, MetricsHandler: metricsHandler, Attributes: taskAttributes, Task: taskInstance})
		m.mu.Unlock()

		return ok, err
	}

	m.mu.Lock()
	m.ExecuteCalls = append(m.ExecuteCalls, struct {
		BaseCtx        context.Context
		MetricsHandler metrics.Handler
		Attributes     TaskAttributes
		Task           any
	}{BaseCtx: baseCtx, MetricsHandler: metricsHandler, Attributes: taskAttributes, Task: taskInstance})
	m.mu.Unlock()

	return false, nil
}

func (m *MockNodePureTask) ValidatePureTask(
	baseCtx context.Context,
	taskAttributes TaskAttributes,
	taskInstance any,
) (bool, error) {
	if m.HandleValidatePureTask != nil {
		ok, err := m.HandleValidatePureTask(baseCtx, taskAttributes, taskInstance)

		m.mu.Lock()
		m.ValidateCalls = append(m.ValidateCalls, struct {
			BaseCtx    context.Context
			Attributes TaskAttributes
			Task       any
		}{BaseCtx: baseCtx, Attributes: taskAttributes, Task: taskInstance})
		m.mu.Unlock()

		return ok, err
	}

	m.mu.Lock()
	m.ValidateCalls = append(m.ValidateCalls, struct {
		BaseCtx    context.Context
		Attributes TaskAttributes
		Task       any
	}{BaseCtx: baseCtx, Attributes: taskAttributes, Task: taskInstance})
	m.mu.Unlock()

	return false, nil
}
