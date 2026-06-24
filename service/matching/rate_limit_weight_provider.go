package matching

import (
	"go.temporal.io/server/common/namespace"
)

// TaskQueueRateLimitWeightProvider returns the weight applied to the
// rate limit for a task queue. The returned weight is always clamped to [0.0, 1.0].
type TaskQueueRateLimitWeightProvider interface {
	GetRateLimitWeight(nsName namespace.Name, tqName string) float64
}

const (
	minRateLimitWeight     = 0.0
	maxRateLimitWeight     = 1.0
	defaultRateLimitWeight = maxRateLimitWeight
)

// TaskQueueRateLimitWeightProviderFunc is a function that implements TaskQueueRateLimitWeightProvider.
type TaskQueueRateLimitWeightProviderFunc func(nsName namespace.Name, tqName string) float64

func (f TaskQueueRateLimitWeightProviderFunc) GetRateLimitWeight(nsName namespace.Name, tqName string) float64 {
	return f(nsName, tqName)
}

// NewTaskQueueRateLimitWeightProvider wraps inner and enforces [0.0, 1.0] on every call.
func NewTaskQueueRateLimitWeightProvider(inner TaskQueueRateLimitWeightProvider) TaskQueueRateLimitWeightProvider {
	return TaskQueueRateLimitWeightProviderFunc(func(nsName namespace.Name, tqName string) float64 {
		return max(min(inner.GetRateLimitWeight(nsName, tqName), maxRateLimitWeight), minRateLimitWeight)
	})
}

type unitRateLimitWeightProvider struct{}

func (p *unitRateLimitWeightProvider) GetRateLimitWeight(_ namespace.Name, _ string) float64 {
	return defaultRateLimitWeight
}

func taskQueueRateLimitWeightProviderProvider() TaskQueueRateLimitWeightProvider {
	return NewTaskQueueRateLimitWeightProvider(&unitRateLimitWeightProvider{})
}
