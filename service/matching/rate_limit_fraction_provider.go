package matching

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
)

// TaskQueueRateLimitFractionProvider returns the fraction of the rate limit for a task queue.
// The returned fraction is always clamped to [0.0, 1.0].
type TaskQueueRateLimitFractionProvider interface {
	GetRateLimitFraction(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) float64
}

const (
	minRateLimitFraction     = 0.0
	maxRateLimitFraction     = 1.0
	defaultRateLimitFraction = maxRateLimitFraction
)

// TaskQueueRateLimitFractionProviderFunc is a function that implements TaskQueueRateLimitFractionProvider.
type TaskQueueRateLimitFractionProviderFunc func(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) float64

func (f TaskQueueRateLimitFractionProviderFunc) GetRateLimitFraction(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) float64 {
	return f(nsName, tqName, tqType)
}

// NewTaskQueueRateLimitFractionProvider wraps inner and enforces [0.0, 1.0] on every call.
func NewTaskQueueRateLimitFractionProvider(inner TaskQueueRateLimitFractionProvider) TaskQueueRateLimitFractionProvider {
	return TaskQueueRateLimitFractionProviderFunc(func(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) float64 {
		return max(min(inner.GetRateLimitFraction(nsName, tqName, tqType), maxRateLimitFraction), minRateLimitFraction)
	})
}

type unitRateLimitFractionProvider struct{}

func (p *unitRateLimitFractionProvider) GetRateLimitFraction(_ namespace.Name, _ string, _ enumspb.TaskQueueType) float64 {
	return defaultRateLimitFraction
}

var defaultTaskQueueRateLimitFractionProvider = NewTaskQueueRateLimitFractionProvider(&unitRateLimitFractionProvider{})

func taskQueueRateLimitFractionProviderProvider() TaskQueueRateLimitFractionProvider {
	return defaultTaskQueueRateLimitFractionProvider
}
