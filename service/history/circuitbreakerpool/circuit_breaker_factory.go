package circuitbreakerpool

import (
	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/collection"
)

type CircuitBreakerPool[K comparable] struct {
	m *collection.OnceMap[K, circuitbreaker.TwoStepCircuitBreaker]
}

func (p *CircuitBreakerPool[K]) Get(key K) circuitbreaker.TwoStepCircuitBreaker {
	return p.m.Get(key)
}

func NewCircuitBreakerPool[K comparable](
	constructor func(key K) circuitbreaker.TwoStepCircuitBreaker,
) *CircuitBreakerPool[K] {
	return &CircuitBreakerPool[K]{
		m: collection.NewOnceMap(constructor),
	}
}
