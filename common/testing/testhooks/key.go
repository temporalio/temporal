package testhooks

import (
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/namespace"
)

// keyCounter provides unique IDs for keys.
var keyCounter atomic.Int64

// Test hook keys with their return type and scope.
// Try to avoid global scope as it requires a dedicated test cluster.
var (
	MatchingDisableSyncMatch                 = newKey[bool, namespace.ID]()
	MatchingLBForceReadPartition             = newKey[int, namespace.ID]()
	MatchingLBForceWritePartition            = newKey[int, namespace.ID]()
	UpdateWithStartInBetweenLockAndStart     = newKey[func(), namespace.ID]()
	UpdateWithStartOnClosingWorkflowRetry    = newKey[func(), namespace.ID]()
	TaskQueuesInDeploymentSyncBatchSize      = newKey[int, global]()
	MatchingIgnoreRoutingConfigRevisionCheck = newKey[bool, namespace.ID]()
	MatchingDeploymentRegisterErrorBackoff   = newKey[time.Duration, namespace.ID]()
)

// keyID is a unique identifier for a key, used as a map key.
type keyID = int64

// global is the scope type for global hooks.
type global struct{}

// GlobalScope is the singleton value for global hooks.
var GlobalScope = global{}

// Scope indicates the scope of a hook at runtime.
type Scope int

const (
	ScopeNamespace Scope = iota
	ScopeGlobal
)

type Hookable interface {
	Scope() Scope
	Apply(TestHooks, any) func()
}

type hook[T any, S any] struct {
	key   Key[T, S]
	value T
}

func (h hook[T, S]) Scope() Scope {
	return h.key.scope
}

func (h hook[T, S]) Apply(th TestHooks, scope any) func() {
	s, ok := scope.(S)
	if !ok {
		panic("testhooks: scope type mismatch")
	}
	return Set(th, h.key, h.value, s)
}

func NewHook[T any, S any](key Key[T, S], value T) hook[T, S] {
	return hook[T, S]{key: key, value: value}
}

type Key[T any, S any] struct {
	id    keyID
	scope Scope
}

func newKey[T any, S any]() Key[T, S] {
	var zero S
	var s Scope
	switch any(zero).(type) {
	case namespace.ID:
		s = ScopeNamespace
	case global:
		s = ScopeGlobal
	default:
		panic("testhooks: unknown scope type")
	}
	return Key[T, S]{id: keyCounter.Add(1), scope: s}
}
