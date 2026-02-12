package testhooks

import (
	"time"

	"go.temporal.io/server/common/namespace"
)

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

// Hook bundles a key and value for type-erased use in InjectHook.
type Hook struct {
	scopeType Scope
	apply     func(TestHooks, any) func()
}

func (h Hook) Scope() Scope                         { return h.scopeType }
func (h Hook) Apply(th TestHooks, scope any) func() { return h.apply(th, scope) }

func NewHook[T any, S any](key Key[T, S], value T) Hook {
	return Hook{
		scopeType: key.scope,
		apply: func(th TestHooks, scope any) func() {
			if _, ok := scope.(S); !ok {
				panic("testhooks: scope type mismatch")
			}
			return Set(th, key, value, scope)
		},
	}
}

type Key[T any, S any] struct {
	id    keyID
	scope Scope
}

