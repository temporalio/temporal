package testhooks

// keyID is a unique identifier for a key, used as a map key.
type keyID = int64

// Key identifies a hook with value type T and scope type S.
type Key[T any, S any] struct {
	id keyID
}

type globalScope struct{}

// GlobalScope is the scope for hooks registered with [NewGlobalKey].
var GlobalScope = globalScope{}

// ScopeType describes how a hook is scoped.
type ScopeType int

const (
	// ScopeValue identifies a hook associated with a caller-supplied scope value.
	ScopeValue ScopeType = iota
	// ScopeGlobal identifies a hook shared across all scopes.
	ScopeGlobal
)

// NewGlobalKey returns a key for a global hook.
func NewGlobalKey[T any]() Key[T, globalScope] {
	return NewKey[T, globalScope]()
}
