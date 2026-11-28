package failpoint

import (
	"fmt"
	"sync"
)

// Key identifies a failpoint by name. The type parameter is the type of the value that the
// failpoint returns, so call sites and tests agree on it at compile time. A key applies to all
// scopes unless it is scoped to one with ForScope. A scope is a string that the code under test
// chooses, such as the ID of a tenant, so that a failpoint only fires for the requests of that
// scope.
type Key[T any] struct {
	// Zero-size type marker so Key[A] cannot be converted to Key[B], while remaining comparable
	// even when T is not comparable.
	_     [0]*T
	name  string
	scope string
}

// AnyKey is a Key of any type, for code that doesn't depend on the type of the failpoint.
type AnyKey interface {
	// Name returns the failpoint name.
	Name() string
	// Scope returns the scope the key is scoped to, or "" for all scopes.
	Scope() string
	String() string
	id() keyID
}

// keyID is the identity of a failpoint in a registry: a name and a scope, whatever the type.
type keyID struct {
	name  string
	scope string
}

func (k keyID) String() string {
	if k.scope == "" {
		return k.name
	}
	return fmt.Sprintf("%s@%s", k.name, k.scope)
}

var (
	keysMu       sync.Mutex
	declaredKeys = map[string]struct{}{}
)

// NewKey returns the key for the named failpoint, whose value has type T. Each name must be
// non-empty and can only be declared once; NewKey panics on an empty or duplicate name so that
// zero-value keys are always invalid and two failpoints cannot share a name by accident.
func NewKey[T any](name string) Key[T] {
	if name == "" {
		//nolint:forbidigo // an empty key name is a programming error
		panic("failpoint: empty key name")
	}
	keysMu.Lock()
	defer keysMu.Unlock()
	if _, ok := declaredKeys[name]; ok {
		//nolint:forbidigo // duplicate key is a programming error
		panic(fmt.Sprintf("failpoint: duplicate key %q", name))
	}
	declaredKeys[name] = struct{}{}
	return Key[T]{name: name}
}

// Name returns the failpoint name.
func (k Key[T]) Name() string {
	return k.name
}

// Scope returns the scope the key is scoped to, or "" for all scopes.
func (k Key[T]) Scope() string {
	return k.scope
}

// ForScope returns the key scoped to scope.
func (k Key[T]) ForScope(scope string) Key[T] {
	return Key[T]{name: k.name, scope: scope}
}

func (k Key[T]) String() string {
	return k.id().String()
}

func (k Key[T]) id() keyID {
	return keyID{name: k.name, scope: k.scope}
}
