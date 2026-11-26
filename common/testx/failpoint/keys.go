package failpoint

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
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

// keyKind is what the declaration of a key tells the rules about its failpoints.
type keyKind struct {
	// accepts reports whether a failpoint of the key can return err, so a rule can use Return.
	accepts func(err error) bool
	// newFailpoint creates a failpoint of the type of the key.
	newFailpoint func(id keyID) ruleTarget
	// typ is the value type of the key.
	typ reflect.Type
}

// reservedPrefix is a prefix that ReserveKeyPrefix reserved, and the kind of every key under it.
type reservedPrefix struct {
	prefix string
	typ    reflect.Type
	kind   keyKind
}

var (
	keysMu           sync.Mutex
	keyKinds         = map[string]keyKind{}
	reservedPrefixes []reservedPrefix
)

func kindOf[T any]() keyKind {
	return keyKind{
		accepts: func(err error) bool {
			_, ok := any(err).(T)
			return ok
		},
		newFailpoint: func(id keyID) ruleTarget {
			fp := NewFailpoint(Key[T]{name: id.name, scope: id.scope})
			fp.managed = true
			return fp
		},
		typ: reflect.TypeFor[T](),
	}
}

// lookupKind returns the kind of the keys with name, or an error unless name is the name of a
// declared key or has a reserved prefix, so a typo in a rule doesn't create a failpoint that no
// call site executes.
func lookupKind(name string) (keyKind, error) {
	keysMu.Lock()
	defer keysMu.Unlock()
	if kind, ok := keyKinds[name]; ok {
		return kind, nil
	}
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p.prefix) {
			return p.kind, nil
		}
	}
	prefixes := make([]string, 0, len(reservedPrefixes))
	for _, p := range reservedPrefixes {
		prefixes = append(prefixes, p.prefix)
	}
	return keyKind{}, fmt.Errorf("unknown key %q, declared keys are %s and keys with the prefixes %s",
		name, strings.Join(slices.Sorted(maps.Keys(keyKinds)), ", "), strings.Join(slices.Sorted(slices.Values(prefixes)), ", "))
}

// ErrorKey returns the key of the failpoint that a request from another process names, such as one
// of a failpointgrpc service. The name must be that of a key declared with NewKey[error], or have
// a prefix reserved with ReserveKeyPrefix[error], as the key of a Rule must be declared.
func ErrorKey(name, scope string) (Key[error], error) {
	kind, err := lookupKind(name)
	if err != nil {
		return Key[error]{}, err
	}
	if kind.typ != reflect.TypeFor[error]() {
		return Key[error]{}, fmt.Errorf("key %q has the value type %v, not error", name, kind.typ)
	}
	return Key[error]{name: name, scope: scope}, nil
}

// KeyPrefix builds the keys of the failpoints that code names at run time, such as from the method
// of a request. All its keys have the value type T, for call sites and for rules. Get one from
// ReserveKeyPrefix.
type KeyPrefix[T any] struct {
	prefix string
}

// ReserveKeyPrefix reserves a prefix for the names of keys that code builds at run time, such as
// from the method of a request, with KeyPrefix.Key. Rules can name the keys under the prefix, and
// their failpoints have the value type T. NewKey panics on a name with a reserved prefix, so a
// declared key can't collide with them. It is meant for init functions and package variables.
//
// ReserveKeyPrefix panics if the prefix is empty, if a key declared with NewKey already has it,
// or if it overlaps a prefix reserved with another type. Reserving a prefix again with the same
// type returns the same KeyPrefix.
func ReserveKeyPrefix[T any](prefix string) KeyPrefix[T] {
	if prefix == "" {
		//nolint:forbidigo // an empty prefix is a programming error
		panic("failpoint: empty key prefix")
	}
	typ := reflect.TypeFor[T]()
	keysMu.Lock()
	defer keysMu.Unlock()
	for name := range keyKinds {
		if strings.HasPrefix(name, prefix) {
			//nolint:forbidigo // a conflicting declaration is a programming error
			panic(fmt.Sprintf("failpoint: key %q is already declared and has the prefix %q", name, prefix))
		}
	}
	for _, p := range reservedPrefixes {
		if p.prefix == prefix && p.typ == typ {
			return KeyPrefix[T]{prefix: prefix}
		}
		if strings.HasPrefix(p.prefix, prefix) || strings.HasPrefix(prefix, p.prefix) {
			//nolint:forbidigo // a conflicting reservation is a programming error
			panic(fmt.Sprintf("failpoint: key prefix %q of type %v overlaps the prefix %q of type %v", prefix, typ, p.prefix, p.typ))
		}
	}
	reservedPrefixes = append(reservedPrefixes, reservedPrefix{prefix: prefix, typ: typ, kind: kindOf[T]()})
	return KeyPrefix[T]{prefix: prefix}
}

// Key returns the key whose name is the prefix followed by suffix. Scope it with ForScope.
func (p KeyPrefix[T]) Key(suffix string) Key[T] {
	return Key[T]{name: p.prefix + suffix}
}

// NewKey returns the key for the named failpoint, whose value has type T. Each name must be
// non-empty and can only be declared once; NewKey panics on an empty or duplicate name so that
// zero-value keys are always invalid and two failpoints cannot share a name by accident. It also
// panics on a name with a reserved prefix, see ReserveKeyPrefix.
func NewKey[T any](name string) Key[T] {
	if name == "" {
		//nolint:forbidigo // an empty key name is a programming error
		panic("failpoint: empty key name")
	}
	keysMu.Lock()
	defer keysMu.Unlock()
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p.prefix) {
			//nolint:forbidigo // a reserved name is a programming error
			panic(fmt.Sprintf("failpoint: key %q has the reserved prefix %q", name, p.prefix))
		}
	}
	if _, ok := keyKinds[name]; ok {
		//nolint:forbidigo // duplicate key is a programming error
		panic(fmt.Sprintf("failpoint: duplicate key %q", name))
	}
	keyKinds[name] = kindOf[T]()
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
