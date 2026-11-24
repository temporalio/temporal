package failpoint

import (
	"fmt"
	"sync"
)

// Key identifies a failpoint by name. A key applies to all scopes unless it is scoped to one with
// ForScope. A scope is a string that the code under test chooses, such as the ID of a tenant, so
// that a failpoint only fires for the requests of that scope.
type Key struct {
	name  string
	scope string
}

var (
	keyNamesMu sync.Mutex
	keyNames   = map[string]struct{}{}
)

// NewKey returns the key for the named failpoint. Each name can only be declared once; NewKey
// panics on a duplicate so that two failpoints cannot share a name by accident.
func NewKey(name string) Key {
	keyNamesMu.Lock()
	defer keyNamesMu.Unlock()
	if _, ok := keyNames[name]; ok {
		panic(fmt.Sprintf("failpoint: duplicate key %q", name))
	}
	keyNames[name] = struct{}{}
	return Key{name: name}
}

// Keys of the failpoints at a transport boundary that fire for every operation. A call site executes
// one after the key of the operation, such as GRPCKey for the request of a gRPC method, and only if
// that failpoint did not fire with any action (see ExecuteWithFired). Their names cannot collide
// with the keys of an operation, which contain a gRPC method or an HTTP request method and path.
var (
	// AnyGRPCKey fires before the handler of every gRPC method runs.
	AnyGRPCKey = Key{name: "grpc:*"}
	// AnyGRPCResponseKey fires after the handler of every gRPC method returns.
	AnyGRPCResponseKey = Key{name: "grpc-response:*"}
	// AnyHTTPKey fires before every outgoing HTTP request is sent.
	AnyHTTPKey = Key{name: "http:*"}
	// AnyHTTPResponseKey fires after every outgoing HTTP request returns.
	AnyHTTPResponseKey = Key{name: "http-response:*"}
)

// GRPCKey returns the key of the failpoint that fires before the handler of a gRPC method runs.
// fullMethod is the full method name, for example "/pkg.Service/Method".
func GRPCKey(fullMethod string) Key {
	return Key{name: "grpc:" + fullMethod}
}

// GRPCResponseKey returns the key of the failpoint that fires after the handler of a gRPC method
// returns.
func GRPCResponseKey(fullMethod string) Key {
	return Key{name: "grpc-response:" + fullMethod}
}

// HTTPKey returns the key of the failpoint that fires before an outgoing HTTP request is sent.
// operation is the request method and path, for example "POST /callback".
func HTTPKey(operation string) Key {
	return Key{name: "http:" + operation}
}

// HTTPResponseKey returns the key of the failpoint that fires after an outgoing HTTP request
// returns.
func HTTPResponseKey(operation string) Key {
	return Key{name: "http-response:" + operation}
}

// KeyFromName returns the key for a failpoint name received from outside the process, for example
// in a remote request. Unlike NewKey, it does not declare the name.
func KeyFromName(name string, scope string) Key {
	return Key{name: name, scope: scope}
}

// Name returns the failpoint name.
func (k Key) Name() string { return k.name }

// Scope returns the scope the key is scoped to, or "" for all scopes.
func (k Key) Scope() string { return k.scope }

// ForScope returns the key scoped to scope.
func (k Key) ForScope(scope string) Key {
	return Key{name: k.name, scope: scope}
}

func (k Key) String() string {
	if k.scope == "" {
		return k.name
	}
	return fmt.Sprintf("%s@%s", k.name, k.scope)
}
