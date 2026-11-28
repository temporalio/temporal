package failpoint

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

// The error catalog lets failpoints configured from outside the process, which can only name an
// error, return the errors that the code under test handles. Packages register the errors of
// their layer, usually in an init function.
var (
	errorsMu     sync.Mutex
	errorsByName = map[string]func(msg string) error{}
)

// RegisterError registers a constructor for the named error. It panics if the name is already
// registered.
func RegisterError(name string, newError func(msg string) error) {
	errorsMu.Lock()
	defer errorsMu.Unlock()
	if _, ok := errorsByName[name]; ok {
		//nolint:forbidigo // duplicate registration is a programming error
		panic(fmt.Sprintf("failpoint: duplicate error %q", name))
	}
	errorsByName[name] = newError
}

// LookupError returns the constructor of the named error. It returns false if no error is
// registered under the name.
func LookupError(name string) (func(msg string) error, bool) {
	errorsMu.Lock()
	defer errorsMu.Unlock()
	newError, ok := errorsByName[name]
	return newError, ok
}

// ErrorNames returns the names of the registered errors, sorted.
func ErrorNames() []string {
	errorsMu.Lock()
	defer errorsMu.Unlock()
	return slices.Sorted(maps.Keys(errorsByName))
}
