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
	errorsMu     sync.RWMutex
	errorsByName = map[string]func(msg string) error{}
)

// RegisterError registers a constructor for the named error. It panics if the name is already
// registered.
func RegisterError(name string, newError func(msg string) error) {
	errorsMu.Lock()
	defer errorsMu.Unlock()
	if _, ok := errorsByName[name]; ok {
		panic(fmt.Sprintf("failpoint: duplicate error %q", name))
	}
	errorsByName[name] = newError
}

// NewError returns the named error with the message msg. It returns false if no error is
// registered under the name.
func NewError(name string, msg string) (error, bool) {
	errorsMu.RLock()
	newError, ok := errorsByName[name]
	errorsMu.RUnlock()
	if !ok {
		return nil, false
	}
	return newError(msg), true
}

// ErrorNames returns the names of the registered errors, sorted.
func ErrorNames() []string {
	errorsMu.RLock()
	defer errorsMu.RUnlock()
	return slices.Sorted(maps.Keys(errorsByName))
}
