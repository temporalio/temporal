package failpoint

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

// The behavior catalog lets failpoints configured from outside the process, which can only name a
// behavior, call code of the process. Packages register their behaviors, usually in an init
// function.
var (
	behaviorsMu     sync.RWMutex
	behaviorsByName = map[string]func() error{}
)

// RegisterBehavior registers fn under name. A Call failpoint that names the behavior calls fn and
// returns its error. It panics if the name is already registered.
func RegisterBehavior(name string, fn func() error) {
	behaviorsMu.Lock()
	defer behaviorsMu.Unlock()
	if _, ok := behaviorsByName[name]; ok {
		panic(fmt.Sprintf("failpoint: duplicate behavior %q", name))
	}
	behaviorsByName[name] = fn
}

// LookupBehavior returns the named behavior. It returns false if no behavior is registered under
// the name.
func LookupBehavior(name string) (func() error, bool) {
	behaviorsMu.RLock()
	defer behaviorsMu.RUnlock()
	fn, ok := behaviorsByName[name]
	return fn, ok
}

// BehaviorNames returns the names of the registered behaviors, sorted.
func BehaviorNames() []string {
	behaviorsMu.RLock()
	defer behaviorsMu.RUnlock()
	return slices.Sorted(maps.Keys(behaviorsByName))
}
