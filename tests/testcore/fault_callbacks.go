package testcore

import (
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/testx/failpoint"
)

// faultCallbacks holds the callbacks that the fault injections of a test added to the error
// failpoints of its cluster, so that several faults, such as the fault injections of one test,
// share the failpoint of a key.
type faultCallbacks struct {
	registry *failpoint.Registry

	mu    sync.Mutex
	lists map[failpoint.Key[error]]*callbackList
}

func newFaultCallbacks(registry *failpoint.Registry) *faultCallbacks {
	return &faultCallbacks{registry: registry, lists: make(map[failpoint.Key[error]]*callbackList)}
}

type callbackList struct {
	fp *failpoint.Failpoint[error]

	mu        sync.Mutex
	callbacks []*callback
}

type callback struct {
	fn func(arg any) error
}

// add adds fn to the callbacks of the error failpoint for key. Each execution calls the callbacks
// with its argument in the order they were added, until one returns an error, which the failpoint
// returns. A callback can run after it was removed if the execution started before.
//
// The failpoint is registered with the first callback and removed with the last, so it is never
// registered without a callback, which would keep the fallback of the registry from deciding for
// the key. add returns an error if another failpoint is registered for key, or replaced the one it
// registered.
func (c *faultCallbacks) add(key failpoint.Key[error], fn func(arg any) error) (remove func(), err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cb := &callback{fn: fn}
	list := c.lists[key]
	if list == nil {
		list = &callbackList{fp: failpoint.NewFailpoint(key), callbacks: []*callback{cb}}
		list.fp.CallWithArg(list.run)
		registered := false
		c.registry.Atomically(func() {
			registered = c.registry.GetOrSet(list.fp) == list.fp
		})
		if !registered {
			return nil, fmt.Errorf("failpoint %s is already registered", key)
		}
		c.lists[key] = list
	} else {
		if c.registry.Get(key) != list.fp {
			return nil, fmt.Errorf("failpoint %s was replaced", key)
		}
		list.mu.Lock()
		list.callbacks = append(list.callbacks, cb)
		list.mu.Unlock()
	}

	var once sync.Once
	return func() { once.Do(func() { c.remove(key, list, cb) }) }, nil
}

func (c *faultCallbacks) remove(key failpoint.Key[error], list *callbackList, cb *callback) {
	c.mu.Lock()
	defer c.mu.Unlock()

	list.mu.Lock()
	if len(list.callbacks) > 1 {
		list.callbacks = slices.DeleteFunc(list.callbacks, func(c *callback) bool { return c == cb })
		list.mu.Unlock()
		return
	}
	list.mu.Unlock()
	// Remove the last callback with its failpoint in one step: executions either find the failpoint
	// with the callback, or no failpoint and fall back to the failpoint without a scope. Only remove
	// this failpoint, not one that replaced it, and disable it, so executions that already found it
	// don't call back.
	c.registry.CompareAndDelete(list.fp)
	list.fp.Disable()
	delete(c.lists, key)
}

func (l *callbackList) run(arg any) error {
	l.mu.Lock()
	callbacks := slices.Clone(l.callbacks)
	l.mu.Unlock()
	for _, cb := range callbacks {
		if err := cb.fn(arg); err != nil {
			return err
		}
	}
	return nil
}
