package faultinjectiontest

import (
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/testx/failpoint"
)

// callbackLists holds the callbacks added to each failpoint by AddCallback.
var callbackLists = struct {
	mu    sync.Mutex
	lists map[callbackListKey]*callbackList
}{lists: make(map[callbackListKey]*callbackList)}

type callbackListKey struct {
	registry failpoint.Registry
	key      failpoint.Key
}

type callbackList struct {
	fp *failpoint.Failpoint[error]

	mu        sync.Mutex
	callbacks []*callback
}

type callback struct {
	fn func(arg any) error
}

// CallbackOwner is the Owner of the failpoints that AddCallback registers, so that other code, such
// as rules, neither reconfigures nor removes them.
const CallbackOwner = "fault injection callbacks"

// AddCallback adds fn to the callbacks of the error failpoint for key in registry, so that several
// callers, such as the fault injections of one test, can share the failpoint. Each execution calls
// the callbacks with its argument in the order they were added, until one returns an error, which
// the failpoint returns. A callback can run after it was removed if the execution started before.
//
// The failpoint is registered with the first callback and removed with the last, so it is never
// registered without a callback. Its Owner is CallbackOwner. AddCallback returns an error if
// another failpoint is registered for key, or replaced the one it registered.
func AddCallback(registry failpoint.Registry, key failpoint.Key, fn func(arg any) error) (remove func(), err error) {
	callbackLists.mu.Lock()
	defer callbackLists.mu.Unlock()

	listKey := callbackListKey{registry: registry, key: key}
	cb := &callback{fn: fn}
	list := callbackLists.lists[listKey]
	if list == nil {
		list = &callbackList{fp: failpoint.NewOwnedFailpoint[error](key, CallbackOwner), callbacks: []*callback{cb}}
		list.fp.CallWith(list.run, failpoint.Always)
		if existing := registry.GetOrSet(key, list.fp); existing != list.fp {
			return nil, fmt.Errorf("failpoint %s is already registered", key)
		}
		callbackLists.lists[listKey] = list
	} else {
		if registry.Get(key) != list.fp {
			return nil, fmt.Errorf("failpoint %s was replaced", key)
		}
		list.mu.Lock()
		list.callbacks = append(list.callbacks, cb)
		list.mu.Unlock()
	}

	var once sync.Once
	return func() { once.Do(func() { removeCallback(listKey, list, cb) }) }, nil
}

func removeCallback(listKey callbackListKey, list *callbackList, cb *callback) {
	callbackLists.mu.Lock()
	defer callbackLists.mu.Unlock()

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
	listKey.registry.CompareAndDelete(listKey.key, list.fp)
	list.fp.Disable()
	delete(callbackLists.lists, listKey)
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
