package verifychildworkflowcompletionrecorded

import (
	"sync"

	"go.temporal.io/server/common/definition"
)

// InFlightResends tracks the parent workflows a shard is currently resending, so concurrent
// verification attempts for the same parent do not each pull its state from the source cluster. A
// resend clones the parent's full mutable state, and callers retry while one is still running.
//
// It also caps how many resends a shard runs at once, bounding the goroutines this path creates.
//
// The zero value is ready to use; hold it by pointer, never copy it.
type InFlightResends struct {
	mu   sync.Mutex
	keys map[definition.WorkflowKey]struct{}
}

// tryClaim reserves key for the caller. It reports claimed=false when a resend for the same parent
// is already running, or atCapacity=true when the shard already has maxInFlight resends. A caller
// that claims the key must release it when the resend finishes.
func (r *InFlightResends) tryClaim(key definition.WorkflowKey, maxInFlight int) (claimed bool, atCapacity bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.keys[key]; ok {
		return false, false
	}
	if len(r.keys) >= maxInFlight {
		return false, true
	}
	if r.keys == nil {
		r.keys = make(map[definition.WorkflowKey]struct{})
	}
	r.keys[key] = struct{}{}
	return true, false
}

func (r *InFlightResends) release(key definition.WorkflowKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keys, key)
}
