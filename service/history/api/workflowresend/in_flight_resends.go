package workflowresend

import (
	"sync"

	"go.temporal.io/server/common/definition"
)

// InFlightResends tracks the workflows a shard is currently resending, so concurrent attempts for
// the same workflow do not each pull its state from the source cluster. It also caps how many
// resends a shard runs at once, bounding the goroutines created by resend paths.
//
// The zero value is ready to use; hold it by pointer, never copy it.
type InFlightResends struct {
	mu   sync.Mutex
	keys map[definition.WorkflowKey]struct{}
}

// TryClaim reserves key for the caller. It reports claimed=false when a resend for the same
// workflow is already running, or atCapacity=true when the shard already has maxInFlight resends.
// A caller that claims the key must release it when the resend finishes.
func (r *InFlightResends) TryClaim(key definition.WorkflowKey, maxInFlight int) (claimed bool, atCapacity bool) {
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

// Release makes key available for a future resend.
func (r *InFlightResends) Release(key definition.WorkflowKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keys, key)
}
