package fc

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
)

// LimiterSource is used to recognize limiters from config sources so that they can be properly
// removed or updated when the config changes.
type LimiterSource int32

const (
	// not valid limiter
	LimiterSourceInvalid LimiterSource = iota
	// limiter came from task queue config
	LimiterSourceConfig
	// future: per-task, namespace policy, etc.
)

// Limiter identifies one flow control limiter attached to a task, with optional configuration.
// Limiter is implicitly scoped to one namespace by the task that it's attached to.
type Limiter struct {
	Key    string
	Type   enumsspb.LimiterType
	Source LimiterSource
	// Config is *taskqueuepb.ConcurrencyLimit for LIMITER_TYPE_CONCURRENCY or
	// *simplelimiter.Params for LIMITER_TYPE_LOCAL_RATE_LIMIT. These are pointers so that
	// storing them in an `any` doesn't allocate. The pointed-to values must not be modified.
	Config        any
	ConfigVersion int64
}

// fcTask is the interface that flow control needs from a task.
// *matching.internalTask implements this.
type fcTask interface {
	// Limiters returns the limiters that apply to the task. NewTx may reorder the returned
	// slice in place, so the caller must have exclusive access to it.
	Limiters() []Limiter
	PriorityAndAge() (int32, time.Time) // TODO(fc): consider moving to ReadinessCallback
}

// ReadinessCallback is something we can notify when we think the readiness state of a limiter
// may have changed.
// Note: because of how we store callbacks, the concrete type implementing ReadinessCallback
// _must_ be a pointer type. Currently it's always *matcherData (except in tests).
type ReadinessCallback interface {
	OnReady()
}
