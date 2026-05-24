package stream

import (
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// Subscription is one entry in Stream.Subscriptions.  Sub-component so it
// can carry its own per-subscription side-effect tasks (delivery, drain,
// close).
//
// Spec correspondence: subscribers[s] entry.
type Subscription struct {
	chasm.UnimplementedComponent
	*streampb.SubscriptionState
}

// LifecycleState: Subscriptions are running for the lifetime of the
// stream (or until explicit Unsubscribe / force-truncate / close-drain).
// We never report Completed at the Subscription level because the
// chasm Map entry is removed when the lifecycle ends.
func (sub *Subscription) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// AdvanceCursor advances the subscription's cursor by n.  Decrements
// outstanding_bytes by the same amount and clears at_capacity if it had
// been set.  Caller (delivery side-effect task ack path) must hold the
// chasm execution write lock.
//
// Spec correspondence: AdvanceCursor(s).
func (sub *Subscription) AdvanceCursor(n int64, ackedBytes int64) {
	sub.SubscriptionState.Cursor += n
	sub.SubscriptionState.OutstandingBytes -= ackedBytes
	if sub.SubscriptionState.OutstandingBytes < 0 {
		// Defensive: should never happen if the delivery accounting is
		// correct.  Floor to 0 rather than corrupt.
		sub.SubscriptionState.OutstandingBytes = 0
	}
	if sub.SubscriptionState.OutstandingBytes < sub.SubscriptionState.MaxOutstandingBytes {
		sub.SubscriptionState.AtCapacity = false
		sub.SubscriptionState.AtCapacitySince = nil
	}
}
