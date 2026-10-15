package callback

import (
	"fmt"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
)

// ScheduleStandbyCallbacks transitions all STANDBY callbacks to SCHEDULED state,
// triggering their invocation. Used by both workflows and standalone activities
// when the execution reaches a terminal state.
func ScheduleStandbyCallbacks(ctx chasm.MutableContext, callbacks chasm.Map[string, *Callback]) error {
	for _, field := range callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
			continue
		}
		if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
			return err
		}
	}
	return nil
}

// CompletionCallbackID defines the stable key used for keeping track of attached completion callbacks
// within a chasm.Map[string, *Callback]. To disambiguate the ID when multiple completion callbacks are
// attached in the same request, both the requestID and the index are required.
func CompletionCallbackID(requestID string, idx int) string {
	return fmt.Sprintf("%s-%d", requestID, idx)
}

// HasCallbacksForRequest returns true if the given completion callbacks map has any callbacks attached
// that came from the given request ID.
func HasCallbacksForRequest(target chasm.Map[string, *Callback], requestID string) bool {
	// i.e. the CHASM component hasn't initialized the callbacks map because it hasn't needed to yet.
	if target == nil {
		return false
	}
	// This relies on attaching being atomic. Assuming that if the first/0th completion callback is
	// present, then all the other callbacks for the same request would be present too.
	_, ok := target[CompletionCallbackID(requestID, 0)]
	return ok
}
