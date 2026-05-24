package stream

import (
	"go.temporal.io/server/chasm"
)

// TruncateInput is the input to Stream.Truncate / Stream.ForceTruncate.
//
// Spec correspondence: Truncate(upTo) and ForceTruncate(upTo).
type TruncateInput struct {
	UpToOffset int64
	Force      bool
}

// TruncateOutput reports the new base offset and how many subscriptions
// were forcibly closed (only non-zero when Force = true).
type TruncateOutput struct {
	NewBaseOffset                   int64
	ForceTruncatedSubscriptionCount int32
}

// Truncate moves base_offset forward.  Guards:
//   - close-guard (~closed)
//   - up_to <= head_offset (TruncateBeyondHead)
//   - up_to >= base_offset (TruncateBeyondBase)
//   - up_to <= min(active subscriber cursors), unless Force = true
//     (TruncateBlockedBySubscriber)
//
// When Force = true, subscriptions whose cursor falls below up_to are
// forcibly closed (their map entry is removed).  Returns the count.
//
// Spec correspondence: Truncate(upTo) action in StreamCommit.tla L457
// (subscriber-pin-respecting variant) and ForceTruncate(upTo) at L471.
func (s *Stream) Truncate(
	ctx chasm.MutableContext,
	input TruncateInput,
) (TruncateOutput, error) {
	if err := s.closeGuard(); err != nil {
		return TruncateOutput{}, err
	}
	if input.UpToOffset > s.StreamState.HeadOffset {
		return TruncateOutput{}, ErrTruncateBeyondHead
	}
	if input.UpToOffset < s.StreamState.BaseOffset {
		return TruncateOutput{}, ErrTruncateBeyondBase
	}

	var forceCount int32
	if !input.Force {
		// Subscriber-pin guard.
		minCursor := s.minSubscriberCursor(ctx)
		if minCursor >= 0 && input.UpToOffset > minCursor {
			return TruncateOutput{}, ErrTruncateBlockedBySub
		}
	} else {
		// Force: close any subscription whose cursor is below up_to.
		for subID, field := range s.Subscriptions {
			sub, ok := field.TryGet(ctx)
			if !ok || sub == nil || sub.SubscriptionState == nil {
				continue
			}
			if sub.SubscriptionState.Cursor < input.UpToOffset {
				delete(s.Subscriptions, subID)
				forceCount++
			}
		}
	}

	s.StreamState.BaseOffset = input.UpToOffset
	return TruncateOutput{
		NewBaseOffset:                   s.StreamState.BaseOffset,
		ForceTruncatedSubscriptionCount: forceCount,
	}, nil
}

// minSubscriberCursor returns the smallest cursor among active
// subscribers, or -1 if there are none.  Spec correspondence:
// MinSubscriberCursor helper.
func (s *Stream) minSubscriberCursor(ctx chasm.Context) int64 {
	min := int64(-1)
	for _, field := range s.Subscriptions {
		sub, ok := field.TryGet(ctx)
		if !ok || sub == nil || sub.SubscriptionState == nil {
			continue
		}
		c := sub.SubscriptionState.Cursor
		if min < 0 || c < min {
			min = c
		}
	}
	return min
}
