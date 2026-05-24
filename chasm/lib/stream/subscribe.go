package stream

import (
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// SubscribeInput is the input to Stream.Subscribe.
//
// Spec correspondence: Subscribe(s) action arguments.
type SubscribeInput struct {
	SubscriberID       string
	InWorkflow         bool
	TargetWorkflowID   string
	TargetRunID        string
	StartOffset        int64
	Topics             []string
	MaxOutstandingBytes int64
}

// SubscribeOutput reports the assigned starting cursor.
type SubscribeOutput struct {
	SubscriberID string
	Cursor       int64
}

// Subscribe creates a new Subscription map entry on the Stream.
//
// Guards:
//   - close-guard (~closed)
//   - StartOffset must be within [base_offset, head_offset]
//   - SubscriberID must be unique within this stream
//
// Spec correspondence: Subscribe(s) action in StreamCommit.tla L518.
func (s *Stream) Subscribe(
	ctx chasm.MutableContext,
	input SubscribeInput,
) (SubscribeOutput, error) {
	if err := s.closeGuard(); err != nil {
		return SubscribeOutput{}, err
	}
	if input.SubscriberID == "" {
		return SubscribeOutput{}, serviceerror.NewInvalidArgument("subscriber_id is required")
	}
	if _, exists := s.Subscriptions[input.SubscriberID]; exists {
		return SubscribeOutput{}, serviceerror.NewAlreadyExist("subscription with this subscriber_id already exists")
	}
	if input.StartOffset < s.StreamState.BaseOffset {
		return SubscribeOutput{}, ErrOffsetTruncated
	}
	if input.StartOffset > s.StreamState.HeadOffset {
		return SubscribeOutput{}, serviceerror.NewInvalidArgument("start_offset exceeds head_offset")
	}

	maxBytes := input.MaxOutstandingBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxOutstandingBytes
	}

	now := ctx.Now(nil)
	sub := &Subscription{
		SubscriptionState: &streampb.SubscriptionState{
			SubscriberId:        input.SubscriberID,
			InWorkflow:          input.InWorkflow,
			TargetWorkflowId:    input.TargetWorkflowID,
			TargetRunId:         input.TargetRunID,
			Cursor:              input.StartOffset,
			Topics:              input.Topics,
			MaxOutstandingBytes: maxBytes,
			CreatedTime:         timestampNow(now),
		},
	}
	s.Subscriptions[input.SubscriberID] = chasm.NewComponentField(ctx, sub)
	return SubscribeOutput{
		SubscriberID: input.SubscriberID,
		Cursor:       input.StartOffset,
	}, nil
}

// UnsubscribeInput is the input to Stream.Unsubscribe.
//
// Spec correspondence: Unsubscribe(s) action arguments.
type UnsubscribeInput struct {
	SubscriberID string
}

// Unsubscribe removes a Subscription map entry.  No close-guard — even
// closed streams allow unsubscribe so external subscribers can release
// resources cleanly during shutdown.
//
// Spec correspondence: Unsubscribe(s) action in StreamCommit.tla L534.
func (s *Stream) Unsubscribe(
	ctx chasm.MutableContext,
	input UnsubscribeInput,
) error {
	if _, exists := s.Subscriptions[input.SubscriberID]; !exists {
		return serviceerror.NewNotFound("subscription not found")
	}
	delete(s.Subscriptions, input.SubscriberID)
	return nil
}

// defaultMaxOutstandingBytes per design-overview.html §4b "Subscription
// buffer".  1 MB matches the default segment max_bytes.
const defaultMaxOutstandingBytes = int64(1 << 20)
