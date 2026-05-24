package stream

import (
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// CloseInput is the input to Stream.Close (phase-1 seal).
//
// Spec correspondence: Close action arguments.
type CloseInput struct {
	ClosedBy    string
	CloseReason streampb.StreamCloseReason
}

// Close seals the stream (phase 1).  Phase 2 (drain) runs as a chained
// CloseCleanupTask pure task that iterates inflights and subscriptions
// in chunks.
//
// Guards: not already closed.  Subsequent mutators (PreparePublish,
// CommitPublish, Truncate, ForceTruncate) all close-guard and return
// StreamClosed.
//
// Spec correspondence: Close action in StreamCommit.tla L491.
// CloseOutput is returned from Close.  Empty struct today; reserved for
// future fields like inflight-count-at-close.
type CloseOutput struct{}

func (s *Stream) Close(
	ctx chasm.MutableContext,
	input CloseInput,
) (CloseOutput, error) {
	if s.StreamState.Closed {
		return CloseOutput{}, ErrStreamClosed
	}
	now := ctx.Now(nil)
	s.StreamState.Closed = true
	s.StreamState.ClosedBy = input.ClosedBy
	s.StreamState.ClosedTime = timestampNow(now)
	s.StreamState.CloseReason = input.CloseReason

	// Arm phase-2 drain.  The CloseCleanupTask handler runs in chunks
	// and re-arms itself until both maps are empty.
	ctx.AddTask(s, chasm.TaskAttributes{}, &streampb.CloseCleanupTask{})
	return CloseOutput{}, nil
}
