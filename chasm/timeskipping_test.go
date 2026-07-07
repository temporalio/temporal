package chasm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestTimeSkippingTransition covers the pure timeSkippingTransition data structure:
// its constructor, validity check, earliest-future-time tracking, and fast-forward
// gating. It needs no mutable state, so it is a plain test rather than a suite method.
func TestTimeSkippingTransition(t *testing.T) {
	t.Parallel()
	base := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("New sets only the current time", func(t *testing.T) {
		tr := NewTimeSkippingTransition(base)
		require.Equal(t, base, tr.CurrentTime)
		require.True(t, tr.GetTargetTime().IsZero())
		require.False(t, tr.DisabledAfterFastForward)
		require.False(t, tr.IsValid(), "a transition with no target and no disable signal is invalid")
	})
	// Invariant 1: every method is nil-safe — on a nil receiver, and (for GateByFastForward)
	// on a nil/absent fast-forward argument.
	t.Run("nil safe", func(t *testing.T) {
		var nilTr *TimeSkippingTransition
		require.False(t, nilTr.IsValid(), "nil transition is never valid")
		require.NotPanics(t, func() { nilTr.TrackEarliestFutureTime(base.Add(time.Hour)) })
		require.NotPanics(t, func() {
			nilTr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
		})

		// A nil or empty fast-forward must be a no-op, not a spurious disable. A nil proto
		// timestamp's AsTime() is the Unix epoch (not the Go zero time), so this guards against
		// treating "no fast-forward" as a past target.
		tr := NewTimeSkippingTransition(base)
		require.NotPanics(t, func() { tr.GateByFastForward(nil) })
		tr.GateByFastForward(nil)
		require.True(t, tr.GetTargetTime().IsZero())
		require.False(t, tr.DisabledAfterFastForward)

		tr.GateByFastForward(&persistencespb.FastForwardInfo{}) // non-nil ff, nil target time
		require.True(t, tr.GetTargetTime().IsZero())
		require.False(t, tr.DisabledAfterFastForward)
		require.False(t, tr.IsValid())
	})

	// Invariant 2: TrackEarliestFutureTime keeps the earliest strictly-trackable future time
	// and ignores anything that is not a usable future skip target.
	t.Run("earliest future time", func(t *testing.T) {
		t.Run("ignores zero and past candidates", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(time.Time{})          // zero candidate
			tr.TrackEarliestFutureTime(base.Add(-time.Hour)) // past candidate
			require.True(t, tr.GetTargetTime().IsZero())
		})

		t.Run("keeps the earliest of several future candidates", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(3 * time.Hour))
			require.Equal(t, base.Add(3*time.Hour), tr.GetTargetTime())

			tr.TrackEarliestFutureTime(base.Add(time.Hour)) // earlier wins
			require.Equal(t, base.Add(time.Hour), tr.GetTargetTime())

			tr.TrackEarliestFutureTime(base.Add(2 * time.Hour)) // later is ignored
			require.Equal(t, base.Add(time.Hour), tr.GetTargetTime())
		})

		t.Run("accepts a candidate equal to the current time", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base)
			require.Equal(t, base, tr.GetTargetTime())
		})
	})

	// Invariant 3: the fast-forward target is taken — and disables time skipping — exactly when
	// it is the earliest target (nothing earlier tracked). When a real candidate is earlier the
	// fast-forward is not reached and skipping stays enabled. An absent/reached/zero fast-forward
	// is a no-op.
	t.Run("fast-forward fallback and gating", func(t *testing.T) {
		t.Run("taken as the target and disables when nothing earlier exists", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, base.Add(time.Hour).Equal(tr.GetTargetTime()))
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("an earlier tracked target wins over a later fast-forward", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(3 * time.Hour))})
			require.Equal(t, base.Add(time.Hour), tr.GetTargetTime())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("an earlier fast-forward wins over a later tracked target and disables", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(3 * time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, base.Add(time.Hour).Equal(tr.GetTargetTime()))
			require.True(t, tr.DisabledAfterFastForward)
		})

		t.Run("ignores an already-reached fast-forward", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{
				HasReached: true,
				TargetTime: timestamppb.New(base.Add(time.Hour)),
			})
			require.True(t, tr.GetTargetTime().IsZero())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("ignores a zero-valued target time", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(time.Time{})})
			require.True(t, tr.GetTargetTime().IsZero())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("target equal to current disables and sets the target", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base)})
			require.True(t, base.Equal(tr.GetTargetTime()))
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("past target disables as a bare signal", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(-time.Hour))})
			require.True(t, tr.GetTargetTime().IsZero(), "a past target is not a future skip target")
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})
	})

	// Invariant 4: without a current time the transition is always invalid and no setter can
	// make it valid — every field is relative to the current time.
	t.Run("no current time always invalid", func(t *testing.T) {
		t.Run("a directly-set target is still invalid", func(t *testing.T) {
			tr := &TimeSkippingTransition{targetTime: base.Add(time.Hour)}
			require.False(t, tr.IsValid())
		})

		t.Run("a directly-set disable signal is still invalid", func(t *testing.T) {
			tr := &TimeSkippingTransition{DisabledAfterFastForward: true}
			require.False(t, tr.IsValid())
		})

		t.Run("setters are no-ops without a current time", func(t *testing.T) {
			tr := &TimeSkippingTransition{}
			tr.TrackEarliestFutureTime(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, tr.GetTargetTime().IsZero())
			require.False(t, tr.DisabledAfterFastForward)
			require.False(t, tr.IsValid())
		})
	})

	// Invariant 5: GetSkippedDuration is the gap from current to target time, and is zero whenever
	// there is nothing to skip — a nil receiver, no current time, or no target time.
	t.Run("skipped duration", func(t *testing.T) {
		t.Run("nil receiver is zero", func(t *testing.T) {
			var nilTr *TimeSkippingTransition
			require.Zero(t, nilTr.GetSkippedDuration())
		})

		t.Run("no current time is zero", func(t *testing.T) {
			tr := &TimeSkippingTransition{targetTime: base.Add(time.Hour)}
			require.Zero(t, tr.GetSkippedDuration())
		})

		t.Run("no target time is zero", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			require.Zero(t, tr.GetSkippedDuration())
		})

		t.Run("target after current is the gap", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(90 * time.Minute))
			require.Equal(t, 90*time.Minute, tr.GetSkippedDuration())
		})

		t.Run("target equal to current is zero", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base)
			require.Zero(t, tr.GetSkippedDuration())
		})

		t.Run("a past candidate leaves no target and is zero", func(t *testing.T) {
			// TrackEarliestFutureTime rejects a past candidate, so the target stays unset and
			// there is nothing to skip — the duration is zero, never negative.
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(-time.Hour))
			require.True(t, tr.GetTargetTime().IsZero())
			require.Zero(t, tr.GetSkippedDuration())
		})
	})
}
