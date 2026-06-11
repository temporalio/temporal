package replication

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	namespaceIsolationManagerSuite struct {
		suite.Suite
		*require.Assertions

		manager *namespaceIsolationManager
	}
)

func TestNamespaceIsolationManagerSuite(t *testing.T) {
	s := new(namespaceIsolationManagerSuite)
	suite.Run(t, s)
}

func (s *namespaceIsolationManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.manager = newNamespaceIsolationManager()
}

func (s *namespaceIsolationManagerSuite) TestDefaultLowCursor() {
	s.Equal(int64(0), s.manager.DefaultLowCursor())
	s.manager.UpdateDefaultLowCursor(42)
	s.Equal(int64(42), s.manager.DefaultLowCursor())
}

func (s *namespaceIsolationManagerSuite) TestIsDefaultLowExcluded_NotThrottled() {
	s.False(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestThrottle_ExcludesFromDefaultLow() {
	s.manager.UpdateDefaultLowCursor(100)
	ok := s.manager.ThrottleNamespace("ns-a")
	s.True(ok)
	s.True(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestThrottle_AlreadyThrottled_ReturnsFalse() {
	s.manager.ThrottleNamespace("ns-a")
	ok := s.manager.ThrottleNamespace("ns-a")
	s.False(ok)
}

func (s *namespaceIsolationManagerSuite) TestRemove_AllowsDefaultLowAgain() {
	s.manager.ThrottleNamespace("ns-a")
	s.True(s.manager.IsDefaultLowExcluded("ns-a"))
	s.manager.Remove("ns-a")
	s.False(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestRemove_CancelsContext() {
	s.manager.ThrottleNamespace("ns-a")
	ctx, _, ok := s.manager.BeginCatchup(context.Background(), "ns-a")
	s.True(ok)
	s.manager.Remove("ns-a")
	s.Error(ctx.Err())
}

func (s *namespaceIsolationManagerSuite) TestRemove_Idempotent() {
	s.manager.ThrottleNamespace("ns-a")
	s.manager.Remove("ns-a")
	s.manager.Remove("ns-a") // should not panic
}

// BeginCatchup tests

func (s *namespaceIsolationManagerSuite) TestBeginCatchup_NotRegistered_ReturnsFalse() {
	_, _, ok := s.manager.BeginCatchup(context.Background(), "ns-missing")
	s.False(ok)
}

func (s *namespaceIsolationManagerSuite) TestBeginCatchup_AlreadyCatching_ReturnsFalse() {
	s.manager.ThrottleNamespace("ns-a")
	_, _, ok := s.manager.BeginCatchup(context.Background(), "ns-a")
	s.True(ok)
	_, _, ok = s.manager.BeginCatchup(context.Background(), "ns-a")
	s.False(ok)
}

func (s *namespaceIsolationManagerSuite) TestBeginCatchup_ReturnsStartCursor() {
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	ctx, startCursor, ok := s.manager.BeginCatchup(context.Background(), "ns-a")
	s.True(ok)
	s.NotNil(ctx)
	s.Equal(int64(100), startCursor)
}

// CheckAndRemoveIfCaughtUp tests

func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_NotThrottled_ReturnsFalse() {
	s.False(s.manager.CheckAndRemoveIfCaughtUp("ns-missing"))
}

func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_NotYetCatching_ReturnsFalse() {
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	// paused but not catching up yet — must not remove
	s.False(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
	s.True(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_BehindDefaultLow_ReturnsFalse() {
	s.manager.UpdateDefaultLowCursor(50)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	// dedicated reader cursor starts at 50, default LOW advances to 100
	s.manager.UpdateDefaultLowCursor(100)
	s.False(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
	s.True(s.manager.IsDefaultLowExcluded("ns-a")) // still excluded
}

func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_CaughtUp_RemovesAndCancels() {
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	ctx, _, _ := s.manager.BeginCatchup(context.Background(), "ns-a")
	// dedicated reader catches up: cursor == defaultLowCursor
	s.manager.UpdateDedicatedCursor("ns-a", 100)
	s.True(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
	s.False(s.manager.IsDefaultLowExcluded("ns-a"))
	s.Error(ctx.Err()) // context cancelled
}

func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_AheadOfDefaultLow_Removes() {
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.UpdateDedicatedCursor("ns-a", 150) // dedicated is ahead
	s.True(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
	s.False(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_Idempotent() {
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.UpdateDedicatedCursor("ns-a", 100)
	s.True(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
	s.False(s.manager.CheckAndRemoveIfCaughtUp("ns-a")) // already gone
}

// UpdateDedicatedCursor tests

func (s *namespaceIsolationManagerSuite) TestUpdateDedicatedCursor_NotThrottled_NoOp() {
	s.manager.UpdateDedicatedCursor("ns-missing", 999) // should not panic
}

func (s *namespaceIsolationManagerSuite) TestUpdateDedicatedCursor_AdvancesCursor() {
	s.manager.UpdateDefaultLowCursor(50)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.UpdateDefaultLowCursor(200)

	// Before update, dedicated cursor is at 50 — not caught up.
	s.False(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))

	// After update to 200, caught up.
	s.manager.UpdateDedicatedCursor("ns-a", 200)
	s.True(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
}

// EffectiveLowCursor tests

func (s *namespaceIsolationManagerSuite) TestEffectiveLowCursor_NoDedicatedReaders() {
	s.manager.UpdateDefaultLowCursor(100)
	s.Equal(int64(100), s.manager.EffectiveLowCursor())
}

func (s *namespaceIsolationManagerSuite) TestEffectiveLowCursor_ThrottledNotCatching() {
	s.manager.UpdateDefaultLowCursor(50)
	s.manager.ThrottleNamespace("ns-a")

	// Default LOW advances; ns-a is paused (not catching up yet).
	s.manager.UpdateDefaultLowCursor(100)

	// The watermark safe to persist is min(100, 50) = 50.
	s.Equal(int64(50), s.manager.EffectiveLowCursor())
}

func (s *namespaceIsolationManagerSuite) TestEffectiveLowCursor_DedicatedReaderBehindDefaultLow() {
	s.manager.UpdateDefaultLowCursor(50)
	s.manager.ThrottleNamespace("ns-a")
	_, startCursor, _ := s.manager.BeginCatchup(context.Background(), "ns-a")
	s.Equal(int64(50), startCursor)

	// Default LOW advances to 100, scanning [50, 100) and skipping ns-a tasks.
	// ns-a's dedicated reader has not yet sent anything past startCursor (50).
	s.manager.UpdateDefaultLowCursor(100)

	// The watermark safe to persist is min(100, 50) = 50.
	s.Equal(int64(50), s.manager.EffectiveLowCursor())
}

func (s *namespaceIsolationManagerSuite) TestEffectiveLowCursor_DedicatedReaderCaughtUp() {
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.UpdateDedicatedCursor("ns-a", 100)
	s.Equal(int64(100), s.manager.EffectiveLowCursor())
}

func (s *namespaceIsolationManagerSuite) TestEffectiveLowCursor_MultipleReaders_ReturnsMin() {
	s.manager.UpdateDefaultLowCursor(200)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.ThrottleNamespace("ns-b")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-b")
	s.manager.UpdateDedicatedCursor("ns-a", 150)
	s.manager.UpdateDedicatedCursor("ns-b", 100)
	// min(defaultLow=200, ns-a=150, ns-b=100) = 100
	s.Equal(int64(100), s.manager.EffectiveLowCursor())
}

// Sync tests

func (s *namespaceIsolationManagerSuite) TestSync_NewThrottle_ReturnsStart() {
	start, drain := s.manager.Sync([]string{"ns-a", "ns-b"})
	s.ElementsMatch([]string{"ns-a", "ns-b"}, start)
	s.Empty(drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_ExistingThrottle_NoChange() {
	s.manager.ThrottleNamespace("ns-a")
	start, drain := s.manager.Sync([]string{"ns-a"})
	s.Empty(start)
	s.Empty(drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_RemovedFromThrottled_ReturnsDrain() {
	s.manager.ThrottleNamespace("ns-a")
	start, drain := s.manager.Sync([]string{})
	s.Empty(start)
	s.Equal([]string{"ns-a"}, drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_StillBehind_RetainedAcrossAcks() {
	// ns-a is behind and stays in drain list on each ack until caught up.
	s.manager.UpdateDefaultLowCursor(100)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.UpdateDefaultLowCursor(200)

	_, drain := s.manager.Sync([]string{})
	s.Equal([]string{"ns-a"}, drain)
	s.False(s.manager.CheckAndRemoveIfCaughtUp("ns-a")) // not caught up

	// Still in drain list on next ack.
	_, drain = s.manager.Sync([]string{})
	s.Equal([]string{"ns-a"}, drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_Mixed() {
	s.manager.ThrottleNamespace("ns-existing")
	start, drain := s.manager.Sync([]string{"ns-new", "ns-existing"})
	s.Equal([]string{"ns-new"}, start)
	s.Empty(drain)
}

// TestCheckAndRemoveIfCaughtUp_ConcurrentUpdateDefaultLow_NoGap verifies that
// CheckAndRemoveIfCaughtUp and UpdateDefaultLowCursor serialize correctly under
// the mutex. Two orderings are possible; neither creates a gap:
//
//   - CheckAndRemoveIfCaughtUp wins the lock: sees defaultLow=200, cursor=200 ≥ 200,
//     removes ns-a. Default LOW resumes from 200. No gap.
//
//   - UpdateDefaultLowCursor wins the lock: advances to 250 first.
//     CheckAndRemoveIfCaughtUp sees cursor=200 < 250, does NOT remove.
//     ns-a stays excluded until the dedicated reader reaches 250.
func (s *namespaceIsolationManagerSuite) TestCheckAndRemoveIfCaughtUp_ConcurrentUpdateDefaultLow_NoGap() {
	s.manager.UpdateDefaultLowCursor(200)
	s.manager.ThrottleNamespace("ns-a")
	s.manager.BeginCatchup(context.Background(), "ns-a")
	s.manager.UpdateDedicatedCursor("ns-a", 200)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.manager.UpdateDefaultLowCursor(250)
	}()

	removed := s.manager.CheckAndRemoveIfCaughtUp("ns-a")
	wg.Wait()

	if removed {
		// Lock acquired before advance: cursor(200) >= defaultLow(200), removed cleanly.
		s.False(s.manager.IsDefaultLowExcluded("ns-a"))
	} else {
		// Advance won the lock: cursor(200) < defaultLow(250), removal deferred.
		// ns-a stays excluded; dedicated reader must reach 250 before next removal.
		s.True(s.manager.IsDefaultLowExcluded("ns-a"))
		s.manager.UpdateDedicatedCursor("ns-a", 250)
		s.True(s.manager.CheckAndRemoveIfCaughtUp("ns-a"))
	}
}
