package replication

import (
	"context"
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
	ctx, startCursor, ok := s.manager.ThrottleNamespace(context.Background(), "ns-a")
	s.True(ok)
	s.NotNil(ctx)
	s.Equal(int64(100), startCursor)
	s.True(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestThrottle_AlreadyThrottled_ReturnsFalse() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	_, _, ok := s.manager.ThrottleNamespace(context.Background(), "ns-a")
	s.False(ok)
}

func (s *namespaceIsolationManagerSuite) TestBeginDrain_SetsDrainTarget() {
	s.manager.UpdateDefaultLowCursor(200)
	s.manager.ThrottleNamespace(context.Background(), "ns-a")

	// Advance cursor before draining
	s.manager.UpdateDefaultLowCursor(300)
	drainAt := s.manager.BeginDrain("ns-a")
	s.Equal(int64(300), drainAt)

	target, draining := s.manager.DrainTarget("ns-a")
	s.True(draining)
	s.Equal(int64(300), target)
}

func (s *namespaceIsolationManagerSuite) TestBeginDrain_NotFound_ReturnsNegOne() {
	result := s.manager.BeginDrain("ns-missing")
	s.Equal(int64(-1), result)
}

func (s *namespaceIsolationManagerSuite) TestDrainTarget_NotThrottled() {
	_, draining := s.manager.DrainTarget("ns-a")
	s.False(draining)
}

func (s *namespaceIsolationManagerSuite) TestDrainTarget_ActivelyThrottled_NotDraining() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	_, draining := s.manager.DrainTarget("ns-a")
	s.False(draining)
}

func (s *namespaceIsolationManagerSuite) TestRemove_AllowsDefaultLowAgain() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	s.True(s.manager.IsDefaultLowExcluded("ns-a"))
	s.manager.Remove("ns-a")
	s.False(s.manager.IsDefaultLowExcluded("ns-a"))
}

func (s *namespaceIsolationManagerSuite) TestRemove_CancelsContext() {
	ctx, _, _ := s.manager.ThrottleNamespace(context.Background(), "ns-a")
	s.manager.Remove("ns-a")
	s.Error(ctx.Err())
}

func (s *namespaceIsolationManagerSuite) TestRemove_Idempotent() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	s.manager.Remove("ns-a")
	s.manager.Remove("ns-a") // should not panic
}

// Sync tests

func (s *namespaceIsolationManagerSuite) TestSync_NewThrottle_ReturnsStart() {
	start, drain := s.manager.Sync([]string{"ns-a", "ns-b"})
	s.ElementsMatch([]string{"ns-a", "ns-b"}, start)
	s.Empty(drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_ExistingThrottle_NoChange() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	start, drain := s.manager.Sync([]string{"ns-a"})
	s.Empty(start)
	s.Empty(drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_RemovedFromThrottled_ReturnsDrain() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	start, drain := s.manager.Sync([]string{})
	s.Empty(start)
	s.Equal([]string{"ns-a"}, drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_AlreadyDraining_NotReturnedInDrain() {
	s.manager.ThrottleNamespace(context.Background(), "ns-a")
	s.manager.BeginDrain("ns-a")
	// ns-a is already draining; removing from throttled set should not re-drain
	_, drain := s.manager.Sync([]string{})
	s.Empty(drain)
}

func (s *namespaceIsolationManagerSuite) TestSync_Mixed() {
	s.manager.ThrottleNamespace(context.Background(), "ns-existing")
	start, drain := s.manager.Sync([]string{"ns-new", "ns-existing"})
	s.Equal([]string{"ns-new"}, start)
	s.Empty(drain)
}
