package replication

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	progressCacheSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		shardContext  *shard.ContextTest
		progressCache ProgressCache
		namespaceID   string
		workflowID    string
		runID         string
	}
)

func TestWorkflowCacheSuite(t *testing.T) {
	s := new(progressCacheSuite)
	suite.Run(t, s)
}

func (s *progressCacheSuite) SetupSuite() {
}

func (s *progressCacheSuite) TearDownSuite() {
}

func (s *progressCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()

	s.shardContext = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)
	s.progressCache = NewProgressCache(s.shardContext.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.namespaceID = tests.NamespaceID.String()
	s.workflowID = uuid.NewString()
	s.runID = uuid.NewString()

}

func (s *progressCacheSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *progressCacheSuite) TestProgressCache() {
	targetClusterID := rand.Int31()
	firstEventID := int64(999)
	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 80,
		TransitionCount:          10,
	}
	versionedTransitions := []*persistencespb.VersionedTransition{versionedTransition}
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(firstEventID, versionedTransition.NamespaceFailoverVersion),
	}
	expected := &ReplicationProgress{
		versionedTransitions:       [][]*persistencespb.VersionedTransition{versionedTransitions},
		eventVersionHistoryItems:   [][]*historyspb.VersionHistoryItem{versionHistoryItems},
		lastVersionTransitionIndex: 0,
	}

	// get non-existing progress
	cachedProgress := s.progressCache.Get(s.runID, targetClusterID)
	s.Nil(cachedProgress)

	err := s.progressCache.Update(s.runID, targetClusterID, versionedTransitions, versionHistoryItems)
	s.Nil(err)

	// get existing progress
	cachedProgress = s.progressCache.Get(s.runID, targetClusterID)
	s.DeepEqual(expected, cachedProgress)

	// update existing versioned transition and version history
	versionedTransitions2 := []*persistencespb.VersionedTransition{
		{
			NamespaceFailoverVersion: 80,
			TransitionCount:          20,
		},
	}
	versionHistoryItems2 := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(firstEventID+1, versionedTransition.NamespaceFailoverVersion),
	}
	err = s.progressCache.Update(s.runID, targetClusterID, versionedTransitions2, versionHistoryItems2)
	s.Nil(err)

	expected2 := &ReplicationProgress{
		versionedTransitions:         [][]*persistencespb.VersionedTransition{versionedTransitions2},
		eventVersionHistoryItems:     [][]*historyspb.VersionHistoryItem{versionHistoryItems2},
		lastVersionTransitionIndex:   0,
		lastEventVersionHistoryIndex: 0,
	}
	cachedProgress = s.progressCache.Get(s.runID, targetClusterID)
	s.DeepEqual(expected2, cachedProgress)

	// add new versioned transition and version history
	versionedTransitions3 := []*persistencespb.VersionedTransition{
		{
			NamespaceFailoverVersion: 90,
			TransitionCount:          15,
		},
	}
	versionHistoryItems3 := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(firstEventID, versionedTransition.NamespaceFailoverVersion),
		versionhistory.NewVersionHistoryItem(firstEventID+1, versionedTransition.NamespaceFailoverVersion+1),
	}
	err = s.progressCache.Update(s.runID, targetClusterID, versionedTransitions3, versionHistoryItems3)
	s.Nil(err)

	expected3 := &ReplicationProgress{
		versionedTransitions:         [][]*persistencespb.VersionedTransition{versionedTransitions2, versionedTransitions3},
		eventVersionHistoryItems:     [][]*historyspb.VersionHistoryItem{versionHistoryItems2, versionHistoryItems3},
		lastVersionTransitionIndex:   1,
		lastEventVersionHistoryIndex: 1,
	}
	cachedProgress = s.progressCache.Get(s.runID, targetClusterID)
	s.DeepEqual(expected3, cachedProgress)

	// noop update: versioned transition and version history are already included in the existing progress
	err = s.progressCache.Update(s.runID, targetClusterID, versionedTransitions, versionHistoryItems)
	s.Nil(err)

	cachedProgress = s.progressCache.Get(s.runID, targetClusterID)
	s.DeepEqual(expected3, cachedProgress)
}

func (s *progressCacheSuite) TestProgressCache_StateAlreadyIncluded() {
	// Covers updateStates branch where incoming versioned transitions are already
	// included in the current versioned transitions (StalenessCheck(transitions, incoming last) == nil).
	targetClusterID := rand.Int31()

	// seed cache with a longer transition history
	seedTransitions := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 80, TransitionCount: 20},
	}
	err := s.progressCache.Update(s.runID, targetClusterID, seedTransitions, nil)
	s.NoError(err)

	expected := &ReplicationProgress{
		versionedTransitions:       [][]*persistencespb.VersionedTransition{seedTransitions},
		lastVersionTransitionIndex: 0,
	}
	s.DeepEqual(expected, s.progressCache.Get(s.runID, targetClusterID))

	// incoming transitions are an older subset (same version, smaller transition count),
	// so they are already included and should be a noop.
	progCacheOlderTransitions := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 80, TransitionCount: 10},
	}
	err = s.progressCache.Update(s.runID, targetClusterID, progCacheOlderTransitions, nil)
	s.NoError(err)
	s.DeepEqual(expected, s.progressCache.Get(s.runID, targetClusterID))
}

func (s *progressCacheSuite) TestProgressCache_EventAlreadyIncluded() {
	// Covers updateEvents branch where incoming version history is already included.
	targetClusterID := rand.Int31()

	seedItems := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 80),
		versionhistory.NewVersionHistoryItem(20, 80),
	}
	err := s.progressCache.Update(s.runID, targetClusterID, nil, seedItems)
	s.NoError(err)

	expected := &ReplicationProgress{
		eventVersionHistoryItems:     [][]*historyspb.VersionHistoryItem{seedItems},
		lastEventVersionHistoryIndex: 0,
	}
	s.DeepEqual(expected, s.progressCache.Get(s.runID, targetClusterID))

	// incoming items are a prefix of the existing items -> already included, noop.
	progCacheSubsetItems := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(5, 80),
	}
	err = s.progressCache.Update(s.runID, targetClusterID, nil, progCacheSubsetItems)
	s.NoError(err)
	s.DeepEqual(expected, s.progressCache.Get(s.runID, targetClusterID))
}

func (s *progressCacheSuite) TestProgressCache_UpdateEventsError() {
	// Covers updateEvents error path (FindLCA returns "no joint point found") and the
	// corresponding error return in Update. Two histories with no common version produce
	// the malformed-history error.
	targetClusterID := rand.Int31()

	seedItems := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 80),
	}
	err := s.progressCache.Update(s.runID, targetClusterID, nil, seedItems)
	s.NoError(err)

	// incoming history shares no version with the existing one. With seed version 80 and
	// incoming version 81, the LCA loop exhausts without a joint point and returns an error,
	// which Update surfaces.
	progCacheErrItems := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 81),
	}
	err = s.progressCache.Update(s.runID, targetClusterID, nil, progCacheErrItems)
	s.Error(err)
}

func (s *progressCacheSuite) TestProgressCache_LastSyncedTransitionAndSent() {
	// Covers LastSyncedTransition and VersionedTransitionSent on both nil and populated
	// ReplicationProgress.
	var progCacheNilProgress *ReplicationProgress
	s.Nil(progCacheNilProgress.LastSyncedTransition())
	s.False(progCacheNilProgress.VersionedTransitionSent(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 80,
		TransitionCount:          10,
	}))

	// empty progress
	progCacheEmpty := &ReplicationProgress{}
	s.Nil(progCacheEmpty.LastSyncedTransition())

	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 80,
		TransitionCount:          10,
	}
	progCachePopulated := &ReplicationProgress{
		versionedTransitions:       [][]*persistencespb.VersionedTransition{{versionedTransition}},
		lastVersionTransitionIndex: 0,
	}
	s.ProtoEqual(versionedTransition, progCachePopulated.LastSyncedTransition())
	s.True(progCachePopulated.VersionedTransitionSent(versionedTransition))
	s.False(progCachePopulated.VersionedTransitionSent(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 90,
		TransitionCount:          1,
	}))

	// out-of-range index returns nil
	progCacheBadIndex := &ReplicationProgress{
		versionedTransitions:       [][]*persistencespb.VersionedTransition{{versionedTransition}},
		lastVersionTransitionIndex: 5,
	}
	s.Nil(progCacheBadIndex.LastSyncedTransition())
}

func TestProgressCacheUsesDistinctMetricsTag(t *testing.T) {
	// Verify that the progress cache uses its own cache_type tag value
	// ("replication_progress") rather than the mutable state cache's tag value
	// ("mutablestate"), preventing Prometheus gauge collision. See #9600.
	ctrl := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	defer ctrl.Finish()
	defer mockShard.StopForTest()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()

	NewProgressCache(mockShard.GetConfig(), mockShard.GetLogger(), metricsHandler)

	snapshot := capture.Snapshot()
	cacheSizeRecordings := snapshot[metrics.CacheSize.Name()]
	require.NotEmpty(t, cacheSizeRecordings, "expected cache_size metric to be recorded")

	// Verify the cache_type tag is "replication_progress", not "mutablestate"
	found := false
	for _, recording := range cacheSizeRecordings {
		if tag, ok := recording.Tags[metrics.CacheTypeTagName]; ok {
			require.Equal(t, metrics.ReplicationProgressCacheTypeTagValue, tag,
				"progress cache should use ReplicationProgressCacheTypeTagValue, not MutableStateCacheTypeTagValue")
			found = true
		}
	}
	require.True(t, found, "expected cache_size metric with cache_type tag")
}
