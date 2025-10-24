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
