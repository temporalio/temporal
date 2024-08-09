// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package replication

import (
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	commonpb "go.temporal.io/api/common/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	progressCacheSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		shardContext  *shard.ContextTest
		progressCache ProgressCache
		namespaceID   string
		workflowID    string
		runID         string
		clusterID     int32
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
	s.workflowID = uuid.New()
	s.runID = uuid.New()
	s.clusterID = rand.Int31()
}

func (s *progressCacheSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *progressCacheSuite) TestProgressCache() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}

	firstEventID := int64(999)
	versionedTransition := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 80}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 2, 3},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: firstEventID,
				Version: versionedTransition.NamespaceFailoverVersion,
			},
		},
	}

	// Get: cluster not exist
	cachedProgress, cacheHit := s.progressCache.Get(
		s.shardContext,
		namespace.ID(s.namespaceID),
		execution,
		s.clusterID,
		versionHistory,
	)
	s.False(cacheHit)
	s.Nil(cachedProgress)

	// Put
	err := s.progressCache.Put(
		s.shardContext,
		namespace.ID(s.namespaceID),
		execution,
		s.clusterID,
		versionHistory,
		versionedTransition,
	)
	s.NoError(err)

	// Get exist
	cachedProgress, cacheHit = s.progressCache.Get(
		s.shardContext,
		namespace.ID(s.namespaceID),
		execution,
		s.clusterID,
		versionHistory,
	)
	s.True(cacheHit)
	s.Equal(cachedProgress, &ReplicationProgress{
		versionedTransition:      versionedTransition,
		eventVersionHistoryItems: versionHistory.GetItems(),
	})

	// Get: branch not exist
	versionHistory2 := &historyspb.VersionHistory{
		BranchToken: []byte{4, 5, 6},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: firstEventID + 1,
				Version: versionedTransition.NamespaceFailoverVersion + 1,
			},
		},
	}
	cachedProgress, cacheHit = s.progressCache.Get(
		s.shardContext,
		namespace.ID(s.namespaceID),
		execution,
		s.clusterID,
		versionHistory2,
	)
	s.False(cacheHit)
	s.Nil(cachedProgress)
}
