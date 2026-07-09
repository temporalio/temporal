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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/mock/gomock"
)

type (
	getTasksSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *historyi.MockShardContext
		mockClusterMetadata *cluster.MockMetadata
		mockAckManager      *replication.MockAckManager
	}
)

func TestGetTasksSuite(t *testing.T) {
	suite.Run(t, new(getTasksSuite))
}

func (s *getTasksSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockShard = historyi.NewMockShardContext(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockAckManager = replication.NewMockAckManager(s.controller)

	s.mockShard.EXPECT().GetClusterMetadata().Return(s.mockClusterMetadata).AnyTimes()
	s.mockShard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
}

// TestUnknownPollingCluster verifies that an error is returned immediately when
// the polling cluster is not present in the cluster info map.
func (s *getTasksSuite) TestUnknownPollingCluster() {
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			ShardCount:             4,
		},
	})

	_, err := GetTasks(
		context.Background(),
		s.mockShard,
		s.mockAckManager,
		"unknown-cluster",
		persistence.EmptyQueueMessageID+1,
		time.Now(),
		0,
	)

	s.Error(err)
}

// TestAckMessageIDEmpty_SkipsReaderStateUpdate verifies that when ackMessageID is
// EmptyQueueMessageID the reader-state update block is skipped entirely.
func (s *getTasksSuite) TestAckMessageIDEmpty_SkipsReaderStateUpdate() {
	clusterInfo := map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			ShardCount:             4,
		},
	}
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(clusterInfo)

	expectedMessages := &replicationspb.ReplicationMessages{LastRetrievedMessageId: 42}
	s.mockAckManager.EXPECT().
		GetTasks(gomock.Any(), cluster.TestAlternativeClusterName, int64(0)).
		Return(expectedMessages, nil)

	msgs, err := GetTasks(
		context.Background(),
		s.mockShard,
		s.mockAckManager,
		cluster.TestAlternativeClusterName,
		persistence.EmptyQueueMessageID,
		time.Now(),
		0,
	)

	s.NoError(err)
	s.Equal(expectedMessages, msgs)
	// UpdateReplicationQueueReaderState and UpdateRemoteClusterInfo must NOT be called —
	// verified implicitly by gomock (no EXPECT set for them).
}

// TestEqualShardCounts_SingleReaderStateUpdate verifies that when source and target
// shard counts are equal, MapShardID returns a single shard ID, resulting in exactly
// one UpdateReplicationQueueReaderState call.
func (s *getTasksSuite) TestEqualShardCounts_SingleReaderStateUpdate() {
	const (
		sourceShardID   = int32(3)
		shardCount      = int32(4)
		ackMsgID        = int64(100)
		pollingFailover = cluster.TestAlternativeClusterInitialFailoverVersion
	)

	clusterInfo := map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			ShardCount:             shardCount,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: pollingFailover,
			ShardCount:             shardCount,
		},
	}

	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(clusterInfo)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	s.mockShard.EXPECT().GetShardID().Return(sourceShardID)

	// MapShardID(4, 4, 3) → [3]; readerID computed for shard 3.
	expectedReaderID := shard.ReplicationReaderIDFromClusterShardID(pollingFailover, sourceShardID)
	s.mockShard.EXPECT().
		UpdateReplicationQueueReaderState(expectedReaderID, gomock.Any()).
		Return(nil).
		Times(1)
	s.mockShard.EXPECT().
		UpdateRemoteClusterInfo(cluster.TestAlternativeClusterName, ackMsgID, gomock.Any()).
		Times(1)

	expectedMessages := &replicationspb.ReplicationMessages{}
	s.mockAckManager.EXPECT().
		GetTasks(gomock.Any(), cluster.TestAlternativeClusterName, int64(0)).
		Return(expectedMessages, nil)

	msgs, err := GetTasks(
		context.Background(),
		s.mockShard,
		s.mockAckManager,
		cluster.TestAlternativeClusterName,
		ackMsgID,
		time.Now(),
		0,
	)

	s.NoError(err)
	s.Equal(expectedMessages, msgs)
}

// TestScaleOut_DoubleShardCount_TwoReaderStateUpdates verifies the new behaviour:
// when the polling cluster has 2× the shard count of the current cluster,
// MapShardID returns two target shard IDs and UpdateReplicationQueueReaderState
// is called once per target shard.
func (s *getTasksSuite) TestScaleOut_DoubleShardCount_TwoReaderStateUpdates() {
	const (
		sourceShardID   = int32(3)
		currentShards   = int32(4)
		pollingShards   = int32(8) // 2× current → scale-out
		ackMsgID        = int64(200)
		pollingFailover = cluster.TestAlternativeClusterInitialFailoverVersion
	)

	clusterInfo := map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			ShardCount:             currentShards,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: pollingFailover,
			ShardCount:             pollingShards,
		},
	}

	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(clusterInfo)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	s.mockShard.EXPECT().GetShardID().Return(sourceShardID)

	// MapShardID(4, 8, 3):
	//   sourceShardID - 1 = 2
	//   ratio = 8 / 4 = 2
	//   targetIDs = [2 + 0*4 + 1, 2 + 1*4 + 1] = [3, 7]
	readerID3 := shard.ReplicationReaderIDFromClusterShardID(pollingFailover, 3)
	readerID7 := shard.ReplicationReaderIDFromClusterShardID(pollingFailover, 7)

	s.mockShard.EXPECT().
		UpdateReplicationQueueReaderState(readerID3, gomock.Any()).
		Return(nil).
		Times(1)
	s.mockShard.EXPECT().
		UpdateReplicationQueueReaderState(readerID7, gomock.Any()).
		Return(nil).
		Times(1)
	s.mockShard.EXPECT().
		UpdateRemoteClusterInfo(cluster.TestAlternativeClusterName, ackMsgID, gomock.Any()).
		Times(1)

	expectedMessages := &replicationspb.ReplicationMessages{}
	s.mockAckManager.EXPECT().
		GetTasks(gomock.Any(), cluster.TestAlternativeClusterName, int64(0)).
		Return(expectedMessages, nil)

	msgs, err := GetTasks(
		context.Background(),
		s.mockShard,
		s.mockAckManager,
		cluster.TestAlternativeClusterName,
		ackMsgID,
		time.Now(),
		0,
	)

	s.NoError(err)
	s.Equal(expectedMessages, msgs)
}

// TestAckManagerError_PropagatesError verifies that an error from
// replicationAckMgr.GetTasks is surfaced to the caller unchanged.
func (s *getTasksSuite) TestAckManagerError_PropagatesError() {
	clusterInfo := map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			ShardCount:             4,
		},
	}

	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(clusterInfo)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	s.mockShard.EXPECT().GetShardID().Return(int32(1))
	s.mockShard.EXPECT().
		UpdateReplicationQueueReaderState(gomock.Any(), gomock.Any()).
		Return(nil)
	s.mockShard.EXPECT().
		UpdateRemoteClusterInfo(gomock.Any(), gomock.Any(), gomock.Any())

	ackMgrErr := errors.New("ack manager failure")
	s.mockAckManager.EXPECT().
		GetTasks(gomock.Any(), cluster.TestAlternativeClusterName, int64(0)).
		Return(nil, ackMgrErr)

	_, err := GetTasks(
		context.Background(),
		s.mockShard,
		s.mockAckManager,
		cluster.TestAlternativeClusterName,
		int64(50),
		time.Now(),
		0,
	)

	s.ErrorIs(err, ackMgrErr)
}
