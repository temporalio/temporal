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

package persistencetests

import (
	"github.com/stretchr/testify/require"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	// ShardPersistenceSuite contains shard persistence tests
	ShardPersistenceSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (s *ShardPersistenceSuite) SetupSuite() {
}

// SetupTest implementation
func (s *ShardPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *ShardPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// TestGetOrCreateShard tests GetOrCreateShard
func (s *ShardPersistenceSuite) TestGetOrCreateShard() {
	shardID := int32(20)
	owner := "test_get_shard"
	rangeID := int64(131)

	shardInfo, err := s.GetOrCreateShard(shardID, owner, rangeID)
	s.NoError(err)
	s.NotNil(shardInfo)
	s.Equal(shardID, shardInfo.GetShardId())
	s.Equal(owner, shardInfo.Owner)
	s.Equal(rangeID, shardInfo.GetRangeId())
	s.Equal(int32(0), shardInfo.StolenSinceRenew)

	// should not set any info
	shardInfo, err = s.GetOrCreateShard(shardID, "new_owner", 567)
	s.NoError(err)
	s.NotNil(shardInfo)
	s.Equal(shardID, shardInfo.GetShardId())
	s.Equal(owner, shardInfo.Owner)
	s.Equal(rangeID, shardInfo.GetRangeId())
}

// TestUpdateShard test
func (s *ShardPersistenceSuite) TestUpdateShard() {
	shardID := int32(30)
	owner := "test_update_shard"
	rangeID := int64(141)

	shardInfo, err1 := s.GetOrCreateShard(shardID, owner, rangeID)
	s.NoError(err1)
	s.NotNil(shardInfo)
	s.Equal(shardID, shardInfo.GetShardId())
	s.Equal(owner, shardInfo.Owner)
	s.Equal(rangeID, shardInfo.GetRangeId())
	s.Equal(int32(0), shardInfo.StolenSinceRenew)

	updatedOwner := "updatedOwner"
	updatedRangeID := int64(142)
	updatedTransferAckLevel := int64(1000)
	updatedReplicationAckLevel := int64(2000)
	updatedStolenSinceRenew := int32(10)
	updatedInfo := copyShardInfo(shardInfo)
	updatedInfo.Owner = updatedOwner
	updatedInfo.RangeId = updatedRangeID
	updatedInfo.TransferAckLevel = updatedTransferAckLevel
	updatedInfo.ReplicationAckLevel = updatedReplicationAckLevel
	updatedInfo.StolenSinceRenew = updatedStolenSinceRenew
	updatedTimerAckLevel := timestamp.TimeNowPtrUtc()
	updatedInfo.TimerAckLevelTime = updatedTimerAckLevel
	err2 := s.UpdateShard(updatedInfo, shardInfo.GetRangeId())
	s.Nil(err2)

	info1, err3 := s.GetOrCreateShard(shardID, "", 0)
	s.Nil(err3)
	s.NotNil(info1)
	s.Equal(updatedOwner, info1.Owner)
	s.Equal(updatedRangeID, info1.GetRangeId())
	s.Equal(updatedTransferAckLevel, info1.TransferAckLevel)
	s.Equal(updatedReplicationAckLevel, info1.ReplicationAckLevel)
	s.Equal(updatedStolenSinceRenew, info1.StolenSinceRenew)
	info1timerAckLevelTime := info1.TimerAckLevelTime
	s.EqualTimes(*updatedTimerAckLevel, *info1timerAckLevelTime)

	failedUpdateInfo := copyShardInfo(shardInfo)
	failedUpdateInfo.Owner = "failed_owner"
	failedUpdateInfo.TransferAckLevel = int64(4000)
	failedUpdateInfo.ReplicationAckLevel = int64(5000)
	err4 := s.UpdateShard(failedUpdateInfo, shardInfo.GetRangeId())
	s.NotNil(err4)
	s.IsType(&p.ShardOwnershipLostError{}, err4)

	info2, err5 := s.GetOrCreateShard(shardID, "", 0)
	s.Nil(err5)
	s.NotNil(info2)
	s.Equal(updatedOwner, info2.Owner)
	s.Equal(updatedRangeID, info2.GetRangeId())
	s.Equal(updatedTransferAckLevel, info2.TransferAckLevel)
	s.Equal(updatedReplicationAckLevel, info2.ReplicationAckLevel)
	s.Equal(updatedStolenSinceRenew, info2.StolenSinceRenew)

	info1timerAckLevelTime = info1.TimerAckLevelTime
	s.EqualTimes(*updatedTimerAckLevel, *info1timerAckLevelTime)
}

func copyShardInfo(sourceInfo *persistencespb.ShardInfo) *persistencespb.ShardInfo {
	return &persistencespb.ShardInfo{
		ShardId:             sourceInfo.GetShardId(),
		Owner:               sourceInfo.Owner,
		RangeId:             sourceInfo.GetRangeId(),
		TransferAckLevel:    sourceInfo.TransferAckLevel,
		ReplicationAckLevel: sourceInfo.ReplicationAckLevel,
		StolenSinceRenew:    sourceInfo.StolenSinceRenew,
		TimerAckLevelTime:   sourceInfo.TimerAckLevelTime,
		VisibilityAckLevel:  sourceInfo.VisibilityAckLevel,
	}
}
