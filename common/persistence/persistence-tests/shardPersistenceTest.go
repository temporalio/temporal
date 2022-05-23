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
	"context"
	"time"

	"github.com/stretchr/testify/require"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// ShardPersistenceSuite contains shard persistence tests
	ShardPersistenceSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation
func (s *ShardPersistenceSuite) SetupSuite() {
}

// SetupTest implementation
func (s *ShardPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*30)
}

// TearDownTest implementation
func (s *ShardPersistenceSuite) TearDownTest() {
	s.cancel()
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

	shardInfo, err := s.GetOrCreateShard(s.ctx, shardID, owner, rangeID)
	s.NoError(err)
	s.NotNil(shardInfo)
	s.Equal(shardID, shardInfo.GetShardId())
	s.Equal(owner, shardInfo.Owner)
	s.Equal(rangeID, shardInfo.GetRangeId())
	s.Equal(int32(0), shardInfo.StolenSinceRenew)

	// should not set any info
	shardInfo, err = s.GetOrCreateShard(s.ctx, shardID, "new_owner", 567)
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

	shardInfo, err1 := s.GetOrCreateShard(s.ctx, shardID, owner, rangeID)
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
	updatedInfo.QueueAckLevels = make(map[int32]*persistencespb.QueueAckLevel)
	updatedInfo.QueueAckLevels[tasks.CategoryTransfer.ID()] = &persistencespb.QueueAckLevel{
		AckLevel: updatedTransferAckLevel,
	}
	updatedInfo.QueueAckLevels[tasks.CategoryReplication.ID()] = &persistencespb.QueueAckLevel{
		AckLevel: updatedReplicationAckLevel,
	}
	updatedInfo.StolenSinceRenew = updatedStolenSinceRenew
	updatedTimerAckLevel := time.Now().UTC()
	updatedInfo.QueueAckLevels[tasks.CategoryTimer.ID()] = &persistencespb.QueueAckLevel{
		AckLevel: updatedTimerAckLevel.UnixNano(),
	}
	err2 := s.UpdateShard(s.ctx, updatedInfo, shardInfo.GetRangeId())
	s.Nil(err2)

	info1, err3 := s.GetOrCreateShard(s.ctx, shardID, "", 0)
	s.Nil(err3)
	s.NotNil(info1)
	s.Equal(updatedOwner, info1.Owner)
	s.Equal(updatedRangeID, info1.GetRangeId())
	s.Equal(updatedTransferAckLevel, info1.QueueAckLevels[tasks.CategoryTransfer.ID()].AckLevel)
	s.Equal(updatedReplicationAckLevel, info1.QueueAckLevels[tasks.CategoryReplication.ID()].AckLevel)
	s.Equal(updatedStolenSinceRenew, info1.StolenSinceRenew)
	s.EqualTimes(updatedTimerAckLevel, timestamp.UnixOrZeroTime(info1.QueueAckLevels[tasks.CategoryTimer.ID()].AckLevel))

	failedUpdateInfo := copyShardInfo(shardInfo)
	failedUpdateInfo.Owner = "failed_owner"
	failedUpdateInfo.QueueAckLevels = make(map[int32]*persistencespb.QueueAckLevel)
	failedUpdateInfo.QueueAckLevels[tasks.CategoryTransfer.ID()] = &persistencespb.QueueAckLevel{
		AckLevel: int64(4000),
	}
	failedUpdateInfo.QueueAckLevels[tasks.CategoryReplication.ID()] = &persistencespb.QueueAckLevel{
		AckLevel: int64(5000),
	}
	err4 := s.UpdateShard(s.ctx, failedUpdateInfo, shardInfo.GetRangeId())
	s.NotNil(err4)
	s.IsType(&p.ShardOwnershipLostError{}, err4)

	info2, err5 := s.GetOrCreateShard(s.ctx, shardID, "", 0)
	s.Nil(err5)
	s.NotNil(info2)
	s.Equal(updatedOwner, info2.Owner)
	s.Equal(updatedRangeID, info2.GetRangeId())
	s.Equal(updatedTransferAckLevel, info2.QueueAckLevels[tasks.CategoryTransfer.ID()].AckLevel)
	s.Equal(updatedReplicationAckLevel, info2.QueueAckLevels[tasks.CategoryReplication.ID()].AckLevel)
	s.Equal(updatedStolenSinceRenew, info2.StolenSinceRenew)
}

func copyShardInfo(sourceInfo *persistencespb.ShardInfo) *persistencespb.ShardInfo {
	return &persistencespb.ShardInfo{
		ShardId:          sourceInfo.GetShardId(),
		Owner:            sourceInfo.Owner,
		RangeId:          sourceInfo.GetRangeId(),
		StolenSinceRenew: sourceInfo.StolenSinceRenew,
		QueueAckLevels:   sourceInfo.QueueAckLevels,
	}
}
