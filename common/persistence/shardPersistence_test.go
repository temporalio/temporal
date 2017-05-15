// Copyright (c) 2017 Uber Technologies, Inc.
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

package persistence

import (
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
)

type (
	shardPersistenceSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestShardPersistenceSuite(t *testing.T) {
	s := new(shardPersistenceSuite)
	suite.Run(t, s)
}

func (s *shardPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.SetupWorkflowStore()
}

func (s *shardPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *shardPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *shardPersistenceSuite) TestCreateShard() {
	err0 := s.CreateShard(19, "test_create_shard1", 123)
	s.Nil(err0, "No error expected.")

	err1 := s.CreateShard(19, "test_create_shard2", 124)
	s.NotNil(err1, "expected non nil error.")
	s.IsType(&ShardAlreadyExistError{}, err1)
	log.Infof("CreateShard failed with error: %v", err1)
}

func (s *shardPersistenceSuite) TestGetShard() {
	shardID := 20
	owner := "test_get_shard"
	rangeID := int64(131)
	err0 := s.CreateShard(shardID, owner, rangeID)
	s.Nil(err0, "No error expected.")

	shardInfo, err1 := s.GetShard(shardID)
	s.Nil(err1)
	s.NotNil(shardInfo)
	s.Equal(shardID, shardInfo.ShardID)
	s.Equal(owner, shardInfo.Owner)
	s.Equal(rangeID, shardInfo.RangeID)
	s.Equal(0, shardInfo.StolenSinceRenew)

	_, err2 := s.GetShard(4766)
	s.NotNil(err2)
	s.IsType(&gen.EntityNotExistsError{}, err2)
	log.Infof("GetShard failed with error: %v", err2)
}

func (s *shardPersistenceSuite) TestUpdateShard() {
	shardID := 30
	owner := "test_update_shard"
	rangeID := int64(141)
	err0 := s.CreateShard(shardID, owner, rangeID)
	s.Nil(err0, "No error expected.")

	shardInfo, err1 := s.GetShard(shardID)
	s.Nil(err1)
	s.NotNil(shardInfo)
	s.Equal(shardID, shardInfo.ShardID)
	s.Equal(owner, shardInfo.Owner)
	s.Equal(rangeID, shardInfo.RangeID)
	s.Equal(0, shardInfo.StolenSinceRenew)

	updatedOwner := "updatedOwner"
	updatedRangeID := int64(142)
	updatedTransferAckLevel := int64(1000)
	updatedStolenSinceRenew := 10
	updatedInfo := copyShardInfo(shardInfo)
	updatedInfo.Owner = updatedOwner
	updatedInfo.RangeID = updatedRangeID
	updatedInfo.TransferAckLevel = updatedTransferAckLevel
	updatedInfo.StolenSinceRenew = updatedStolenSinceRenew
	err2 := s.UpdateShard(updatedInfo, shardInfo.RangeID)
	s.Nil(err2)

	info1, err3 := s.GetShard(shardID)
	s.Nil(err3)
	s.NotNil(info1)
	s.Equal(updatedOwner, info1.Owner)
	s.Equal(updatedRangeID, info1.RangeID)
	s.Equal(updatedTransferAckLevel, info1.TransferAckLevel)
	s.Equal(updatedStolenSinceRenew, info1.StolenSinceRenew)

	failedUpdateInfo := copyShardInfo(shardInfo)
	failedUpdateInfo.Owner = "failed_owner"
	err4 := s.UpdateShard(failedUpdateInfo, shardInfo.RangeID)
	s.NotNil(err4)
	s.IsType(&ShardOwnershipLostError{}, err4)
	log.Infof("Update shard failed with error: %v", err4)

	info2, err5 := s.GetShard(shardID)
	s.Nil(err5)
	s.NotNil(info2)
	s.Equal(updatedOwner, info2.Owner)
	s.Equal(updatedRangeID, info2.RangeID)
	s.Equal(updatedTransferAckLevel, info2.TransferAckLevel)
	s.Equal(updatedStolenSinceRenew, info2.StolenSinceRenew)
}

func copyShardInfo(sourceInfo *ShardInfo) *ShardInfo {
	return &ShardInfo{
		ShardID:          sourceInfo.ShardID,
		Owner:            sourceInfo.Owner,
		RangeID:          sourceInfo.RangeID,
		TransferAckLevel: sourceInfo.TransferAckLevel,
		StolenSinceRenew: sourceInfo.StolenSinceRenew,
	}
}
