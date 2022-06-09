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

package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	ShardSuite struct {
		suite.Suite
		*require.Assertions

		ShardManager p.ShardManager
		Logger       log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}
)

func NewShardSuite(
	t *testing.T,
	shardStore p.ShardStore,
	serializer serialization.Serializer,
	logger log.Logger,
) *ShardSuite {
	return &ShardSuite{
		Assertions: require.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serializer,
		),
		Logger: logger,
	}
}

func (s *ShardSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *ShardSuite) TearDownSuite() {

}

func (s *ShardSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), time.Second*30)
}

func (s *ShardSuite) TearDownTest() {
	s.Cancel()
}

func (s *ShardSuite) TestGetOrCreateShard_Create() {
	shardID := int32(1)
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(shardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.Equal(shardInfo, resp.ShardInfo)

}

func (s *ShardSuite) TestGetOrCreateShard_Get() {
	shardID := int32(2)
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(shardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.Equal(shardInfo, resp.ShardInfo)

	resp, err = s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: RandomShardInfo(shardID, rand.Int63()),
	})
	s.NoError(err)
	s.Equal(shardInfo, resp.ShardInfo)
}

func (s *ShardSuite) TestUpdateShard_OwnershipLost() {
	shardID := int32(3)
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(shardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.Equal(shardInfo, resp.ShardInfo)

	updateRangeID := rand.Int63()
	updateShardInfo := RandomShardInfo(shardID, rand.Int63())
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       updateShardInfo,
		PreviousRangeID: updateRangeID,
	})
	s.IsType(&p.ShardOwnershipLostError{}, err)

	resp, err = s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.Equal(shardInfo, resp.ShardInfo)
}

func (s *ShardSuite) TestUpdateShard_Success() {
	shardID := int32(4)
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(shardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.Equal(shardInfo, resp.ShardInfo)

	updateShardInfo := RandomShardInfo(shardID, rangeID+1)
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       updateShardInfo,
		PreviousRangeID: rangeID,
	})
	s.NoError(err)

	resp, err = s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.Equal(updateShardInfo, resp.ShardInfo)
}
