package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
)

type (
	ShardSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		ShardID int32

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
		Assertions:      require.New(t),
		ProtoAssertions: protorequire.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serializer,
		),
		Logger: logger,
	}
}

func (s *ShardSuite) SetupSuite() {
}

func (s *ShardSuite) TearDownSuite() {
}

func (s *ShardSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.ShardID++
}

func (s *ShardSuite) TearDownTest() {
	s.Cancel()
}

func (s *ShardSuite) TestGetOrCreateShard_Create() {
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(s.ShardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.ProtoEqual(shardInfo, resp.ShardInfo)

}

func (s *ShardSuite) TestGetOrCreateShard_Get() {
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(s.ShardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.ProtoEqual(shardInfo, resp.ShardInfo)

	resp, err = s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: RandomShardInfo(s.ShardID, rand.Int63()),
	})
	s.NoError(err)
	s.ProtoEqual(shardInfo, resp.ShardInfo)
}

func (s *ShardSuite) TestUpdateShard_OwnershipLost() {
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(s.ShardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.ProtoEqual(shardInfo, resp.ShardInfo)

	updateRangeID := rand.Int63()
	updateShardInfo := RandomShardInfo(s.ShardID, rand.Int63())
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       updateShardInfo,
		PreviousRangeID: updateRangeID,
	})
	s.IsType(&p.ShardOwnershipLostError{}, err)

	resp, err = s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.ProtoEqual(shardInfo, resp.ShardInfo)
}

func (s *ShardSuite) TestUpdateShard_Success() {
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(s.ShardID, rangeID)

	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.ProtoEqual(shardInfo, resp.ShardInfo)

	updateShardInfo := RandomShardInfo(s.ShardID, rangeID+1)
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       updateShardInfo,
		PreviousRangeID: rangeID,
	})
	s.NoError(err)

	resp, err = s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID:          s.ShardID,
		InitialShardInfo: shardInfo,
	})
	s.NoError(err)
	s.ProtoEqual(updateShardInfo, resp.ShardInfo)
}
