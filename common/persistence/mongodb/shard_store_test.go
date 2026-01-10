package mongodb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb"
)

type ShardStoreSuite struct {
	suite.Suite
	factory    *mongodb.Factory
	shardStore persistence.ShardStore
	ctx        context.Context
	cancel     context.CancelFunc
	dbName     string
}

func TestShardStoreSuite(t *testing.T) {
	suite.Run(t, new(ShardStoreSuite))
}

func (s *ShardStoreSuite) SetupSuite() {
	s.dbName = fmt.Sprintf("temporal_test_shard_%d", time.Now().UnixNano())
	cfg := newMongoTestConfig(s.dbName)
	cfg.ConnectTimeout = 10 * time.Second

	logger := log.NewTestLogger()
	var err error
	s.factory, err = mongodb.NewFactory(cfg, "test-cluster", logger, metrics.NoopMetricsHandler)
	if err != nil {
		s.T().Skipf("Skipping test suite: %v", err)
	}
	s.Require().NoError(err)

	s.shardStore, err = s.factory.NewShardStore()
	s.Require().NoError(err)
}

func (s *ShardStoreSuite) TearDownSuite() {
	if s.factory != nil {
		s.factory.Close()
	}
}

func (s *ShardStoreSuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second)
}

func (s *ShardStoreSuite) TearDownTest() {
	s.cancel()
}

func (s *ShardStoreSuite) TestGetOrCreateShard() {
	shardID := int32(1)
	rangeID := int64(10)
	shardInfo := &commonpb.DataBlob{
		Data:         []byte("test-shard-data"),
		EncodingType: 1,
	}

	// Test Create
	resp, err := s.shardStore.GetOrCreateShard(s.ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return rangeID, shardInfo, nil
		},
		LifecycleContext: s.ctx,
	})
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.Require().Equal(shardInfo.Data, resp.ShardInfo.Data)

	// Test Get (should return existing)
	resp, err = s.shardStore.GetOrCreateShard(s.ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return rangeID + 1, shardInfo, nil // Should not be called/used
		},
		LifecycleContext: s.ctx,
	})
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.Require().Equal(shardInfo.Data, resp.ShardInfo.Data)
}

func (s *ShardStoreSuite) TestUpdateShard() {
	shardID := int32(2)
	rangeID := int64(20)
	shardInfo := &commonpb.DataBlob{
		Data:         []byte("test-shard-data-2"),
		EncodingType: 1,
	}

	// Create first
	_, err := s.shardStore.GetOrCreateShard(s.ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return rangeID, shardInfo, nil
		},
		LifecycleContext: s.ctx,
	})
	s.Require().NoError(err)

	// Update
	newRangeID := rangeID + 1
	newShardInfo := &commonpb.DataBlob{
		Data:         []byte("updated-shard-data"),
		EncodingType: 1,
	}

	err = s.shardStore.UpdateShard(s.ctx, &persistence.InternalUpdateShardRequest{
		ShardID:         shardID,
		RangeID:         newRangeID,
		Owner:           "test-owner",
		ShardInfo:       newShardInfo,
		PreviousRangeID: rangeID,
	})
	s.Require().NoError(err)

	// Verify Update
	resp, err := s.shardStore.GetOrCreateShard(s.ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 0, nil, nil
		},
		LifecycleContext: s.ctx,
	})
	s.Require().NoError(err)
	s.Require().Equal(newShardInfo.Data, resp.ShardInfo.Data)
}

func (s *ShardStoreSuite) TestAssertShardOwnership() {
	shardID := int32(3)
	rangeID := int64(30)
	shardInfo := &commonpb.DataBlob{
		Data:         []byte("test-shard-data-3"),
		EncodingType: 1,
	}

	// Create first
	_, err := s.shardStore.GetOrCreateShard(s.ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return rangeID, shardInfo, nil
		},
		LifecycleContext: s.ctx,
	})
	s.Require().NoError(err)

	// Assert Ownership Success
	err = s.shardStore.AssertShardOwnership(s.ctx, &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: rangeID,
	})
	s.Require().NoError(err)

	// Assert Ownership Failure
	// Note: AssertShardOwnership is not implemented in MongoDB (nor SQL/Cassandra), so it returns nil.
	err = s.shardStore.AssertShardOwnership(s.ctx, &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: rangeID + 1,
	})
	s.Require().NoError(err)
}
