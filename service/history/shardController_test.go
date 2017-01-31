package history

import (
	"fmt"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	"errors"

	"code.uber.internal/devexp/minions/common/membership"
	mmocks "code.uber.internal/devexp/minions/common/mocks"
	"code.uber.internal/devexp/minions/common/persistence"
)

type (
	shardControllerSuite struct {
		suite.Suite
		hostInfo            *membership.HostInfo
		controller          *shardController
		mockShardManager    *mmocks.ShardManager
		mockServiceResolver *mmocks.ServiceResolver
		mockEngineFactory   *MockHistoryEngineFactory
		logger              bark.Logger
	}
)

func TestShardControllerSuite(t *testing.T) {
	s := new(shardControllerSuite)
	suite.Run(t, s)
}

func (s *shardControllerSuite) SetupTest() {
	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.hostInfo = membership.NewHostInfo("shardController-host-test", nil)
	s.mockShardManager = &mmocks.ShardManager{}
	s.mockServiceResolver = &mmocks.ServiceResolver{}
	s.mockEngineFactory = &MockHistoryEngineFactory{}
	s.controller = newShardController(1, s.hostInfo, s.mockServiceResolver, s.mockShardManager, s.mockEngineFactory,
		s.logger)
}

func (s *shardControllerSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
	s.mockServiceResolver.AssertExpectations(s.T())
	s.mockEngineFactory.AssertExpectations(s.T())
}

func (s *shardControllerSuite) TestAcquireShardSuccess() {
	numShards := 10
	s.controller.numberOfShards = numShards
	myShards := []int{}
	for shardID := 0; shardID < numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			mockEngine := &MockHistoryEngine{}
			mockEngine.On("Start").Return().Once()
			s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
			s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(mockEngine).Once()
			s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
				&persistence.GetShardResponse{
					ShardInfo: &persistence.ShardInfo{
						ShardID: shardID,
						Owner:   s.hostInfo.Identity(),
						RangeID: 5,
					},
				}, nil).Once()
			s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
				ShardInfo: &persistence.ShardInfo{
					ShardID:          shardID,
					Owner:            s.hostInfo.Identity(),
					RangeID:          6,
					StolenSinceRenew: 1,
					TransferAckLevel: 0,
				},
				PreviousRangeID: 5,
			}).Return(nil).Once()
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.On("Lookup", string(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil).Once()
		}
	}

	s.controller.acquireShards()
	count := 0
	for _, shardID := range myShards {
		s.NotNil(s.controller.getEngineForShard(shardID))
		count++
	}
	s.Equal(3, count)
}

func (s *shardControllerSuite) TestAcquireShardLookupFailure() {
	numShards := 2
	s.controller.numberOfShards = numShards
	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(nil, errors.New("ring failure")).Once()
	}

	s.controller.acquireShards()
	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(nil, errors.New("ring failure")).Once()
		s.Nil(s.controller.getEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestAcquireShardRenewSuccess() {
	numShards := 2
	s.controller.numberOfShards = numShards
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := &MockHistoryEngine{}
		mockEngine.On("Start").Return().Once()
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
		s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(mockEngine).Once()
		s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
			&persistence.GetShardResponse{
				ShardInfo: &persistence.ShardInfo{
					ShardID: shardID,
					Owner:   s.hostInfo.Identity(),
					RangeID: 5,
				},
			}, nil).Once()
		s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
			ShardInfo: &persistence.ShardInfo{
				ShardID:          shardID,
				Owner:            s.hostInfo.Identity(),
				RangeID:          6,
				StolenSinceRenew: 1,
				TransferAckLevel: 0,
			},
			PreviousRangeID: 5,
		}).Return(nil).Once()
	}

	s.controller.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Once()
	}
	s.controller.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.NotNil(s.controller.getEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestAcquireShardRenewLookupFailed() {
	numShards := 2
	s.controller.numberOfShards = numShards
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := &MockHistoryEngine{}
		mockEngine.On("Start").Return().Once()
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
		s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(mockEngine).Once()
		s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
			&persistence.GetShardResponse{
				ShardInfo: &persistence.ShardInfo{
					ShardID: shardID,
					Owner:   s.hostInfo.Identity(),
					RangeID: 5,
				},
			}, nil).Once()
		s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
			ShardInfo: &persistence.ShardInfo{
				ShardID:          shardID,
				Owner:            s.hostInfo.Identity(),
				RangeID:          6,
				StolenSinceRenew: 1,
				TransferAckLevel: 0,
			},
			PreviousRangeID: 5,
		}).Return(nil).Once()
	}

	s.controller.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(nil, errors.New("ring failure")).Once()
	}
	s.controller.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.NotNil(s.controller.getEngineForShard(shardID))
	}
}
