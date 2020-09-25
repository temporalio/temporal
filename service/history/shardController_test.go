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

package history

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/convert"

	"go.temporal.io/server/api/persistenceblobs/v1"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	mmocks "go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	shardControllerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockHistoryEngine   *MockEngine
		mockClusterMetadata *cluster.MockMetadata
		mockServiceResolver *membership.MockServiceResolver

		hostInfo          *membership.HostInfo
		mockShardManager  *mmocks.ShardManager
		mockEngineFactory *MockHistoryEngineFactory

		config          *Config
		logger          log.Logger
		shardController *shardController
	}
)

func TestShardControllerSuite(t *testing.T) {
	s := new(shardControllerSuite)
	suite.Run(t, s)
}

func (s *shardControllerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockHistoryEngine = NewMockEngine(s.controller)

	s.mockEngineFactory = &MockHistoryEngineFactory{}
	s.mockShardManager = s.mockResource.ShardMgr
	s.mockServiceResolver = s.mockResource.HistoryServiceResolver
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.hostInfo = s.mockResource.GetHostInfo()

	s.logger = s.mockResource.Logger
	s.config = NewDynamicConfigForTest()

	s.shardController = newShardController(s.mockResource, s.mockEngineFactory, s.config)
}

func (s *shardControllerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
	s.mockEngineFactory.AssertExpectations(s.T())
}

func (s *shardControllerSuite) TestAcquireShardSuccess() {
	numShards := 10
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	myShards := []int{}
	for shardID := int32(1); shardID <= int32(numShards); shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, int(shardID))
			s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
			s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
				&persistence.GetShardResponse{
					ShardInfo: &persistenceblobs.ShardInfo{
						ShardId:             shardID,
						Owner:               s.hostInfo.Identity(),
						RangeId:             5,
						ReplicationAckLevel: replicationAck,
						TransferAckLevel:    currentClusterTransferAck,
						TimerAckLevelTime:   currentClusterTimerAck,
						ClusterTransferAckLevel: map[string]int64{
							cluster.TestCurrentClusterName:     currentClusterTransferAck,
							cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
						},
						ClusterTimerAckLevel: map[string]*time.Time{
							cluster.TestCurrentClusterName:     currentClusterTimerAck,
							cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
						},
						ClusterReplicationLevel: map[string]int64{},
					},
				}, nil).Once()
			s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
				ShardInfo: &persistenceblobs.ShardInfo{
					ShardId:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeId:             6,
					StolenSinceRenew:    1,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevelTime:   currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]*time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					ClusterReplicationLevel: map[string]int64{},
				},
				PreviousRangeID: 5,
			}).Return(nil).Once()
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil).Times(1)
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()
	count := 0
	for _, shardID := range myShards {
		s.NotNil(s.shardController.getEngineForShard(shardID))
		count++
	}
	s.Equal(2, count)
}

func (s *shardControllerSuite) TestAcquireShardsConcurrently() {
	numShards := 10
	s.config.NumberOfShards = numShards
	s.config.AcquireShardConcurrency = func(opts ...dynamicconfig.FilterOption) int {
		return 10
	}

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	var myShards []int
	for shardID := int32(1); shardID <= int32(numShards); shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, int(shardID))
			s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
			s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
				&persistence.GetShardResponse{
					ShardInfo: &persistenceblobs.ShardInfo{
						ShardId:             shardID,
						Owner:               s.hostInfo.Identity(),
						RangeId:             5,
						ReplicationAckLevel: replicationAck,
						TransferAckLevel:    currentClusterTransferAck,
						TimerAckLevelTime:   currentClusterTimerAck,
						ClusterTransferAckLevel: map[string]int64{
							cluster.TestCurrentClusterName:     currentClusterTransferAck,
							cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
						},
						ClusterTimerAckLevel: map[string]*time.Time{
							cluster.TestCurrentClusterName:     currentClusterTimerAck,
							cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
						},
						ClusterReplicationLevel: map[string]int64{},
					},
				}, nil).Once()
			s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
				ShardInfo: &persistenceblobs.ShardInfo{
					ShardId:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeId:             6,
					StolenSinceRenew:    1,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevelTime:   currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]*time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					ClusterReplicationLevel: map[string]int64{},
				},
				PreviousRangeID: 5,
			}).Return(nil).Once()
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil).Times(1)
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()
	count := 0
	for _, shardID := range myShards {
		s.NotNil(s.shardController.getEngineForShard(shardID))
		count++
	}
	s.Equal(2, count)
}

func (s *shardControllerSuite) TestAcquireShardLookupFailure() {
	numShards := 2
	s.config.NumberOfShards = numShards
	for shardID := 1; shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(nil, errors.New("ring failure")).Times(1)
	}

	s.shardController.acquireShards()
	for shardID := 1; shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(nil, errors.New("ring failure")).Times(1)
		s.Nil(s.shardController.getEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestAcquireShardRenewSuccess() {
	numShards := 2
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	for shardID := int32(1); shardID <= int32(numShards); shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
		s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
			&persistence.GetShardResponse{
				ShardInfo: &persistenceblobs.ShardInfo{
					ShardId:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeId:             5,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevelTime:   currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]*time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					ClusterReplicationLevel: map[string]int64{},
				},
			}, nil).Once()
		s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:             shardID,
				Owner:               s.hostInfo.Identity(),
				RangeId:             6,
				StolenSinceRenew:    1,
				ReplicationAckLevel: replicationAck,
				TransferAckLevel:    currentClusterTransferAck,
				TimerAckLevelTime:   currentClusterTimerAck,
				ClusterTransferAckLevel: map[string]int64{
					cluster.TestCurrentClusterName:     currentClusterTransferAck,
					cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
				},
				ClusterTimerAckLevel: map[string]*time.Time{
					cluster.TestCurrentClusterName:     currentClusterTimerAck,
					cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
				},
				ClusterReplicationLevel: map[string]int64{},
			},
			PreviousRangeID: 5,
		}).Return(nil).Once()
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()

	for shardID := 1; shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(s.hostInfo, nil).Times(1)
	}
	s.shardController.acquireShards()

	for shardID := 1; shardID <= numShards; shardID++ {
		s.NotNil(s.shardController.getEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestAcquireShardRenewLookupFailed() {
	numShards := 2
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	for shardID := int32(1); shardID <= int32(numShards); shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
		s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
			&persistence.GetShardResponse{
				ShardInfo: &persistenceblobs.ShardInfo{
					ShardId:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeId:             5,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevelTime:   currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]*time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					ClusterReplicationLevel: map[string]int64{},
				},
			}, nil).Once()
		s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:             shardID,
				Owner:               s.hostInfo.Identity(),
				RangeId:             6,
				StolenSinceRenew:    1,
				ReplicationAckLevel: replicationAck,
				TransferAckLevel:    currentClusterTransferAck,
				TimerAckLevelTime:   currentClusterTimerAck,
				ClusterTransferAckLevel: map[string]int64{
					cluster.TestCurrentClusterName:     currentClusterTransferAck,
					cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
				},
				ClusterTimerAckLevel: map[string]*time.Time{
					cluster.TestCurrentClusterName:     currentClusterTimerAck,
					cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
				},
				ClusterReplicationLevel: map[string]int64{},
			},
			PreviousRangeID: 5,
		}).Return(nil).Once()
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()

	for shardID := 1; shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(nil, errors.New("ring failure")).Times(1)
	}
	s.shardController.acquireShards()

	for shardID := 1; shardID <= numShards; shardID++ {
		s.NotNil(s.shardController.getEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestHistoryEngineClosed() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = newShardController(s.mockResource, s.mockEngineFactory, s.config)
	historyEngines := make(map[int]*MockEngine)
	for shardID := 1; shardID <= numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	s.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName,
		gomock.Any()).Return(nil).AnyTimes()
	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.Start()
	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := 1; shardID <= numShards; shardID++ {
					engine, err := s.shardController.getEngineForShard(shardID)
					s.Nil(err)
					s.NotNil(engine)
				}
			}
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	differentHostInfo := membership.NewHostInfo("another-host", nil)
	for shardID := 1; shardID <= 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(differentHostInfo, nil).AnyTimes()
		s.shardController.shardClosedCallback(shardID, nil)
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := 3; shardID <= numShards; shardID++ {
					engine, err := s.shardController.getEngineForShard(shardID)
					s.Nil(err)
					s.NotNil(engine)
					time.Sleep(20 * time.Millisecond)
				}
			}
			workerWG.Done()
		}()
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := 1; shardID <= 2; shardID++ {
					_, err := s.shardController.getEngineForShard(shardID)
					if err != nil {
						s.logger.Error("ShardLost", tag.Error(err))
						shardLost = true
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			s.True(shardLost)
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	s.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()
	for shardID := 3; shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
}

func (s *shardControllerSuite) TestShardControllerClosed() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = newShardController(s.mockResource, s.mockEngineFactory, s.config)
	historyEngines := make(map[int]*MockEngine)
	for shardID := 1; shardID <= numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	s.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).Return(nil).AnyTimes()
	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.Start()

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := 1; shardID <= numShards; shardID++ {
					_, err := s.shardController.getEngineForShard(shardID)
					if err != nil {
						s.logger.Error("ShardLost", tag.Error(err))
						shardLost = true
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			s.True(shardLost)
			workerWG.Done()
		}()
	}

	s.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()
	for shardID := 1; shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
	workerWG.Wait()
}

func (s *shardControllerSuite) setupMocksForAcquireShard(shardID int, mockEngine *MockEngine, currentRangeID,
	newRangeID int64) {

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	// s.mockResource.ExecutionMgr.On("Close").Return()
	mockEngine.EXPECT().Start().Times(1)
	s.mockServiceResolver.EXPECT().Lookup(convert.IntToString(shardID)).Return(s.hostInfo, nil).Times(2)
	s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(mockEngine).Once()
	s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: int32(shardID)}).Return(
		&persistence.GetShardResponse{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:             int32(shardID),
				Owner:               s.hostInfo.Identity(),
				RangeId:             currentRangeID,
				ReplicationAckLevel: replicationAck,
				TransferAckLevel:    currentClusterTransferAck,
				TimerAckLevelTime:   currentClusterTimerAck,
				ClusterTransferAckLevel: map[string]int64{
					cluster.TestCurrentClusterName:     currentClusterTransferAck,
					cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
				},
				ClusterTimerAckLevel: map[string]*time.Time{
					cluster.TestCurrentClusterName:     currentClusterTimerAck,
					cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
				},
				ClusterReplicationLevel: map[string]int64{},
			},
		}, nil).Once()
	s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
		ShardInfo: &persistenceblobs.ShardInfo{
			ShardId:             int32(shardID),
			Owner:               s.hostInfo.Identity(),
			RangeId:             newRangeID,
			StolenSinceRenew:    1,
			ReplicationAckLevel: replicationAck,
			TransferAckLevel:    currentClusterTransferAck,
			TimerAckLevelTime:   currentClusterTimerAck,
			ClusterTransferAckLevel: map[string]int64{
				cluster.TestCurrentClusterName:     currentClusterTransferAck,
				cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
			},
			ClusterTimerAckLevel: map[string]*time.Time{
				cluster.TestCurrentClusterName:     currentClusterTimerAck,
				cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
			},
			ClusterReplicationLevel: map[string]int64{},
		},
		PreviousRangeID: currentRangeID,
	}).Return(nil).Once()
}
