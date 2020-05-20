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

package shard

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/resource"
)

type (
	shardControllerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockHistoryEngine   *engine.MockEngine
		mockClusterMetadata *cluster.MockMetadata
		mockServiceResolver *membership.MockServiceResolver

		hostInfo          *membership.HostInfo
		mockShardManager  *mmocks.ShardManager
		mockEngineFactory *MockEngineFactory

		config          *config.Config
		logger          log.Logger
		shardController *controller
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
	s.mockEngineFactory = NewMockEngineFactory(s.controller)

	s.mockHistoryEngine = engine.NewMockEngine(s.controller)

	s.mockShardManager = s.mockResource.ShardMgr
	s.mockServiceResolver = s.mockResource.HistoryServiceResolver
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.hostInfo = s.mockResource.GetHostInfo()

	s.logger = s.mockResource.Logger
	s.config = config.NewForTest()

	s.shardController = NewShardController(s.mockResource, s.mockEngineFactory, s.config).(*controller)
}

func (s *shardControllerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
}

func (s *shardControllerSuite) TestAcquireShardSuccess() {
	numShards := 10
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := time.Now().Add(-100 * time.Second)
	alternativeClusterTimerAck := time.Now().Add(-200 * time.Second)

	myShards := []int{}
	for shardID := 0; shardID < numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
			s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine).Times(1)
			s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
				&persistence.GetShardResponse{
					ShardInfo: &persistence.ShardInfo{
						ShardID:             shardID,
						Owner:               s.hostInfo.Identity(),
						RangeID:             5,
						ReplicationAckLevel: replicationAck,
						TransferAckLevel:    currentClusterTransferAck,
						TimerAckLevel:       currentClusterTimerAck,
						ClusterTransferAckLevel: map[string]int64{
							cluster.TestCurrentClusterName:     currentClusterTransferAck,
							cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
						},
						ClusterTimerAckLevel: map[string]time.Time{
							cluster.TestCurrentClusterName:     currentClusterTimerAck,
							cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
						},
						ClusterReplicationLevel: map[string]int64{},
					},
				}, nil).Once()
			s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
				ShardInfo: &persistence.ShardInfo{
					ShardID:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeID:             6,
					StolenSinceRenew:    1,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevel:       currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					TransferFailoverLevels:  map[string]persistence.TransferFailoverLevel{},
					TimerFailoverLevels:     map[string]persistence.TimerFailoverLevel{},
					ClusterReplicationLevel: map[string]int64{},
				},
				PreviousRangeID: 5,
			}).Return(nil).Once()
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil).Times(1)
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()
	count := 0
	for _, shardID := range myShards {
		s.NotNil(s.shardController.GetEngineForShard(shardID))
		count++
	}
	s.Equal(3, count)
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
	currentClusterTimerAck := time.Now().Add(-100 * time.Second)
	alternativeClusterTimerAck := time.Now().Add(-200 * time.Second)

	var myShards []int
	for shardID := 0; shardID < numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
			s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine).Times(1)
			s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
				&persistence.GetShardResponse{
					ShardInfo: &persistence.ShardInfo{
						ShardID:             shardID,
						Owner:               s.hostInfo.Identity(),
						RangeID:             5,
						ReplicationAckLevel: replicationAck,
						TransferAckLevel:    currentClusterTransferAck,
						TimerAckLevel:       currentClusterTimerAck,
						ClusterTransferAckLevel: map[string]int64{
							cluster.TestCurrentClusterName:     currentClusterTransferAck,
							cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
						},
						ClusterTimerAckLevel: map[string]time.Time{
							cluster.TestCurrentClusterName:     currentClusterTimerAck,
							cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
						},
						ClusterReplicationLevel: map[string]int64{},
					},
				}, nil).Once()
			s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
				ShardInfo: &persistence.ShardInfo{
					ShardID:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeID:             6,
					StolenSinceRenew:    1,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevel:       currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					TransferFailoverLevels:  map[string]persistence.TransferFailoverLevel{},
					TimerFailoverLevels:     map[string]persistence.TimerFailoverLevel{},
					ClusterReplicationLevel: map[string]int64{},
				},
				PreviousRangeID: 5,
			}).Return(nil).Once()
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil).Times(1)
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()
	count := 0
	for _, shardID := range myShards {
		s.NotNil(s.shardController.GetEngineForShard(shardID))
		count++
	}
	s.Equal(3, count)
}

func (s *shardControllerSuite) TestAcquireShardLookupFailure() {
	numShards := 2
	s.config.NumberOfShards = numShards
	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(nil, errors.New("ring failure")).Times(1)
	}

	s.shardController.acquireShards()
	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(nil, errors.New("ring failure")).Times(1)
		s.Nil(s.shardController.GetEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestAcquireShardRenewSuccess() {
	numShards := 2
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := time.Now().Add(-100 * time.Second)
	alternativeClusterTimerAck := time.Now().Add(-200 * time.Second)

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine).Times(1)
		s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
			&persistence.GetShardResponse{
				ShardInfo: &persistence.ShardInfo{
					ShardID:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeID:             5,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevel:       currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					ClusterReplicationLevel: map[string]int64{},
				},
			}, nil).Once()
		s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
			ShardInfo: &persistence.ShardInfo{
				ShardID:             shardID,
				Owner:               s.hostInfo.Identity(),
				RangeID:             6,
				StolenSinceRenew:    1,
				ReplicationAckLevel: replicationAck,
				TransferAckLevel:    currentClusterTransferAck,
				TimerAckLevel:       currentClusterTimerAck,
				ClusterTransferAckLevel: map[string]int64{
					cluster.TestCurrentClusterName:     currentClusterTransferAck,
					cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
				},
				ClusterTimerAckLevel: map[string]time.Time{
					cluster.TestCurrentClusterName:     currentClusterTimerAck,
					cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
				},
				TransferFailoverLevels:  map[string]persistence.TransferFailoverLevel{},
				TimerFailoverLevels:     map[string]persistence.TimerFailoverLevel{},
				ClusterReplicationLevel: map[string]int64{},
			},
			PreviousRangeID: 5,
		}).Return(nil).Once()
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).Times(1)
	}
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.NotNil(s.shardController.GetEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestAcquireShardRenewLookupFailed() {
	numShards := 2
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := time.Now().Add(-100 * time.Second)
	alternativeClusterTimerAck := time.Now().Add(-200 * time.Second)

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine).Times(1)
		s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
			&persistence.GetShardResponse{
				ShardInfo: &persistence.ShardInfo{
					ShardID:             shardID,
					Owner:               s.hostInfo.Identity(),
					RangeID:             5,
					ReplicationAckLevel: replicationAck,
					TransferAckLevel:    currentClusterTransferAck,
					TimerAckLevel:       currentClusterTimerAck,
					ClusterTransferAckLevel: map[string]int64{
						cluster.TestCurrentClusterName:     currentClusterTransferAck,
						cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
					},
					ClusterTimerAckLevel: map[string]time.Time{
						cluster.TestCurrentClusterName:     currentClusterTimerAck,
						cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
					},
					ClusterReplicationLevel: map[string]int64{},
				},
			}, nil).Once()
		s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
			ShardInfo: &persistence.ShardInfo{
				ShardID:             shardID,
				Owner:               s.hostInfo.Identity(),
				RangeID:             6,
				StolenSinceRenew:    1,
				ReplicationAckLevel: replicationAck,
				TransferAckLevel:    currentClusterTransferAck,
				TimerAckLevel:       currentClusterTimerAck,
				ClusterTransferAckLevel: map[string]int64{
					cluster.TestCurrentClusterName:     currentClusterTransferAck,
					cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
				},
				ClusterTimerAckLevel: map[string]time.Time{
					cluster.TestCurrentClusterName:     currentClusterTimerAck,
					cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
				},
				TransferFailoverLevels:  map[string]persistence.TransferFailoverLevel{},
				TimerFailoverLevels:     map[string]persistence.TimerFailoverLevel{},
				ClusterReplicationLevel: map[string]int64{},
			},
			PreviousRangeID: 5,
		}).Return(nil).Once()
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(nil, errors.New("ring failure")).Times(1)
	}
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.NotNil(s.shardController.GetEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestHistoryEngineClosed() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = NewShardController(s.mockResource, s.mockEngineFactory, s.config).(*controller)
	historyEngines := make(map[int]*engine.MockEngine)
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := engine.NewMockEngine(s.controller)
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
				for shardID := 0; shardID < numShards; shardID++ {
					engine, err := s.shardController.GetEngineForShard(shardID)
					s.Nil(err)
					s.NotNil(engine)
				}
			}
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	differentHostInfo := membership.NewHostInfo("another-host", nil)
	for shardID := 0; shardID < 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(differentHostInfo, nil).AnyTimes()
		s.shardController.shardClosedCallback(shardID, nil)
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := 2; shardID < numShards; shardID++ {
					engine, err := s.shardController.GetEngineForShard(shardID)
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
				for shardID := 0; shardID < 2; shardID++ {
					_, err := s.shardController.GetEngineForShard(shardID)
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
	for shardID := 2; shardID < numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
}

func (s *shardControllerSuite) TestShardControllerClosed() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = NewShardController(s.mockResource, s.mockEngineFactory, s.config).(*controller)
	historyEngines := make(map[int]*engine.MockEngine)
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := engine.NewMockEngine(s.controller)
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
				for shardID := 0; shardID < numShards; shardID++ {
					_, err := s.shardController.GetEngineForShard(shardID)
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
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Times(1)
		s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
	workerWG.Wait()
}

func (s *shardControllerSuite) setupMocksForAcquireShard(shardID int, mockEngine *engine.MockEngine, currentRangeID,
	newRangeID int64) {

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := time.Now().Add(-100 * time.Second)
	alternativeClusterTimerAck := time.Now().Add(-200 * time.Second)

	// s.mockResource.ExecutionMgr.On("Close").Return()
	mockEngine.EXPECT().Start().Times(1)
	s.mockServiceResolver.EXPECT().Lookup(string(shardID)).Return(s.hostInfo, nil).Times(2)
	s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(mockEngine).Times(1)
	s.mockShardManager.On("GetShard", &persistence.GetShardRequest{ShardID: shardID}).Return(
		&persistence.GetShardResponse{
			ShardInfo: &persistence.ShardInfo{
				ShardID:             shardID,
				Owner:               s.hostInfo.Identity(),
				RangeID:             currentRangeID,
				ReplicationAckLevel: replicationAck,
				TransferAckLevel:    currentClusterTransferAck,
				TimerAckLevel:       currentClusterTimerAck,
				ClusterTransferAckLevel: map[string]int64{
					cluster.TestCurrentClusterName:     currentClusterTransferAck,
					cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
				},
				ClusterTimerAckLevel: map[string]time.Time{
					cluster.TestCurrentClusterName:     currentClusterTimerAck,
					cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
				},
				ClusterReplicationLevel: map[string]int64{},
			},
		}, nil).Once()
	s.mockShardManager.On("UpdateShard", &persistence.UpdateShardRequest{
		ShardInfo: &persistence.ShardInfo{
			ShardID:             shardID,
			Owner:               s.hostInfo.Identity(),
			RangeID:             newRangeID,
			StolenSinceRenew:    1,
			ReplicationAckLevel: replicationAck,
			TransferAckLevel:    currentClusterTransferAck,
			TimerAckLevel:       currentClusterTimerAck,
			ClusterTransferAckLevel: map[string]int64{
				cluster.TestCurrentClusterName:     currentClusterTransferAck,
				cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
			},
			ClusterTimerAckLevel: map[string]time.Time{
				cluster.TestCurrentClusterName:     currentClusterTimerAck,
				cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
			},
			TransferFailoverLevels:  map[string]persistence.TransferFailoverLevel{},
			TimerFailoverLevels:     map[string]persistence.TimerFailoverLevel{},
			ClusterReplicationLevel: map[string]int64{},
		},
		PreviousRangeID: currentRangeID,
	}).Return(nil).Once()
}
