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

package history

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	mmocks "github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service"
	"github.com/uber-go/tally"
)

type (
	shardControllerSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		mockHistoryEngine *MockEngine

		hostInfo                *membership.HostInfo
		mockShardManager        *mmocks.ShardManager
		mockExecutionMgrFactory *mmocks.ExecutionManagerFactory
		mockHistoryV2Mgr        *mmocks.HistoryV2Manager
		mockServiceResolver     *mmocks.ServiceResolver
		mockMessaging           *mmocks.KafkaProducer
		mockClusterMetadata     *mmocks.ClusterMetadata
		mockClientBean          *client.MockClientBean
		mockEngineFactory       *MockHistoryEngineFactory
		mockMessagingClient     messaging.Client
		mockService             service.Service
		domainCache             cache.DomainCache
		config                  *Config
		logger                  log.Logger
		metricsClient           metrics.Client
		shardController         *shardController
	}
)

func TestShardControllerSuite(t *testing.T) {
	s := new(shardControllerSuite)
	suite.Run(t, s)
}

func (s *shardControllerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockHistoryEngine = NewMockEngine(s.controller)

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForTest()
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.hostInfo = membership.NewHostInfo("shardController-host-test", nil)
	s.mockShardManager = &mmocks.ShardManager{}
	s.mockExecutionMgrFactory = &mmocks.ExecutionManagerFactory{}
	s.mockHistoryV2Mgr = &mmocks.HistoryV2Manager{}
	s.mockServiceResolver = &mmocks.ServiceResolver{}
	s.mockEngineFactory = &MockHistoryEngineFactory{}
	s.mockMessaging = &mmocks.KafkaProducer{}
	s.mockClusterMetadata = &mmocks.ClusterMetadata{}
	s.mockMessagingClient = mmocks.NewMockMessagingClient(s.mockMessaging, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, s.metricsClient, s.mockClientBean, nil, nil, nil)
	s.shardController = newShardController(s.mockService, s.hostInfo, s.mockServiceResolver, s.mockShardManager,
		s.mockHistoryV2Mgr, nil, s.mockExecutionMgrFactory, s.mockEngineFactory, s.config, s.logger, s.metricsClient)
}

func (s *shardControllerSuite) TearDownTest() {
	s.mockExecutionMgrFactory.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockServiceResolver.AssertExpectations(s.T())
	s.mockEngineFactory.AssertExpectations(s.T())
	s.mockMessaging.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.controller.Finish()
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
			mockExecutionMgr := &mmocks.ExecutionManager{}
			s.mockExecutionMgrFactory.On("NewExecutionManager", mock.Anything).Return(mockExecutionMgr, nil).Once()
			s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
			s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
			s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
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
			s.mockServiceResolver.On("Lookup", string(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil).Once()
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.shardController.acquireShards()
	count := 0
	for _, shardID := range myShards {
		s.NotNil(s.shardController.getEngineForShard(shardID))
		count++
	}
	s.Equal(3, count)
}

func (s *shardControllerSuite) TestAcquireShardLookupFailure() {
	numShards := 2
	s.config.NumberOfShards = numShards
	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(nil, errors.New("ring failure")).Once()
	}

	s.shardController.acquireShards()
	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(nil, errors.New("ring failure")).Once()
		s.Nil(s.shardController.getEngineForShard(shardID))
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
		mockExecutionMgr := &mmocks.ExecutionManager{}
		s.mockExecutionMgrFactory.On("NewExecutionManager", mock.Anything).Return(mockExecutionMgr, nil).Once()
		s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
		s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
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
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Once()
	}
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.NotNil(s.shardController.getEngineForShard(shardID))
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
		mockExecutionMgr := &mmocks.ExecutionManager{}
		s.mockExecutionMgrFactory.On("NewExecutionManager", mock.Anything).Return(mockExecutionMgr, nil).Once()
		s.mockHistoryEngine.EXPECT().Start().Return().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
		s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(s.mockHistoryEngine).Once()
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
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(nil, errors.New("ring failure")).Once()
	}
	s.shardController.acquireShards()

	for shardID := 0; shardID < numShards; shardID++ {
		s.NotNil(s.shardController.getEngineForShard(shardID))
	}
}

func (s *shardControllerSuite) TestHistoryEngineClosed() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = newShardController(s.mockService, s.hostInfo, s.mockServiceResolver, s.mockShardManager, s.mockHistoryV2Mgr,
		s.domainCache, s.mockExecutionMgrFactory, s.mockEngineFactory, s.config, s.logger, s.metricsClient)
	historyEngines := make(map[int]*MockEngine)
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	s.mockServiceResolver.On("AddListener", shardControllerMembershipUpdateListenerName,
		mock.Anything).Return(nil)
	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.shardController.Start()
	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := 0; shardID < numShards; shardID++ {
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
	for shardID := 0; shardID < 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(differentHostInfo, nil)
		s.shardController.shardClosedCh <- shardID
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := 2; shardID < numShards; shardID++ {
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
				for shardID := 0; shardID < 2; shardID++ {
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

	s.mockServiceResolver.On("RemoveListener", shardControllerMembershipUpdateListenerName).Return(nil)
	for shardID := 2; shardID < numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil)
	}
	s.shardController.Stop()
}

func (s *shardControllerSuite) TestRingUpdated() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = newShardController(s.mockService, s.hostInfo, s.mockServiceResolver, s.mockShardManager, s.mockHistoryV2Mgr,
		s.domainCache, s.mockExecutionMgrFactory, s.mockEngineFactory, s.config, s.logger, s.metricsClient)
	historyEngines := make(map[int]*MockEngine)
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	s.mockServiceResolver.On("AddListener", shardControllerMembershipUpdateListenerName,
		mock.Anything).Return(nil)
	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.shardController.Start()

	differentHostInfo := membership.NewHostInfo("another-host", nil)
	for shardID := 0; shardID < 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(differentHostInfo, nil)
	}
	s.mockServiceResolver.On("Lookup", string(2)).Return(s.hostInfo, nil)
	s.mockServiceResolver.On("Lookup", string(3)).Return(s.hostInfo, nil)
	s.shardController.membershipUpdateCh <- &membership.ChangedEvent{}

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := 2; shardID < numShards; shardID++ {
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
				for shardID := 0; shardID < 2; shardID++ {
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

	s.mockServiceResolver.On("RemoveListener", shardControllerMembershipUpdateListenerName).Return(nil)
	for shardID := 2; shardID < numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil)
	}
	s.shardController.Stop()
}

func (s *shardControllerSuite) TestShardControllerClosed() {
	numShards := 4
	s.config.NumberOfShards = numShards
	s.shardController = newShardController(s.mockService, s.hostInfo, s.mockServiceResolver, s.mockShardManager, s.mockHistoryV2Mgr,
		s.domainCache, s.mockExecutionMgrFactory, s.mockEngineFactory, s.config, s.logger, s.metricsClient)
	historyEngines := make(map[int]*MockEngine)
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	s.mockServiceResolver.On("AddListener", shardControllerMembershipUpdateListenerName,
		mock.Anything).Return(nil)
	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.shardController.Start()

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := 0; shardID < numShards; shardID++ {
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

	s.mockServiceResolver.On("RemoveListener", shardControllerMembershipUpdateListenerName).Return(nil)
	for shardID := 0; shardID < numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Times(1)
		s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil)
	}
	s.shardController.Stop()
	workerWG.Wait()
}

func (s *shardControllerSuite) setupMocksForAcquireShard(shardID int, mockEngine *MockEngine, currentRangeID,
	newRangeID int64) {

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := time.Now().Add(-100 * time.Second)
	alternativeClusterTimerAck := time.Now().Add(-200 * time.Second)

	mockExecutionMgr := &mmocks.ExecutionManager{}
	mockExecutionMgr.On("Close").Return()
	s.mockExecutionMgrFactory.On("NewExecutionManager", shardID).Return(mockExecutionMgr, nil).Once()
	mockEngine.EXPECT().Start().Times(1)
	s.mockServiceResolver.On("Lookup", string(shardID)).Return(s.hostInfo, nil).Twice()
	s.mockEngineFactory.On("CreateEngine", mock.Anything).Return(mockEngine).Once()
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
