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

package shard

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"

	persistencespb "go.temporal.io/server/api/persistence/v1"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
)

type (
	contextMatcher struct {
		shardID int32
	}

	controllerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockHistoryEngine   *MockEngine
		mockClusterMetadata *cluster.MockMetadata
		mockServiceResolver *membership.MockServiceResolver

		hostInfo          *membership.HostInfo
		mockShardManager  *persistence.MockShardManager
		mockEngineFactory *MockEngineFactory

		config               *configs.Config
		logger               log.Logger
		shardController      *ControllerImpl
		mockHostInfoProvider *resource.MockHostInfoProvider
	}
)

func TestShardControllerSuite(t *testing.T) {
	s := new(controllerSuite)
	suite.Run(t, s)
}

func (s *controllerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockHistoryEngine = NewMockEngine(s.controller)
	s.mockEngineFactory = NewMockEngineFactory(s.controller)

	s.mockShardManager = s.mockResource.ShardMgr
	s.mockServiceResolver = s.mockResource.HistoryServiceResolver
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.hostInfo = s.mockResource.GetHostInfo()
	s.mockHostInfoProvider = resource.NewMockHostInfoProvider(s.controller)
	s.mockHostInfoProvider.EXPECT().HostInfo().Return(s.hostInfo).AnyTimes()

	s.logger = s.mockResource.Logger
	s.config = tests.NewDynamicConfig()

	s.shardController = NewController(
		s.mockEngineFactory,
		s.config,
		s.mockResource.Logger,
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetShardManager(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetHistoryServiceResolver(),
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetPayloadSerializer(),
		s.mockResource.GetTimeSource(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesMapper(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetArchivalMetadata(),
		s.mockHostInfoProvider,
	)
}

func (s *controllerSuite) TearDownTest() {
	s.controller.Finish()
}

type getOrCreateShardRequestMatcher int32

func (s getOrCreateShardRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*persistence.GetOrCreateShardRequest)
	return ok && req.ShardID == int32(s)
}

func (s getOrCreateShardRequestMatcher) String() string {
	return strconv.Itoa(int(s))
}

func (s *controllerSuite) TestAcquireShardSuccess() {
	numShards := int32(10)
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	var myShards []int32
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			s.mockHistoryEngine.EXPECT().Start().Return()
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
			s.mockShardManager.EXPECT().GetOrCreateShard(getOrCreateShardRequestMatcher(shardID)).Return(
				&persistence.GetOrCreateShardResponse{
					ShardInfo: &persistencespb.ShardInfo{
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
						ReplicationDlqAckLevel:  map[string]int64{},
					},
				}, nil)
			s.mockShardManager.EXPECT().UpdateShard(&persistence.UpdateShardRequest{
				ShardInfo: &persistencespb.ShardInfo{
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
					ReplicationDlqAckLevel:  map[string]int64{},
				},
				PreviousRangeID: 5,
			}).Return(nil)
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil)
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shardID := range myShards {
		s.NotNil(s.shardController.GetEngineForShard(ctx, shardID))
		count++
	}
	s.Equal(2, count)
}

func (s *controllerSuite) TestAcquireShardsConcurrently() {
	numShards := int32(10)
	s.config.NumberOfShards = numShards
	s.config.AcquireShardConcurrency = func(opts ...dynamicconfig.FilterOption) int {
		return 10
	}

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	var myShards []int32
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			s.mockHistoryEngine.EXPECT().Start().Return()
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
			s.mockShardManager.EXPECT().GetOrCreateShard(getOrCreateShardRequestMatcher(shardID)).Return(
				&persistence.GetOrCreateShardResponse{
					ShardInfo: &persistencespb.ShardInfo{
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
						ReplicationDlqAckLevel:  map[string]int64{},
					},
				}, nil)
			s.mockShardManager.EXPECT().UpdateShard(&persistence.UpdateShardRequest{
				ShardInfo: &persistencespb.ShardInfo{
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
					ReplicationDlqAckLevel:  map[string]int64{},
				},
				PreviousRangeID: 5,
			}).Return(nil)
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(membership.NewHostInfo(ownerHost, nil), nil)
		}
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shardID := range myShards {
		s.NotNil(s.shardController.GetEngineForShard(ctx, shardID))
		count++
	}
	s.Equal(2, count)
}

func (s *controllerSuite) TestAcquireShardLookupFailure() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
	}

	s.shardController.acquireShards()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
		s.Nil(s.shardController.GetEngineForShard(ctx, shardID))
	}
}

func (s *controllerSuite) TestAcquireShardRenewSuccess() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
		s.mockShardManager.EXPECT().GetOrCreateShard(getOrCreateShardRequestMatcher(shardID)).Return(
			&persistence.GetOrCreateShardResponse{
				ShardInfo: &persistencespb.ShardInfo{
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
					ReplicationDlqAckLevel:  map[string]int64{},
				},
			}, nil)
		s.mockShardManager.EXPECT().UpdateShard(&persistence.UpdateShardRequest{
			ShardInfo: &persistencespb.ShardInfo{
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
				ReplicationDlqAckLevel:  map[string]int64{},
			},
			PreviousRangeID: 5,
		}).Return(nil)
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil)
	}
	s.shardController.acquireShards()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.NotNil(s.shardController.GetEngineForShard(ctx, shardID))
	}
}

func (s *controllerSuite) TestAcquireShardRenewLookupFailed() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
		s.mockShardManager.EXPECT().GetOrCreateShard(getOrCreateShardRequestMatcher(shardID)).Return(
			&persistence.GetOrCreateShardResponse{
				ShardInfo: &persistencespb.ShardInfo{
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
					ReplicationDlqAckLevel:  map[string]int64{},
				},
			}, nil)
		s.mockShardManager.EXPECT().UpdateShard(&persistence.UpdateShardRequest{
			ShardInfo: &persistencespb.ShardInfo{
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
				ReplicationDlqAckLevel:  map[string]int64{},
			},
			PreviousRangeID: 5,
		}).Return(nil)
	}

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.shardController.acquireShards()

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
	}
	s.shardController.acquireShards()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.NotNil(s.shardController.GetEngineForShard(ctx, shardID))
	}
}

func (s *controllerSuite) TestHistoryEngineClosed() {
	numShards := int32(4)
	s.config.NumberOfShards = numShards
	s.shardController = NewController(
		s.mockEngineFactory,
		s.config,
		s.mockResource.Logger,
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetShardManager(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetHistoryServiceResolver(),
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetPayloadSerializer(),
		s.mockResource.GetTimeSource(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesMapper(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetArchivalMetadata(),
		s.mockHostInfoProvider,
	)
	historyEngines := make(map[int32]*MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
				for shardID := int32(1); shardID <= numShards; shardID++ {
					engine, err := s.shardController.GetEngineForShard(ctx, shardID)
					s.Nil(err)
					s.NotNil(engine)
				}
			}
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	differentHostInfo := membership.NewHostInfo("another-host", nil)
	for shardID := int32(1); shardID <= 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(differentHostInfo, nil).AnyTimes()
		s.shardController.CloseShardByID(shardID)
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := int32(3); shardID <= numShards; shardID++ {
					engine, err := s.shardController.GetEngineForShard(ctx, shardID)
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
				for shardID := int32(1); shardID <= 2; shardID++ {
					_, err := s.shardController.GetEngineForShard(ctx, shardID)
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
	for shardID := int32(3); shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
}

func (s *controllerSuite) TestShardControllerClosed() {
	numShards := int32(4)
	s.config.NumberOfShards = numShards
	s.shardController = NewController(
		s.mockEngineFactory,
		s.config,
		s.mockResource.Logger,
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetShardManager(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetHistoryServiceResolver(),
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetPayloadSerializer(),
		s.mockResource.GetTimeSource(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesMapper(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetArchivalMetadata(),
		s.mockHostInfoProvider,
	)

	historyEngines := make(map[int32]*MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
				for shardID := int32(1); shardID <= numShards; shardID++ {
					_, err := s.shardController.GetEngineForShard(ctx, shardID)
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
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
	workerWG.Wait()
}

func (s *controllerSuite) setupMocksForAcquireShard(shardID int32, mockEngine *MockEngine, currentRangeID,
	newRangeID int64) {

	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)

	// s.mockResource.ExecutionMgr.On("Close").Return()
	mockEngine.EXPECT().Start()
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
	s.mockEngineFactory.EXPECT().CreateEngine(newContextMatcher(shardID)).Return(mockEngine)
	s.mockShardManager.EXPECT().GetOrCreateShard(getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:             shardID,
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
				ReplicationDlqAckLevel:  map[string]int64{},
			},
		}, nil)
	s.mockShardManager.EXPECT().UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:             shardID,
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
			ReplicationDlqAckLevel:  map[string]int64{},
		},
		PreviousRangeID: currentRangeID,
	}).Return(nil)
}

func newContextMatcher(shardID int32) *contextMatcher {
	return &contextMatcher{shardID: shardID}
}

func (m *contextMatcher) Matches(x interface{}) bool {
	context, ok := x.(Context)
	if !ok {
		return false
	}
	return m.shardID == context.GetShardID()
}

func (m *contextMatcher) String() string {
	// noop, not used
	return ""
}
