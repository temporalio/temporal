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
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
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
		mockHostInfoProvider *membership.MockHostInfoProvider
	}
)

func NewTestController(
	engineFactory *MockEngineFactory,
	config *configs.Config,
	resource *resource.Test,
	hostInfoProvider *membership.MockHostInfoProvider,
) *ControllerImpl {
	return &ControllerImpl{
		config:                      config,
		logger:                      resource.GetLogger(),
		throttledLogger:             resource.GetThrottledLogger(),
		contextTaggedLogger:         log.With(resource.GetLogger(), tag.ComponentShardController, tag.Address(resource.GetHostInfo().Identity())),
		persistenceExecutionManager: resource.GetExecutionManager(),
		persistenceShardManager:     resource.GetShardManager(),
		clientBean:                  resource.GetClientBean(),
		historyClient:               resource.GetHistoryClient(),
		historyServiceResolver:      resource.GetHistoryServiceResolver(),
		metricsClient:               resource.GetMetricsClient(),
		payloadSerializer:           resource.GetPayloadSerializer(),
		timeSource:                  resource.GetTimeSource(),
		namespaceRegistry:           resource.GetNamespaceRegistry(),
		saProvider:                  resource.GetSearchAttributesProvider(),
		saMapper:                    resource.GetSearchAttributesMapper(),
		clusterMetadata:             resource.GetClusterMetadata(),
		archivalMetadata:            resource.GetArchivalMetadata(),
		hostInfoProvider:            hostInfoProvider,

		status:             common.DaemonStatusInitialized,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 10),
		engineFactory:      engineFactory,
		shutdownCh:         make(chan struct{}),
		metricsScope:       resource.GetMetricsClient().Scope(metrics.HistoryShardControllerScope),
		historyShards:      make(map[int32]*ContextImpl),
	}
}

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
	s.mockHostInfoProvider = membership.NewMockHostInfoProvider(s.controller)
	s.mockHostInfoProvider.EXPECT().HostInfo().Return(s.hostInfo).AnyTimes()

	s.logger = s.mockResource.Logger
	s.config = tests.NewDynamicConfig()

	s.shardController = NewTestController(
		s.mockEngineFactory,
		s.config,
		s.mockResource,
		s.mockHostInfoProvider,
	)
}

func (s *controllerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *controllerSuite) TestAcquireShardSuccess() {
	numShards := int32(8)
	s.config.NumberOfShards = numShards

	queueAckLevels := s.queueAckLevels()
	queueStates := s.queueStates()

	var myShards []int32
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			s.mockHistoryEngine.EXPECT().Start().Return()
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
			s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
				&persistence.GetOrCreateShardResponse{
					ShardInfo: &persistencespb.ShardInfo{
						ShardId:                shardID,
						Owner:                  s.hostInfo.Identity(),
						RangeId:                5,
						ReplicationDlqAckLevel: map[string]int64{},
						QueueAckLevels:         queueAckLevels,
						QueueStates:            queueStates,
					},
				}, nil)
			s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), &persistence.UpdateShardRequest{
				ShardInfo: &persistencespb.ShardInfo{
					ShardId:                shardID,
					Owner:                  s.hostInfo.Identity(),
					RangeId:                6,
					StolenSinceRenew:       1,
					ReplicationDlqAckLevel: map[string]int64{},
					QueueAckLevels:         queueAckLevels,
					QueueStates:            queueStates,
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, shardID := range myShards {
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		_, err = shard.GetEngine(ctx)
		s.NoError(err)
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

	queueAckLevels := s.queueAckLevels()
	queueStates := s.queueStates()

	var myShards []int32
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			s.mockHistoryEngine.EXPECT().Start().Return()
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
			s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
			s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
				&persistence.GetOrCreateShardResponse{
					ShardInfo: &persistencespb.ShardInfo{
						ShardId:                shardID,
						Owner:                  s.hostInfo.Identity(),
						RangeId:                5,
						ReplicationDlqAckLevel: map[string]int64{},
						QueueAckLevels:         queueAckLevels,
						QueueStates:            queueStates,
					},
				}, nil)
			s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), &persistence.UpdateShardRequest{
				ShardInfo: &persistencespb.ShardInfo{
					ShardId:                shardID,
					Owner:                  s.hostInfo.Identity(),
					RangeId:                6,
					StolenSinceRenew:       1,
					ReplicationDlqAckLevel: map[string]int64{},
					QueueAckLevels:         queueAckLevels,
					QueueStates:            queueStates,
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
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		_, err = shard.GetEngine(ctx)
		s.NoError(err)
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
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
		shard, err := s.shardController.GetShardByID(shardID)
		s.Error(err)
		s.Nil(shard)
	}
}

func (s *controllerSuite) TestAcquireShardRenewSuccess() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	queueAckLevels := s.queueAckLevels()
	queueStates := s.queueStates()

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
		s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
			&persistence.GetOrCreateShardResponse{
				ShardInfo: &persistencespb.ShardInfo{
					ShardId:                shardID,
					Owner:                  s.hostInfo.Identity(),
					RangeId:                5,
					ReplicationDlqAckLevel: map[string]int64{},
					QueueAckLevels:         queueAckLevels,
					QueueStates:            queueStates,
				},
			}, nil)
		s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), &persistence.UpdateShardRequest{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                6,
				StolenSinceRenew:       1,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueAckLevels:         queueAckLevels,
				QueueStates:            queueStates,
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
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		s.NotNil(shard)
		engine, err := shard.GetEngine(ctx)
		s.NoError(err)
		s.NotNil(engine)
	}
}

func (s *controllerSuite) TestAcquireShardRenewLookupFailed() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	queueAckLevels := s.queueAckLevels()
	queueStates := s.queueStates()

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockHistoryEngine.EXPECT().Start().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2)
		s.mockEngineFactory.EXPECT().CreateEngine(gomock.Any()).Return(s.mockHistoryEngine)
		s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
			&persistence.GetOrCreateShardResponse{
				ShardInfo: &persistencespb.ShardInfo{
					ShardId:                shardID,
					Owner:                  s.hostInfo.Identity(),
					RangeId:                5,
					ReplicationDlqAckLevel: map[string]int64{},
					QueueAckLevels:         queueAckLevels,
					QueueStates:            queueStates,
				},
			}, nil)
		s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), &persistence.UpdateShardRequest{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                6,
				StolenSinceRenew:       1,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueAckLevels:         queueAckLevels,
				QueueStates:            queueStates,
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
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		s.NotNil(shard)
		engine, err := shard.GetEngine(ctx)
		s.NoError(err)
		s.NotNil(engine)
	}
}

func (s *controllerSuite) TestHistoryEngineClosed() {
	numShards := int32(4)
	s.config.NumberOfShards = numShards
	s.shardController = NewTestController(
		s.mockEngineFactory,
		s.config,
		s.mockResource,
		s.mockHostInfoProvider,
	)
	historyEngines := make(map[int32]*MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
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
					shard, err := s.shardController.GetShardByID(shardID)
					s.NoError(err)
					s.NotNil(shard)
					engine, err := shard.GetEngine(ctx)
					s.NoError(err)
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
					shard, err := s.shardController.GetShardByID(shardID)
					s.NoError(err)
					s.NotNil(shard)
					engine, err := shard.GetEngine(ctx)
					s.NoError(err)
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
					_, err := s.shardController.GetShardByID(shardID)
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
	s.shardController = NewTestController(
		s.mockEngineFactory,
		s.config,
		s.mockResource,
		s.mockHostInfoProvider,
	)

	historyEngines := make(map[int32]*MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
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
				for shardID := int32(1); shardID <= numShards; shardID++ {
					_, err := s.shardController.GetShardByID(shardID)
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

func (s *controllerSuite) TestShardExplicitUnload() {
	s.config.NumberOfShards = 1

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	mockEngine := NewMockEngine(s.controller)
	mockEngine.EXPECT().Stop().AnyTimes()
	s.setupMocksForAcquireShard(0, mockEngine, 5, 6, false)

	shard, err := s.shardController.getOrCreateShardContext(0)
	s.NoError(err)
	s.Equal(1, s.shardController.NumShards())

	shard.Unload()

	for tries := 0; tries < 100 && s.shardController.NumShards() != 0; tries++ {
		// removal from map happens asynchronously
		time.Sleep(1 * time.Millisecond)
	}
	s.Equal(0, s.shardController.NumShards())
	s.False(shard.isValid())
}

func (s *controllerSuite) TestShardExplicitUnloadCancelGetOrCreate() {
	s.config.NumberOfShards = 1

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	mockEngine := NewMockEngine(s.controller)
	mockEngine.EXPECT().Stop().AnyTimes()

	shardID := int32(0)
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil)

	ready := make(chan struct{})
	wasCanceled := make(chan bool)
	// GetOrCreateShard blocks for 5s or until canceled
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).DoAndReturn(
		func(ctx context.Context, req *persistence.GetOrCreateShardRequest) (*persistence.GetOrCreateShardResponse, error) {
			ready <- struct{}{}
			select {
			case <-time.After(5 * time.Second):
				wasCanceled <- false
				return nil, errors.New("timed out")
			case <-ctx.Done():
				wasCanceled <- true
				return nil, ctx.Err()
			}
		})

	// get shard, will start initializing in background
	shard, err := s.shardController.getOrCreateShardContext(0)
	s.NoError(err)

	<-ready
	// now shard is blocked on GetOrCreateShard
	s.False(shard.engineFuture.Ready())

	start := time.Now()
	shard.Unload() // this cancels the context so GetOrCreateShard returns immediately
	s.True(<-wasCanceled)
	s.Less(time.Since(start), 500*time.Millisecond)
}

func (s *controllerSuite) setupMocksForAcquireShard(
	shardID int32,
	mockEngine *MockEngine,
	currentRangeID, newRangeID int64,
	required bool,
) {

	queueAckLevels := s.queueAckLevels()
	queueStates := s.queueStates()

	minTimes := 0
	if required {
		minTimes = 1
	}

	// s.mockResource.ExecutionMgr.On("Close").Return()
	mockEngine.EXPECT().Start().MinTimes(minTimes)
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2).MinTimes(minTimes)
	s.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine).MinTimes(minTimes)
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                currentRangeID,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueAckLevels:         queueAckLevels,
				QueueStates:            queueStates,
			},
		}, nil).MinTimes(minTimes)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), &persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  s.hostInfo.Identity(),
			RangeId:                newRangeID,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueAckLevels:         queueAckLevels,
			QueueStates:            queueStates,
		},
		PreviousRangeID: currentRangeID,
	}).Return(nil).MinTimes(minTimes)
}

func (s *controllerSuite) queueAckLevels() map[int32]*persistencespb.QueueAckLevel {
	replicationAck := int64(201)
	currentClusterTransferAck := int64(210)
	alternativeClusterTransferAck := int64(320)
	currentClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-100)
	alternativeClusterTimerAck := timestamp.TimeNowPtrUtcAddSeconds(-200)
	return map[int32]*persistencespb.QueueAckLevel{
		tasks.CategoryTransfer.ID(): {
			AckLevel: currentClusterTransferAck,
			ClusterAckLevel: map[string]int64{
				cluster.TestCurrentClusterName:     currentClusterTransferAck,
				cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
			},
		},
		tasks.CategoryTimer.ID(): {
			AckLevel: currentClusterTimerAck.UnixNano(),
			ClusterAckLevel: map[string]int64{
				cluster.TestCurrentClusterName:     currentClusterTimerAck.UnixNano(),
				cluster.TestAlternativeClusterName: alternativeClusterTimerAck.UnixNano(),
			},
		},
		tasks.CategoryReplication.ID(): {
			AckLevel:        replicationAck,
			ClusterAckLevel: map[string]int64{},
		},
	}
}

func (s *controllerSuite) queueStates() map[int32]*persistencespb.QueueState {
	return map[int32]*persistencespb.QueueState{
		tasks.CategoryTransfer.ID(): {
			ReaderStates: nil,
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: &tasks.DefaultFireTime,
				TaskId:   rand.Int63(),
			},
		},
		tasks.CategoryTimer.ID(): {
			ReaderStates: make(map[int32]*persistencespb.QueueReaderState),
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: timestamp.TimeNowPtrUtc(),
				TaskId:   rand.Int63(),
			},
		},
		tasks.CategoryReplication.ID(): {
			ReaderStates: map[int32]*persistencespb.QueueReaderState{
				0: {
					Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: &tasks.DefaultFireTime,
									TaskId:   1000,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: &tasks.DefaultFireTime,
									TaskId:   2000,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enums.PREDICATE_TYPE_UNIVERSAL,
								Attributes: &persistencespb.Predicate_UniversalPredicateAttributes{
									UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
								},
							},
						},
					},
				},
				1: nil,
			},
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: &tasks.DefaultFireTime,
				TaskId:   3000,
			},
		},
	}
}

// This is needed to avoid race conditions when using this matcher, since
// fmt.Sprintf("%v"), used by gomock, would otherwise access private fields.
// See https://github.com/temporalio/temporal/issues/2777
var _ fmt.Stringer = (*ContextImpl)(nil)

type contextMatcher int32

func (s contextMatcher) Matches(x interface{}) bool {
	context, ok := x.(Context)
	return ok && context.GetShardID() == int32(s)
}

func (s contextMatcher) String() string {
	return strconv.Itoa(int(s))
}

type getOrCreateShardRequestMatcher int32

func (s getOrCreateShardRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*persistence.GetOrCreateShardRequest)
	return ok && req.ShardID == int32(s)
}

func (s getOrCreateShardRequestMatcher) String() string {
	return strconv.Itoa(int(s))
}
