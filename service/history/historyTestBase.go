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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/service"
	cconfig "github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

var (
	testDomainActiveID           = "7b3fe0f6-e98f-4960-bdb7-220d0fb3f521"
	testDomainStandbyID          = "ede1e8a6-bdb7-e98f-4960-448b0cdef134"
	testDomainActiveName         = "test active domain name"
	testDomainStandbyName        = "test standby domain name"
	testDomainRetention          = int32(10)
	testDomainEmitMetric         = true
	testDomainActiveClusterName  = cluster.TestCurrentClusterName
	testDomainStandbyClusterName = cluster.TestAlternativeClusterName
	testDomainIsGlobalDomain     = false
	testDomainAllClusters        = []*persistence.ClusterReplicationConfig{
		{ClusterName: testDomainActiveClusterName},
		{ClusterName: testDomainStandbyClusterName},
	}
)

type (
	// TestShardContext shard context for testing.
	TestShardContext struct {
		shardID int
		sync.RWMutex
		service                service.Service
		shardInfo              *persistence.ShardInfo
		transferSequenceNumber int64
		historyV2Mgr           persistence.HistoryV2Manager
		executionMgr           persistence.ExecutionManager
		domainCache            cache.DomainCache
		clusterMetadata        cluster.Metadata
		eventsCache            eventsCache
		engine                 Engine

		config                    *Config
		logger                    log.Logger
		metricsClient             metrics.Client
		standbyClusterCurrentTime map[string]time.Time
		timerMaxReadLevelMap      map[string]time.Time
	}

	// TestBase wraps the base setup needed to create workflows over engine layer.
	TestBase struct {
		persistencetests.TestBase
		ShardContext *TestShardContext
	}
)

var _ ShardContext = (*TestShardContext)(nil)

func newTestShardContext(shardInfo *persistence.ShardInfo, transferSequenceNumber int64,
	historyV2Mgr persistence.HistoryV2Manager, executionMgr persistence.ExecutionManager,
	metadataMgr persistence.MetadataManager, clusterMetadata cluster.Metadata,
	clientBean client.Bean, config *Config, logger log.Logger) *TestShardContext {
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	domainCache := cache.NewDomainCache(metadataMgr, clusterMetadata, metricsClient, logger)
	serializer := persistence.NewPayloadSerializer()
	// initialize the cluster current time to be the same as ack level
	standbyClusterCurrentTime := make(map[string]time.Time)
	timerMaxReadLevelMap := make(map[string]time.Time)
	for clusterName, info := range clusterMetadata.GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if clusterName != clusterMetadata.GetCurrentClusterName() {
			if currentTime, ok := shardInfo.ClusterTimerAckLevel[clusterName]; ok {
				standbyClusterCurrentTime[clusterName] = currentTime
				timerMaxReadLevelMap[clusterName] = currentTime
			} else {
				standbyClusterCurrentTime[clusterName] = shardInfo.TimerAckLevel
				timerMaxReadLevelMap[clusterName] = shardInfo.TimerAckLevel
			}
		} else { // active cluster
			timerMaxReadLevelMap[clusterName] = shardInfo.TimerAckLevel
		}
	}

	shardCtx := &TestShardContext{
		shardID: 0,
		service: service.NewTestService(
			clusterMetadata,
			nil,
			metricsClient,
			clientBean,
			nil,
			nil,
			serializer),
		shardInfo:                 shardInfo,
		transferSequenceNumber:    transferSequenceNumber,
		historyV2Mgr:              historyV2Mgr,
		executionMgr:              executionMgr,
		domainCache:               domainCache,
		clusterMetadata:           clusterMetadata,
		config:                    config,
		logger:                    logger,
		metricsClient:             metricsClient,
		standbyClusterCurrentTime: standbyClusterCurrentTime,
		timerMaxReadLevelMap:      timerMaxReadLevelMap,
	}

	shardCtx.eventsCache = newEventsCache(shardCtx)
	return shardCtx
}

// GetShardID test implementation
func (s *TestShardContext) GetShardID() int {
	return s.shardID
}

// GetService test implementation
func (s *TestShardContext) GetService() service.Service {
	return s.service
}

// GetExecutionManager test implementation
func (s *TestShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.executionMgr
}

// GetHistoryV2Manager return historyV2
func (s *TestShardContext) GetHistoryV2Manager() persistence.HistoryV2Manager {
	return s.historyV2Mgr
}

// GetDomainCache test implementation
func (s *TestShardContext) GetDomainCache() cache.DomainCache {
	return s.domainCache
}

// GetClusterMetadata test implementation
func (s *TestShardContext) GetClusterMetadata() cluster.Metadata {
	return s.clusterMetadata
}

// GetEventsCache test implementation
func (s *TestShardContext) GetEventsCache() eventsCache {
	return s.eventsCache
}

// GetEngine test implementation
func (s *TestShardContext) GetEngine() Engine {
	return s.engine
}

// SetEngine test implementation
func (s *TestShardContext) SetEngine(engine Engine) {
	s.engine = engine
}

// GenerateTransferTaskID test implementation
func (s *TestShardContext) GenerateTransferTaskID() (int64, error) {
	return atomic.AddInt64(&s.transferSequenceNumber, 1), nil
}

// GenerateTransferTaskIDs test implementation
func (s *TestShardContext) GenerateTransferTaskIDs(number int) ([]int64, error) {
	result := []int64{}
	for i := 0; i < number; i++ {
		id, err := s.GenerateTransferTaskID()
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}

// GetTransferMaxReadLevel test implementation
func (s *TestShardContext) GetTransferMaxReadLevel() int64 {
	return atomic.LoadInt64(&s.transferSequenceNumber)
}

// GetTransferAckLevel test implementation
func (s *TestShardContext) GetTransferAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TransferAckLevel
}

// UpdateTransferAckLevel test implementation
func (s *TestShardContext) UpdateTransferAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferAckLevel = ackLevel
	return nil
}

// GetTransferClusterAckLevel test implementation
func (s *TestShardContext) GetTransferClusterAckLevel(cluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding ack level
	if ackLevel, ok := s.shardInfo.ClusterTransferAckLevel[cluster]; ok {
		return ackLevel
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	// this can happen if you add more cluster
	return s.shardInfo.TransferAckLevel
}

// UpdateTransferClusterAckLevel test implementation
func (s *TestShardContext) UpdateTransferClusterAckLevel(cluster string, ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTransferAckLevel[cluster] = ackLevel
	return nil
}

// GetReplicatorAckLevel test implementation
func (s *TestShardContext) GetReplicatorAckLevel() int64 {
	return atomic.LoadInt64(&s.shardInfo.ReplicationAckLevel)
}

// UpdateReplicatorAckLevel test implementation
func (s *TestShardContext) UpdateReplicatorAckLevel(ackLevel int64) error {
	atomic.StoreInt64(&s.shardInfo.ReplicationAckLevel, ackLevel)
	return nil
}

// GetTimerAckLevel test implementation
func (s *TestShardContext) GetTimerAckLevel() time.Time {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TimerAckLevel
}

// UpdateTimerAckLevel test implementation
func (s *TestShardContext) UpdateTimerAckLevel(ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerAckLevel = ackLevel
	return nil
}

// GetTimerClusterAckLevel test implementation
func (s *TestShardContext) GetTimerClusterAckLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding ack level
	if ackLevel, ok := s.shardInfo.ClusterTimerAckLevel[cluster]; ok {
		return ackLevel
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	// this can happen if you add more cluster
	return s.shardInfo.TimerAckLevel
}

// UpdateTimerClusterAckLevel test implementation
func (s *TestShardContext) UpdateTimerClusterAckLevel(cluster string, ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTimerAckLevel[cluster] = ackLevel
	return nil
}

// UpdateTransferFailoverLevel test implementation
func (s *TestShardContext) UpdateTransferFailoverLevel(failoverID string, level persistence.TransferFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferFailoverLevels[failoverID] = level
	return nil
}

// DeleteTransferFailoverLevel test implementation
func (s *TestShardContext) DeleteTransferFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	delete(s.shardInfo.TransferFailoverLevels, failoverID)
	return nil
}

// GetAllTransferFailoverLevels test implementation
func (s *TestShardContext) GetAllTransferFailoverLevels() map[string]persistence.TransferFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TransferFailoverLevel{}
	for k, v := range s.shardInfo.TransferFailoverLevels {
		ret[k] = v
	}
	return ret
}

// UpdateTimerFailoverLevel test implementation
func (s *TestShardContext) UpdateTimerFailoverLevel(failoverID string, level persistence.TimerFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerFailoverLevels[failoverID] = level
	return nil
}

// DeleteTimerFailoverLevel test implementation
func (s *TestShardContext) DeleteTimerFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	delete(s.shardInfo.TimerFailoverLevels, failoverID)
	return nil
}

// GetAllTimerFailoverLevels test implementation
func (s *TestShardContext) GetAllTimerFailoverLevels() map[string]persistence.TimerFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TimerFailoverLevel{}
	for k, v := range s.shardInfo.TimerFailoverLevels {
		ret[k] = v
	}
	return ret
}

// GetClusterReplicationLevel test implementation
func (s *TestShardContext) GetClusterReplicationLevel(cluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	if replicationLevel, ok := s.shardInfo.ClusterReplicationLevel[cluster]; ok {
		return replicationLevel
	}

	return -1
}

// UpdateClusterReplicationLevel test implementation
func (s *TestShardContext) UpdateClusterReplicationLevel(cluster string, lastTaskID int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterReplicationLevel[cluster] = lastTaskID
	return nil
}

// GetDomainNotificationVersion test implementation
func (s *TestShardContext) GetDomainNotificationVersion() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.DomainNotificationVersion
}

// UpdateDomainNotificationVersion test implementation
func (s *TestShardContext) UpdateDomainNotificationVersion(domainNotificationVersion int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.DomainNotificationVersion = domainNotificationVersion
	return nil
}

// CreateWorkflowExecution test implementation
func (s *TestShardContext) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse, error) {
	return s.executionMgr.CreateWorkflowExecution(request)
}

// UpdateWorkflowExecution test implementation
func (s *TestShardContext) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
	// assign IDs for the timer tasks. They need to be assigned under shard lock.
	clusterMetadata := s.GetService().GetClusterMetadata()
	clusterName := clusterMetadata.GetCurrentClusterName()
	for _, task := range request.UpdateWorkflowMutation.TimerTasks {
		ts := task.GetVisibilityTimestamp()
		if task.GetVersion() != common.EmptyVersion {
			clusterName = clusterMetadata.ClusterNameForFailoverVersion(task.GetVersion())
		}
		if ts.Before(s.timerMaxReadLevelMap[clusterName]) {
			// This can happen if shard move and new host have a time SKU, or there is db write delay.
			// We generate a new timer ID using timerMaxReadLevel.
			s.logger.Debug(fmt.Sprintf("%v: New timer generated is less than read level. timestamp: %v, timerMaxReadLevel: %v",
				time.Now(), ts, s.timerMaxReadLevelMap[clusterName]), tag.ClusterName(clusterName))
			task.SetVisibilityTimestamp(s.timerMaxReadLevelMap[clusterName].Add(time.Millisecond))
		}
		seqID, err := s.GenerateTransferTaskID()
		if err != nil {
			panic(err)
		}
		task.SetTaskID(seqID)
		visibilityTs := task.GetVisibilityTimestamp()
		s.logger.Info(fmt.Sprintf("%v: TestShardContext: Assigning timer (timestamp: %v, seq: %v)",
			time.Now().UTC(), visibilityTs, task.GetTaskID()))
	}
	resp, err := s.executionMgr.UpdateWorkflowExecution(request)
	return resp, err
}

// UpdateTimerMaxReadLevel test implementation
func (s *TestShardContext) UpdateTimerMaxReadLevel(cluster string) time.Time {
	s.Lock()
	defer s.Unlock()

	currentTime := time.Now()
	if cluster != "" && cluster != s.GetService().GetClusterMetadata().GetCurrentClusterName() {
		currentTime = s.standbyClusterCurrentTime[cluster]
	}

	s.timerMaxReadLevelMap[cluster] = currentTime.Add(s.GetConfig().TimerProcessorMaxTimeShift())
	return s.timerMaxReadLevelMap[cluster]
}

// GetTimerMaxReadLevel test implementation
func (s *TestShardContext) GetTimerMaxReadLevel(cluster string) time.Time {
	// This test method is called with shard lock
	return s.timerMaxReadLevelMap[cluster]
}

// ConflictResolveWorkflowExecution test implementation
func (s *TestShardContext) ConflictResolveWorkflowExecution(request *persistence.ConflictResolveWorkflowExecutionRequest) error {
	return s.executionMgr.ConflictResolveWorkflowExecution(request)
}

// ResetWorkflowExecution test implementation
func (s *TestShardContext) ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error {
	return s.executionMgr.ResetWorkflowExecution(request)
}

// AppendHistoryV2Events append history V2 events
func (s *TestShardContext) AppendHistoryV2Events(
	request *persistence.AppendHistoryNodesRequest, domainID string, execution shared.WorkflowExecution) (int, error) {
	request.ShardID = common.IntPtr(s.shardID)
	resp, err := s.historyV2Mgr.AppendHistoryNodes(request)
	if err != nil {
		return 0, err
	}
	return resp.Size, nil
}

// GetConfig test implementation
func (s *TestShardContext) GetConfig() *Config {
	return s.config
}

// GetLogger test implementation
func (s *TestShardContext) GetLogger() log.Logger {
	return s.logger
}

// GetThrottledLogger returns a throttled logger
func (s *TestShardContext) GetThrottledLogger() log.Logger {
	return s.logger
}

// GetMetricsClient test implementation
func (s *TestShardContext) GetMetricsClient() metrics.Client {
	return s.metricsClient
}

// Reset test implementation
func (s *TestShardContext) Reset() {
	atomic.StoreInt64(&s.shardInfo.RangeID, 0)
	atomic.StoreInt64(&s.shardInfo.TransferAckLevel, 0)
}

// GetRangeID test implementation
func (s *TestShardContext) GetRangeID() int64 {
	return atomic.LoadInt64(&s.shardInfo.RangeID)
}

// GetTimeSource test implementation
func (s *TestShardContext) GetTimeSource() clock.TimeSource {
	return clock.NewRealTimeSource()
}

// SetCurrentTime test implementation
func (s *TestShardContext) SetCurrentTime(cluster string, currentTime time.Time) {
	s.Lock()
	defer s.Unlock()
	if cluster != s.GetService().GetClusterMetadata().GetCurrentClusterName() {
		prevTime := s.standbyClusterCurrentTime[cluster]
		if prevTime.Before(currentTime) {
			s.standbyClusterCurrentTime[cluster] = currentTime
		}
	} else {
		panic("Cannot set current time for current cluster")
	}
}

// GetCurrentTime test implementation
func (s *TestShardContext) GetCurrentTime(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()
	if cluster != s.GetService().GetClusterMetadata().GetCurrentClusterName() {
		return s.standbyClusterCurrentTime[cluster]
	}
	return time.Now()
}

// NewDynamicConfigForTest return dc for test
func NewDynamicConfigForTest() *Config {
	dc := dynamicconfig.NewNopCollection()
	config := NewConfig(dc, 1, cconfig.StoreTypeCassandra, false)
	// reduce the duration of long poll to increase test speed
	config.LongPollExpirationInterval = dc.GetDurationPropertyFilteredByDomain(dynamicconfig.HistoryLongPollExpirationInterval, 10*time.Second)
	return config
}

// SetupWorkflowStore to setup workflow test base
func (s *TestBase) SetupWorkflowStore() {
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup()
	log := loggerimpl.NewDevelopmentForTest(s.Suite)
	config := NewDynamicConfigForTest()
	clusterMetadata := cluster.GetTestClusterMetadata(false, false)
	s.ShardContext = newTestShardContext(s.ShardInfo, 0,
		s.HistoryV2Mgr, s.ExecutionManager, s.MetadataManager, clusterMetadata, nil, config, log)
	s.TestBase.TaskIDGenerator = s.ShardContext
}

// SetupDomains setup the domains used for testing
func (s *TestBase) SetupDomains() {
	// create the domains which are active / standby
	version := int64(0)
	if !testDomainIsGlobalDomain {
		version = common.EmptyVersion
	}
	createDomainRequest := &persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:     testDomainActiveID,
			Name:   testDomainActiveName,
			Status: persistence.DomainStatusRegistered,
		},
		Config: &persistence.DomainConfig{
			Retention:  testDomainRetention,
			EmitMetric: testDomainEmitMetric,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: testDomainActiveClusterName,
			Clusters:          testDomainAllClusters,
		},
		IsGlobalDomain:  testDomainIsGlobalDomain,
		FailoverVersion: version,
	}
	_, err := s.MetadataManager.CreateDomain(createDomainRequest)
	if err != nil {
		s.Fail(err.Error())
	}
	createDomainRequest.Info.ID = testDomainStandbyID
	createDomainRequest.Info.Name = testDomainStandbyName
	createDomainRequest.ReplicationConfig.ActiveClusterName = testDomainStandbyClusterName
	_, err = s.MetadataManager.CreateDomain(createDomainRequest)
	if err != nil {
		s.Fail(err.Error())
	}
	s.ShardContext.domainCache.Start()
}

// TeardownDomains delete the domains used for testing
func (s *TestBase) TeardownDomains() {
	s.ShardContext.domainCache.Stop()
	err := s.MetadataManager.DeleteDomain(&persistence.DeleteDomainRequest{ID: testDomainActiveID})
	if err != nil {
		s.Fail(err.Error())
	}
	err = s.MetadataManager.DeleteDomain(&persistence.DeleteDomainRequest{ID: testDomainStandbyID})
	if err != nil {
		s.Fail(err.Error())
	}
}
