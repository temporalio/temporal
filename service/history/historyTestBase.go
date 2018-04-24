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
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
	testDatacenter           = ""
	testSchemaDir            = "../.."
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
	testDomainIsGlobalDomain     = true
	testDomainAllClusters        = []*persistence.ClusterReplicationConfig{
		&persistence.ClusterReplicationConfig{ClusterName: testDomainActiveClusterName},
		&persistence.ClusterReplicationConfig{ClusterName: testDomainStandbyClusterName},
	}
)

type (
	// TestShardContext shard context for testing.
	TestShardContext struct {
		sync.RWMutex
		service                   service.Service
		shardInfo                 *persistence.ShardInfo
		transferSequenceNumber    int64
		historyMgr                persistence.HistoryManager
		executionMgr              persistence.ExecutionManager
		domainCache               cache.DomainCache
		config                    *Config
		logger                    bark.Logger
		metricsClient             metrics.Client
		standbyClusterCurrentTime map[string]time.Time
	}

	// TestBase wraps the base setup needed to create workflows over engine layer.
	TestBase struct {
		persistence.TestBase
		ShardContext *TestShardContext
	}
)

var _ ShardContext = (*TestShardContext)(nil)

func newTestShardContext(shardInfo *persistence.ShardInfo, transferSequenceNumber int64,
	historyMgr persistence.HistoryManager, executionMgr persistence.ExecutionManager,
	metadataMgr persistence.MetadataManager, clusterMetadata cluster.Metadata, config *Config,
	logger bark.Logger) *TestShardContext {
	domainCache := cache.NewDomainCache(metadataMgr, clusterMetadata, logger)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)

	// initialize the cluster current time to be the same as ack level
	standbyClusterCurrentTime := make(map[string]time.Time)
	for clusterName := range clusterMetadata.GetAllClusterFailoverVersions() {
		if clusterName != clusterMetadata.GetCurrentClusterName() {
			if currentTime, ok := shardInfo.ClusterTimerAckLevel[clusterName]; ok {
				standbyClusterCurrentTime[clusterName] = currentTime
			} else {
				standbyClusterCurrentTime[clusterName] = shardInfo.TimerAckLevel
			}
		}
	}

	return &TestShardContext{
		service:                   service.NewTestService(clusterMetadata, nil, metricsClient, logger),
		shardInfo:                 shardInfo,
		transferSequenceNumber:    transferSequenceNumber,
		historyMgr:                historyMgr,
		executionMgr:              executionMgr,
		domainCache:               domainCache,
		config:                    config,
		logger:                    logger,
		metricsClient:             metricsClient,
		standbyClusterCurrentTime: standbyClusterCurrentTime,
	}
}

// GetService test implementation
func (s *TestShardContext) GetService() service.Service {
	return s.service
}

// GetExecutionManager test implementation
func (s *TestShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.executionMgr
}

// GetHistoryManager test implementation
func (s *TestShardContext) GetHistoryManager() persistence.HistoryManager {
	return s.historyMgr
}

// GetDomainCache test implementation
func (s *TestShardContext) GetDomainCache() cache.DomainCache {
	return s.domainCache
}

// GetNextTransferTaskID test implementation
func (s *TestShardContext) GetNextTransferTaskID() (int64, error) {
	return atomic.AddInt64(&s.transferSequenceNumber, 1), nil
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

// CreateWorkflowExecution test implementation
func (s *TestShardContext) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse, error) {
	return s.executionMgr.CreateWorkflowExecution(request)
}

// UpdateWorkflowExecution test implementation
func (s *TestShardContext) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error {
	// assign IDs for the timer tasks. They need to be assigned under shard lock.
	for _, task := range request.TimerTasks {
		seqID, err := s.GetNextTransferTaskID()
		if err != nil {
			panic(err)
		}
		task.SetTaskID(seqID)
		s.logger.Infof("%v: TestShardContext: Assigning timer (timestamp: %v, seq: %v)",
			time.Now().UTC(), persistence.GetVisibilityTSFrom(task).UTC(), task.GetTaskID())
	}
	return s.executionMgr.UpdateWorkflowExecution(request)
}

// AppendHistoryEvents test implementation
func (s *TestShardContext) AppendHistoryEvents(request *persistence.AppendHistoryEventsRequest) error {
	return s.historyMgr.AppendHistoryEvents(request)
}

// NotifyNewHistoryEvent test implementation
func (s *TestShardContext) NotifyNewHistoryEvent(event *historyEventNotification) error {
	return nil
}

// GetConfig test implementation
func (s *TestShardContext) GetConfig() *Config {
	return s.config
}

// GetLogger test implementation
func (s *TestShardContext) GetLogger() bark.Logger {
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
func (s *TestShardContext) GetTimeSource() common.TimeSource {
	return common.NewRealTimeSource()
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

// SetupWorkflowStoreWithOptions to setup workflow test base
func (s *TestBase) SetupWorkflowStoreWithOptions(options persistence.TestBaseOptions) {
	s.TestBase.SetupWorkflowStoreWithOptions(options)
	log := bark.NewLoggerFromLogrus(log.New())
	config := NewConfig(dynamicconfig.NewNopCollection(), 1)
	clusterMetadata := cluster.GetTestClusterMetadata(options.EnableGlobalDomain, options.IsMasterCluster)
	s.ShardContext = newTestShardContext(s.ShardInfo, 0, s.HistoryMgr, s.WorkflowMgr, s.MetadataManager, clusterMetadata,
		config, log)
	s.TestBase.TaskIDGenerator = s.ShardContext
}

// SetupWorkflowStore to setup workflow test base
func (s *TestBase) SetupWorkflowStore() {
	s.TestBase.SetupWorkflowStore()
	log := bark.NewLoggerFromLogrus(log.New())
	config := NewConfig(dynamicconfig.NewNopCollection(), 1)
	clusterMetadata := cluster.GetTestClusterMetadata(false, false)
	s.ShardContext = newTestShardContext(s.ShardInfo, 0, s.HistoryMgr, s.WorkflowMgr, s.MetadataManager, clusterMetadata,
		config, log)
	s.TestBase.TaskIDGenerator = s.ShardContext
}

// SetupDomains setup the domains used for testing
func (s *TestBase) SetupDomains() {
	// create the domains which are active / standby
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
		IsGlobalDomain: testDomainIsGlobalDomain,
	}
	s.MetadataManager.CreateDomain(createDomainRequest)
	createDomainRequest.Info.ID = testDomainStandbyID
	createDomainRequest.Info.Name = testDomainStandbyName
	createDomainRequest.ReplicationConfig.ActiveClusterName = testDomainStandbyClusterName
	s.MetadataManager.CreateDomain(createDomainRequest)
}

// TeardownDomains delete the domains used for testing
func (s *TestBase) TeardownDomains() {
	s.MetadataManager.DeleteDomain(&persistence.DeleteDomainRequest{ID: testDomainActiveID})
	s.MetadataManager.DeleteDomain(&persistence.DeleteDomainRequest{ID: testDomainStandbyID})
}
