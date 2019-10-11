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

package dynamicconfig

// Key represents a key/property stored in dynamic config
type Key int

func (k Key) String() string {
	keyName, ok := keys[k]
	if !ok {
		return keys[unknownKey]
	}
	return keyName
}

// Mapping from Key to keyName, where keyName are used dynamic config source.
var keys = map[Key]string{
	unknownKey: "unknownKey",

	// tests keys
	testGetPropertyKey:                               "testGetPropertyKey",
	testGetIntPropertyKey:                            "testGetIntPropertyKey",
	testGetFloat64PropertyKey:                        "testGetFloat64PropertyKey",
	testGetDurationPropertyKey:                       "testGetDurationPropertyKey",
	testGetBoolPropertyKey:                           "testGetBoolPropertyKey",
	testGetStringPropertyKey:                         "testGetStringPropertyKey",
	testGetMapPropertyKey:                            "testGetMapPropertyKey",
	testGetIntPropertyFilteredByDomainKey:            "testGetIntPropertyFilteredByDomainKey",
	testGetDurationPropertyFilteredByDomainKey:       "testGetDurationPropertyFilteredByDomainKey",
	testGetIntPropertyFilteredByTaskListInfoKey:      "testGetIntPropertyFilteredByTaskListInfoKey",
	testGetDurationPropertyFilteredByTaskListInfoKey: "testGetDurationPropertyFilteredByTaskListInfoKey",
	testGetBoolPropertyFilteredByTaskListInfoKey:     "testGetBoolPropertyFilteredByTaskListInfoKey",

	// system settings
	EnableGlobalDomain:                  "system.enableGlobalDomain",
	EnableNDC:                           "system.enableNDC",
	EnableNewKafkaClient:                "system.enableNewKafkaClient",
	EnableVisibilitySampling:            "system.enableVisibilitySampling",
	EnableReadFromClosedExecutionV2:     "system.enableReadFromClosedExecutionV2",
	AdvancedVisibilityWritingMode:       "system.advancedVisibilityWritingMode",
	EnableReadVisibilityFromES:          "system.enableReadVisibilityFromES",
	HistoryArchivalStatus:               "system.historyArchivalStatus",
	EnableReadFromHistoryArchival:       "system.enableReadFromHistoryArchival",
	VisibilityArchivalStatus:            "system.visibilityArchivalStatus",
	EnableReadFromVisibilityArchival:    "system.enableReadFromVisibilityArchival",
	EnableDomainNotActiveAutoForwarding: "system.enableDomainNotActiveAutoForwarding",
	TransactionSizeLimit:                "system.transactionSizeLimit",
	MinRetentionDays:                    "system.minRetentionDays",
	MaxDecisionStartToCloseSeconds:      "system.maxDecisionStartToCloseSeconds",
	EnableBatcher:                       "worker.enableBatcher",
	EnableParentClosePolicyWorker:       "system.enableParentClosePolicyWorker",

	// size limit
	BlobSizeLimitError:     "limit.blobSize.error",
	BlobSizeLimitWarn:      "limit.blobSize.warn",
	HistorySizeLimitError:  "limit.historySize.error",
	HistorySizeLimitWarn:   "limit.historySize.warn",
	HistoryCountLimitError: "limit.historyCount.error",
	HistoryCountLimitWarn:  "limit.historyCount.warn",
	MaxIDLengthLimit:       "limit.maxIDLength",

	// frontend settings
	FrontendPersistenceMaxQPS:         "frontend.persistenceMaxQPS",
	FrontendVisibilityMaxPageSize:     "frontend.visibilityMaxPageSize",
	FrontendVisibilityListMaxQPS:      "frontend.visibilityListMaxQPS",
	FrontendESVisibilityListMaxQPS:    "frontend.esVisibilityListMaxQPS",
	FrontendMaxBadBinaries:            "frontend.maxBadBinaries",
	FrontendESIndexMaxResultWindow:    "frontend.esIndexMaxResultWindow",
	FrontendHistoryMaxPageSize:        "frontend.historyMaxPageSize",
	FrontendRPS:                       "frontend.rps",
	FrontendDomainRPS:                 "frontend.domainrps",
	FrontendHistoryMgrNumConns:        "frontend.historyMgrNumConns",
	DisableListVisibilityByFilter:     "frontend.disableListVisibilityByFilter",
	FrontendThrottledLogRPS:           "frontend.throttledLogRPS",
	EnableClientVersionCheck:          "frontend.enableClientVersionCheck",
	ValidSearchAttributes:             "frontend.validSearchAttributes",
	SearchAttributesNumberOfKeysLimit: "frontend.searchAttributesNumberOfKeysLimit",
	SearchAttributesSizeOfValueLimit:  "frontend.searchAttributesSizeOfValueLimit",
	SearchAttributesTotalSizeLimit:    "frontend.searchAttributesTotalSizeLimit",

	// matching settings
	MatchingRPS:                             "matching.rps",
	MatchingPersistenceMaxQPS:               "matching.persistenceMaxQPS",
	MatchingMinTaskThrottlingBurstSize:      "matching.minTaskThrottlingBurstSize",
	MatchingGetTasksBatchSize:               "matching.getTasksBatchSize",
	MatchingLongPollExpirationInterval:      "matching.longPollExpirationInterval",
	MatchingEnableSyncMatch:                 "matching.enableSyncMatch",
	MatchingUpdateAckInterval:               "matching.updateAckInterval",
	MatchingIdleTasklistCheckInterval:       "matching.idleTasklistCheckInterval",
	MaxTasklistIdleTime:                     "matching.maxTasklistIdleTime",
	MatchingOutstandingTaskAppendsThreshold: "matching.outstandingTaskAppendsThreshold",
	MatchingMaxTaskBatchSize:                "matching.maxTaskBatchSize",
	MatchingMaxTaskDeleteBatchSize:          "matching.maxTaskDeleteBatchSize",
	MatchingThrottledLogRPS:                 "matching.throttledLogRPS",
	MatchingNumTasklistWritePartitions:      "matching.numTasklistWritePartitions",
	MatchingNumTasklistReadPartitions:       "matching.numTasklistReadPartitions",
	MatchingForwarderMaxOutstandingPolls:    "matching.forwarderMaxOutstandingPolls",
	MatchingForwarderMaxOutstandingTasks:    "matching.forwarderMaxOutstandingTasks",
	MatchingForwarderMaxRatePerSecond:       "matching.forwarderMaxRatePerSecond",
	MatchingForwarderMaxChildrenPerNode:     "matching.forwarderMaxChildrenPerNode",

	// history settings
	HistoryRPS:                                            "history.rps",
	HistoryPersistenceMaxQPS:                              "history.persistenceMaxQPS",
	HistoryVisibilityOpenMaxQPS:                           "history.historyVisibilityOpenMaxQPS",
	HistoryVisibilityClosedMaxQPS:                         "history.historyVisibilityClosedMaxQPS",
	HistoryLongPollExpirationInterval:                     "history.longPollExpirationInterval",
	HistoryCacheInitialSize:                               "history.cacheInitialSize",
	HistoryMaxAutoResetPoints:                             "history.historyMaxAutoResetPoints",
	HistoryCacheMaxSize:                                   "history.cacheMaxSize",
	HistoryCacheTTL:                                       "history.cacheTTL",
	EventsCacheInitialSize:                                "history.eventsCacheInitialSize",
	EventsCacheMaxSize:                                    "history.eventsCacheMaxSize",
	EventsCacheTTL:                                        "history.eventsCacheTTL",
	AcquireShardInterval:                                  "history.acquireShardInterval",
	StandbyClusterDelay:                                   "history.standbyClusterDelay",
	TimerTaskBatchSize:                                    "history.timerTaskBatchSize",
	TimerTaskWorkerCount:                                  "history.timerTaskWorkerCount",
	TimerTaskMaxRetryCount:                                "history.timerTaskMaxRetryCount",
	TimerProcessorGetFailureRetryCount:                    "history.timerProcessorGetFailureRetryCount",
	TimerProcessorCompleteTimerFailureRetryCount:          "history.timerProcessorCompleteTimerFailureRetryCount",
	TimerProcessorUpdateShardTaskCount:                    "history.timerProcessorUpdateShardTaskCount",
	TimerProcessorUpdateAckInterval:                       "history.timerProcessorUpdateAckInterval",
	TimerProcessorUpdateAckIntervalJitterCoefficient:      "history.timerProcessorUpdateAckIntervalJitterCoefficient",
	TimerProcessorCompleteTimerInterval:                   "history.timerProcessorCompleteTimerInterval",
	TimerProcessorFailoverMaxPollRPS:                      "history.timerProcessorFailoverMaxPollRPS",
	TimerProcessorMaxPollRPS:                              "history.timerProcessorMaxPollRPS",
	TimerProcessorMaxPollInterval:                         "history.timerProcessorMaxPollInterval",
	TimerProcessorMaxPollIntervalJitterCoefficient:        "history.timerProcessorMaxPollIntervalJitterCoefficient",
	TimerProcessorMaxTimeShift:                            "history.timerProcessorMaxTimeShift",
	TimerProcessorHistoryArchivalSizeLimit:                "history.timerProcessorHistoryArchivalSizeLimit",
	TimerProcessorArchivalTimeLimit:                       "history.TimerProcessorArchivalTimeLimit",
	TransferTaskBatchSize:                                 "history.transferTaskBatchSize",
	TransferProcessorFailoverMaxPollRPS:                   "history.transferProcessorFailoverMaxPollRPS",
	TransferProcessorMaxPollRPS:                           "history.transferProcessorMaxPollRPS",
	TransferTaskWorkerCount:                               "history.transferTaskWorkerCount",
	TransferTaskMaxRetryCount:                             "history.transferTaskMaxRetryCount",
	TransferProcessorCompleteTransferFailureRetryCount:    "history.transferProcessorCompleteTransferFailureRetryCount",
	TransferProcessorUpdateShardTaskCount:                 "history.transferProcessorUpdateShardTaskCount",
	TransferProcessorMaxPollInterval:                      "history.transferProcessorMaxPollInterval",
	TransferProcessorMaxPollIntervalJitterCoefficient:     "history.transferProcessorMaxPollIntervalJitterCoefficient",
	TransferProcessorUpdateAckInterval:                    "history.transferProcessorUpdateAckInterval",
	TransferProcessorUpdateAckIntervalJitterCoefficient:   "history.transferProcessorUpdateAckIntervalJitterCoefficient",
	TransferProcessorCompleteTransferInterval:             "history.transferProcessorCompleteTransferInterval",
	TransferProcessorVisibilityArchivalTimeLimit:          "history.transferProcessorVisibilityArchivalTimeLimit",
	ReplicatorTaskBatchSize:                               "history.replicatorTaskBatchSize",
	ReplicatorTaskWorkerCount:                             "history.replicatorTaskWorkerCount",
	ReplicatorTaskMaxRetryCount:                           "history.replicatorTaskMaxRetryCount",
	ReplicatorProcessorMaxPollRPS:                         "history.replicatorProcessorMaxPollRPS",
	ReplicatorProcessorUpdateShardTaskCount:               "history.replicatorProcessorUpdateShardTaskCount",
	ReplicatorProcessorMaxPollInterval:                    "history.replicatorProcessorMaxPollInterval",
	ReplicatorProcessorMaxPollIntervalJitterCoefficient:   "history.replicatorProcessorMaxPollIntervalJitterCoefficient",
	ReplicatorProcessorUpdateAckInterval:                  "history.replicatorProcessorUpdateAckInterval",
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient: "history.replicatorProcessorUpdateAckIntervalJitterCoefficient",
	ExecutionMgrNumConns:                                  "history.executionMgrNumConns",
	HistoryMgrNumConns:                                    "history.historyMgrNumConns",
	MaximumBufferedEventsBatch:                            "history.maximumBufferedEventsBatch",
	MaximumSignalsPerExecution:                            "history.maximumSignalsPerExecution",
	ShardUpdateMinInterval:                                "history.shardUpdateMinInterval",
	ShardSyncMinInterval:                                  "history.shardSyncMinInterval",
	DefaultEventEncoding:                                  "history.defaultEventEncoding",
	EnableAdminProtection:                                 "history.enableAdminProtection",
	AdminOperationToken:                                   "history.adminOperationToken",
	EnableParentClosePolicy:                               "history.enableParentClosePolicy",
	NumArchiveSystemWorkflows:                             "history.numArchiveSystemWorkflows",
	ArchiveRequestRPS:                                     "history.archiveRequestRPS",
	EmitShardDiffLog:                                      "history.emitShardDiffLog",
	HistoryThrottledLogRPS:                                "history.throttledLogRPS",
	StickyTTL:                                             "history.stickyTTL",
	DecisionHeartbeatTimeout:                              "history.decisionHeartbeatTimeout",
	ParentClosePolicyThreshold:                            "history.parentClosePolicyThreshold",
	NumParentClosePolicySystemWorkflows:                   "history.numParentClosePolicySystemWorkflows",

	WorkerPersistenceMaxQPS:                         "worker.persistenceMaxQPS",
	WorkerReplicatorMetaTaskConcurrency:             "worker.replicatorMetaTaskConcurrency",
	WorkerReplicatorTaskConcurrency:                 "worker.replicatorTaskConcurrency",
	WorkerReplicatorMessageConcurrency:              "worker.replicatorMessageConcurrency",
	WorkerReplicatorActivityBufferRetryCount:        "worker.replicatorActivityBufferRetryCount",
	WorkerReplicatorHistoryBufferRetryCount:         "worker.replicatorHistoryBufferRetryCount",
	WorkerReplicationTaskMaxRetryCount:              "worker.replicationTaskMaxRetryCount",
	WorkerReplicationTaskMaxRetryDuration:           "worker.replicationTaskMaxRetryDuration",
	WorkerIndexerConcurrency:                        "worker.indexerConcurrency",
	WorkerESProcessorNumOfWorkers:                   "worker.ESProcessorNumOfWorkers",
	WorkerESProcessorBulkActions:                    "worker.ESProcessorBulkActions",
	WorkerESProcessorBulkSize:                       "worker.ESProcessorBulkSize",
	WorkerESProcessorFlushInterval:                  "worker.ESProcessorFlushInterval",
	EnableArchivalCompression:                       "worker.EnableArchivalCompression",
	WorkerHistoryPageSize:                           "worker.WorkerHistoryPageSize",
	WorkerTargetArchivalBlobSize:                    "worker.WorkerTargetArchivalBlobSize",
	WorkerArchiverConcurrency:                       "worker.ArchiverConcurrency",
	WorkerArchivalsPerIteration:                     "worker.ArchivalsPerIteration",
	WorkerDeterministicConstructionCheckProbability: "worker.DeterministicConstructionCheckProbability",
	WorkerBlobIntegrityCheckProbability:             "worker.BlobIntegrityCheckProbability",
	WorkerTimeLimitPerArchivalIteration:             "worker.TimeLimitPerArchivalIteration",
	WorkerThrottledLogRPS:                           "worker.throttledLogRPS",
	ScannerPersistenceMaxQPS:                        "worker.scannerPersistenceMaxQPS",
}

const (
	unknownKey Key = iota

	// key for tests
	testGetPropertyKey
	testGetIntPropertyKey
	testGetFloat64PropertyKey
	testGetDurationPropertyKey
	testGetBoolPropertyKey
	testGetStringPropertyKey
	testGetMapPropertyKey
	testGetIntPropertyFilteredByDomainKey
	testGetDurationPropertyFilteredByDomainKey
	testGetIntPropertyFilteredByTaskListInfoKey
	testGetDurationPropertyFilteredByTaskListInfoKey
	testGetBoolPropertyFilteredByTaskListInfoKey

	// EnableGlobalDomain is key for enable global domain
	EnableGlobalDomain
	// EnableNDC is key for enable N data center events replication
	EnableNDC
	// EnableNewKafkaClient is key for using New Kafka client
	EnableNewKafkaClient
	// EnableVisibilitySampling is key for enable visibility sampling
	EnableVisibilitySampling
	// EnableReadFromClosedExecutionV2 is key for enable read from cadence_visibility.closed_executions_v2
	EnableReadFromClosedExecutionV2
	// AdvancedVisibilityWritingMode is key for how to write to advanced visibility
	AdvancedVisibilityWritingMode
	// EmitShardDiffLog whether emit the shard diff log
	EmitShardDiffLog
	// EnableReadVisibilityFromES is key for enable read from elastic search
	EnableReadVisibilityFromES
	// DisableListVisibilityByFilter is config to disable list open/close workflow using filter
	DisableListVisibilityByFilter
	// HistoryArchivalStatus is key for the status of history archival
	HistoryArchivalStatus
	// EnableReadFromHistoryArchival is key for enabling reading history from archival store
	EnableReadFromHistoryArchival
	// VisibilityArchivalStatus is key for the status of visibility archival
	VisibilityArchivalStatus
	// EnableReadFromVisibilityArchival is key for enabling reading visibility from archival store
	EnableReadFromVisibilityArchival
	// EnableDomainNotActiveAutoForwarding whether enabling DC auto forwarding to active cluster
	// for signal / start / signal with start API if domain is not active
	EnableDomainNotActiveAutoForwarding
	// TransactionSizeLimit is the largest allowed transaction size to persistence
	TransactionSizeLimit
	// MinRetentionDays is the minimal allowed retention days for domain
	MinRetentionDays
	// MaxDecisionStartToCloseSeconds is the minimal allowed decision start to close timeout in seconds
	MaxDecisionStartToCloseSeconds

	// BlobSizeLimitError is the per event blob size limit
	BlobSizeLimitError
	// BlobSizeLimitWarn is the per event blob size limit for warning
	BlobSizeLimitWarn
	// HistorySizeLimitError is the per workflow execution history size limit
	HistorySizeLimitError
	// HistorySizeLimitWarn is the per workflow execution history size limit for warning
	HistorySizeLimitWarn
	// HistoryCountLimitError is the per workflow execution history event count limit
	HistoryCountLimitError
	// HistoryCountLimitWarn is the per workflow execution history event count limit for warning
	HistoryCountLimitWarn

	// MaxIDLengthLimit is the length limit for various IDs, including: Domain, TaskList, WorkflowID, ActivityID, TimerID,
	// WorkflowType, ActivityType, SignalName, MarkerName, ErrorReason/FailureReason/CancelCause, Identity, RequestID
	MaxIDLengthLimit

	// key for frontend

	// FrontendPersistenceMaxQPS is the max qps frontend host can query DB
	FrontendPersistenceMaxQPS
	// FrontendVisibilityMaxPageSize is default max size for ListWorkflowExecutions in one page
	FrontendVisibilityMaxPageSize
	// FrontendVisibilityListMaxQPS is max qps frontend can list open/close workflows
	FrontendVisibilityListMaxQPS
	// FrontendESVisibilityListMaxQPS is max qps frontend can list open/close workflows from ElasticSearch
	FrontendESVisibilityListMaxQPS
	// FrontendESIndexMaxResultWindow is ElasticSearch index setting max_result_window
	FrontendESIndexMaxResultWindow
	// FrontendHistoryMaxPageSize is default max size for GetWorkflowExecutionHistory in one page
	FrontendHistoryMaxPageSize
	// FrontendRPS is workflow rate limit per second
	FrontendRPS
	// FrontendDomainRPS is workflow domain rate limit per second
	FrontendDomainRPS
	// FrontendHistoryMgrNumConns is for persistence cluster.NumConns
	FrontendHistoryMgrNumConns
	// FrontendThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	FrontendThrottledLogRPS
	// EnableClientVersionCheck enables client version check for frontend
	EnableClientVersionCheck
	// FrontendMaxBadBinaries is the max number of bad binaries in domain config
	FrontendMaxBadBinaries
	// ValidSearchAttributes is legal indexed keys that can be used in list APIs
	ValidSearchAttributes
	// SearchAttributesNumberOfKeysLimit is the limit of number of keys
	SearchAttributesNumberOfKeysLimit
	// SearchAttributesSizeOfValueLimit is the size limit of each value
	SearchAttributesSizeOfValueLimit
	// SearchAttributesTotalSizeLimit is the size limit of the whole map
	SearchAttributesTotalSizeLimit

	// key for matching

	// MatchingRPS is request rate per second for each matching host
	MatchingRPS
	// MatchingPersistenceMaxQPS is the max qps matching host can query DB
	MatchingPersistenceMaxQPS
	// MatchingMinTaskThrottlingBurstSize is the minimum burst size for task list throttling
	MatchingMinTaskThrottlingBurstSize
	// MatchingGetTasksBatchSize is the maximum batch size to fetch from the task buffer
	MatchingGetTasksBatchSize
	// MatchingLongPollExpirationInterval is the long poll expiration interval in the matching service
	MatchingLongPollExpirationInterval
	// MatchingEnableSyncMatch is to enable sync match
	MatchingEnableSyncMatch
	// MatchingUpdateAckInterval is the interval for update ack
	MatchingUpdateAckInterval
	// MatchingIdleTasklistCheckInterval is the IdleTasklistCheckInterval
	MatchingIdleTasklistCheckInterval
	// MaxTasklistIdleTime is the max time tasklist being idle
	MaxTasklistIdleTime
	// MatchingOutstandingTaskAppendsThreshold is the threshold for outstanding task appends
	MatchingOutstandingTaskAppendsThreshold
	// MatchingMaxTaskBatchSize is max batch size for task writer
	MatchingMaxTaskBatchSize
	// MatchingMaxTaskDeleteBatchSize is the max batch size for range deletion of tasks
	MatchingMaxTaskDeleteBatchSize
	// MatchingThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	MatchingThrottledLogRPS
	// MatchingNumTasklistWritePartitions is the number of write partitions for a task list
	MatchingNumTasklistWritePartitions
	// MatchingNumTasklistReadPartitions is the number of read partitions for a task list
	MatchingNumTasklistReadPartitions
	// MatchingForwarderMaxOutstandingPolls is the max number of inflight polls from the forwarder
	MatchingForwarderMaxOutstandingPolls
	// MatchingForwarderMaxOutstandingTasks is the max number of inflight addTask/queryTask from the forwarder
	MatchingForwarderMaxOutstandingTasks
	// MatchingForwarderMaxRatePerSecond is the max rate at which add/query can be forwarded
	MatchingForwarderMaxRatePerSecond
	// MatchingForwarderMaxChildrenPerNode is the max number of children per node in the task list partition tree
	MatchingForwarderMaxChildrenPerNode

	// key for history

	// HistoryRPS is request rate per second for each history host
	HistoryRPS
	// HistoryPersistenceMaxQPS is the max qps history host can query DB
	HistoryPersistenceMaxQPS
	// HistoryVisibilityOpenMaxQPS is max qps one history host can write visibility open_executions
	HistoryVisibilityOpenMaxQPS
	// HistoryVisibilityClosedMaxQPS is max qps one history host can write visibility closed_executions
	HistoryVisibilityClosedMaxQPS
	// HistoryLongPollExpirationInterval is the long poll expiration interval in the history service
	HistoryLongPollExpirationInterval
	// HistoryCacheInitialSize is initial size of history cache
	HistoryCacheInitialSize
	// HistoryCacheMaxSize is max size of history cache
	HistoryCacheMaxSize
	// HistoryCacheTTL is TTL of history cache
	HistoryCacheTTL
	// EventsCacheInitialSize is initial size of events cache
	EventsCacheInitialSize
	// EventsCacheMaxSize is max size of events cache
	EventsCacheMaxSize
	// EventsCacheTTL is TTL of events cache
	EventsCacheTTL
	// AcquireShardInterval is interval that timer used to acquire shard
	AcquireShardInterval
	// StandbyClusterDelay is the atrificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay
	// TimerTaskBatchSize is batch size for timer processor to process tasks
	TimerTaskBatchSize
	// TimerTaskWorkerCount is number of task workers for timer processor
	TimerTaskWorkerCount
	// TimerTaskMaxRetryCount is max retry count for timer processor
	TimerTaskMaxRetryCount
	// TimerProcessorGetFailureRetryCount is retry count for timer processor get failure operation
	TimerProcessorGetFailureRetryCount
	// TimerProcessorCompleteTimerFailureRetryCount is retry count for timer processor complete timer operation
	TimerProcessorCompleteTimerFailureRetryCount
	// TimerProcessorUpdateShardTaskCount is update shard count for timer processor
	TimerProcessorUpdateShardTaskCount
	// TimerProcessorUpdateAckInterval is update interval for timer processor
	TimerProcessorUpdateAckInterval
	// TimerProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	TimerProcessorUpdateAckIntervalJitterCoefficient
	// TimerProcessorCompleteTimerInterval is complete timer interval for timer processor
	TimerProcessorCompleteTimerInterval
	// TimerProcessorFailoverMaxPollRPS is max poll rate per second for timer processor
	TimerProcessorFailoverMaxPollRPS
	// TimerProcessorMaxPollRPS is max poll rate per second for timer processor
	TimerProcessorMaxPollRPS
	// TimerProcessorMaxPollInterval is max poll interval for timer processor
	TimerProcessorMaxPollInterval
	// TimerProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	TimerProcessorMaxPollIntervalJitterCoefficient
	// TimerProcessorMaxTimeShift is the max shift timer processor can have
	TimerProcessorMaxTimeShift
	// TimerProcessorHistoryArchivalSizeLimit is the max history size for inline archival
	TimerProcessorHistoryArchivalSizeLimit
	// TimerProcessorArchivalTimeLimit is the upper time limit for inline history archival
	TimerProcessorArchivalTimeLimit
	// TransferTaskBatchSize is batch size for transferQueueProcessor
	TransferTaskBatchSize
	// TransferProcessorFailoverMaxPollRPS is max poll rate per second for transferQueueProcessor
	TransferProcessorFailoverMaxPollRPS
	// TransferProcessorMaxPollRPS is max poll rate per second for transferQueueProcessor
	TransferProcessorMaxPollRPS
	// TransferTaskWorkerCount is number of worker for transferQueueProcessor
	TransferTaskWorkerCount
	// TransferTaskMaxRetryCount is max times of retry for transferQueueProcessor
	TransferTaskMaxRetryCount
	// TransferProcessorCompleteTransferFailureRetryCount is times of retry for failure
	TransferProcessorCompleteTransferFailureRetryCount
	// TransferProcessorUpdateShardTaskCount is update shard count for transferQueueProcessor
	TransferProcessorUpdateShardTaskCount
	// TransferProcessorMaxPollInterval max poll interval for transferQueueProcessor
	TransferProcessorMaxPollInterval
	// TransferProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	TransferProcessorMaxPollIntervalJitterCoefficient
	// TransferProcessorUpdateAckInterval is update interval for transferQueueProcessor
	TransferProcessorUpdateAckInterval
	// TransferProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	TransferProcessorUpdateAckIntervalJitterCoefficient
	// TransferProcessorCompleteTransferInterval is complete timer interval for transferQueueProcessor
	TransferProcessorCompleteTransferInterval
	// TransferProcessorVisibilityArchivalTimeLimit is the upper time limit for archiving visibility records
	TransferProcessorVisibilityArchivalTimeLimit
	// ReplicatorTaskBatchSize is batch size for ReplicatorProcessor
	ReplicatorTaskBatchSize
	// ReplicatorTaskWorkerCount is number of worker for ReplicatorProcessor
	ReplicatorTaskWorkerCount
	// ReplicatorTaskMaxRetryCount is max times of retry for ReplicatorProcessor
	ReplicatorTaskMaxRetryCount
	// ReplicatorProcessorMaxPollRPS is max poll rate per second for ReplicatorProcessor
	ReplicatorProcessorMaxPollRPS
	// ReplicatorProcessorUpdateShardTaskCount is update shard count for ReplicatorProcessor
	ReplicatorProcessorUpdateShardTaskCount
	// ReplicatorProcessorMaxPollInterval is max poll interval for ReplicatorProcessor
	ReplicatorProcessorMaxPollInterval
	// ReplicatorProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	ReplicatorProcessorMaxPollIntervalJitterCoefficient
	// ReplicatorProcessorUpdateAckInterval is update interval for ReplicatorProcessor
	ReplicatorProcessorUpdateAckInterval
	// ReplicatorProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient
	// ExecutionMgrNumConns is persistence connections number for ExecutionManager
	ExecutionMgrNumConns
	// HistoryMgrNumConns is persistence connections number for HistoryManager
	HistoryMgrNumConns
	// MaximumBufferedEventsBatch is max number of buffer event in mutable state
	MaximumBufferedEventsBatch
	// MaximumSignalsPerExecution is max number of signals supported by single execution
	MaximumSignalsPerExecution
	// ShardUpdateMinInterval is the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval
	// ShardSyncMinInterval is the minimal time interval which the shard info should be sync to remote
	ShardSyncMinInterval
	// DefaultEventEncoding is the encoding type for history events
	DefaultEventEncoding
	// NumArchiveSystemWorkflows is key for number of archive system workflows running in total
	NumArchiveSystemWorkflows
	// ArchiveRequestRPS is the rate limit on the number of archive request per second
	ArchiveRequestRPS

	// EnableAdminProtection is whether to enable admin checking
	EnableAdminProtection
	// AdminOperationToken is the token to pass admin checking
	AdminOperationToken
	// HistoryMaxAutoResetPoints is the key for max number of auto reset points stored in mutableState
	HistoryMaxAutoResetPoints

	// EnableParentClosePolicy whether to  ParentClosePolicy
	EnableParentClosePolicy
	// ParentClosePolicyThreshold decides that parent close policy will be processed by sys workers(if enabled) if
	// the number of children greater than or equal to this threshold
	ParentClosePolicyThreshold
	// NumParentClosePolicySystemWorkflows is key for number of parentClosePolicy system workflows running in total
	NumParentClosePolicySystemWorkflows

	// HistoryThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	HistoryThrottledLogRPS
	// StickyTTL is to expire a sticky tasklist if no update more than this duration
	StickyTTL
	// DecisionHeartbeatTimeout for decision heartbeat
	DecisionHeartbeatTimeout

	// key for worker

	// WorkerPersistenceMaxQPS is the max qps worker host can query DB
	WorkerPersistenceMaxQPS
	// WorkerReplicatorMetaTaskConcurrency is the number of coroutine handling metadata related tasks
	WorkerReplicatorMetaTaskConcurrency
	// WorkerReplicatorTaskConcurrency is the number of coroutine handling non metadata related tasks
	WorkerReplicatorTaskConcurrency
	// WorkerReplicatorMessageConcurrency is the max concurrent tasks provided by messaging client
	WorkerReplicatorMessageConcurrency
	// WorkerReplicatorActivityBufferRetryCount is the retry attempt when encounter retry error on activity
	WorkerReplicatorActivityBufferRetryCount
	// WorkerReplicatorHistoryBufferRetryCount is the retry attempt when encounter retry error on history
	WorkerReplicatorHistoryBufferRetryCount
	// WorkerReplicationTaskMaxRetryCount is the max retry count for any task
	WorkerReplicationTaskMaxRetryCount
	// WorkerReplicationTaskMaxRetryDuration is the max retry duration for any task
	WorkerReplicationTaskMaxRetryDuration
	// WorkerIndexerConcurrency is the max concurrent messages to be processed at any given time
	WorkerIndexerConcurrency
	// WorkerESProcessorNumOfWorkers is num of workers for esProcessor
	WorkerESProcessorNumOfWorkers
	// WorkerESProcessorBulkActions is max number of requests in bulk for esProcessor
	WorkerESProcessorBulkActions
	// WorkerESProcessorBulkSize is max total size of bulk in bytes for esProcessor
	WorkerESProcessorBulkSize
	// WorkerESProcessorFlushInterval is flush interval for esProcessor
	WorkerESProcessorFlushInterval
	// EnableArchivalCompression indicates whether blobs are compressed before they are archived
	EnableArchivalCompression
	// WorkerHistoryPageSize indicates the page size of history fetched from persistence for archival
	WorkerHistoryPageSize
	// WorkerTargetArchivalBlobSize indicates the target blob size in bytes for archival, actual blob size may vary
	WorkerTargetArchivalBlobSize
	// WorkerArchiverConcurrency controls the number of coroutines handling archival work per archival workflow
	WorkerArchiverConcurrency
	// WorkerArchivalsPerIteration controls the number of archivals handled in each iteration of archival workflow
	WorkerArchivalsPerIteration
	// WorkerDeterministicConstructionCheckProbability controls the probability of running a deterministic construction check for any given archival
	WorkerDeterministicConstructionCheckProbability
	// WorkerBlobIntegrityCheckProbability controls the probability of running an integrity check for any given archival
	WorkerBlobIntegrityCheckProbability
	// WorkerTimeLimitPerArchivalIteration controls the time limit of each iteration of archival workflow
	WorkerTimeLimitPerArchivalIteration
	// WorkerThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	WorkerThrottledLogRPS
	// ScannerPersistenceMaxQPS is the maximum rate of persistence calls from worker.Scanner
	ScannerPersistenceMaxQPS
	// EnableBatcher decides whether start batcher in our worker
	EnableBatcher
	// EnableParentClosePolicyWorker decides whether or not enable system workers for processing parent close policy task
	EnableParentClosePolicyWorker

	// lastKeyForTest must be the last one in this const group for testing purpose
	lastKeyForTest
)

// Filter represents a filter on the dynamic config key
type Filter int

func (f Filter) String() string {
	if f <= unknownFilter || f > TaskType {
		return filters[unknownFilter]
	}
	return filters[f]
}

var filters = []string{
	"unknownFilter",
	"domainName",
	"taskListName",
	"taskType",
}

const (
	unknownFilter Filter = iota
	// DomainName is the domain name
	DomainName
	// TaskListName is the tasklist name
	TaskListName
	// TaskType is the task type (0:Decision, 1:Activity)
	TaskType

	// lastFilterTypeForTest must be the last one in this const group for testing purpose
	lastFilterTypeForTest
)

// FilterOption is used to provide filters for dynamic config keys
type FilterOption func(filterMap map[Filter]interface{})

// TaskListFilter filters by task list name
func TaskListFilter(name string) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[TaskListName] = name
	}
}

// DomainFilter filters by domain name
func DomainFilter(name string) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[DomainName] = name
	}
}

// TaskTypeFilter filters by task type
func TaskTypeFilter(taskType int) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[TaskType] = taskType
	}
}
