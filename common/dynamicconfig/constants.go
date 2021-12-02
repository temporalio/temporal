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

package dynamicconfig

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// Key represents a key/property stored in dynamic config
type Key int

func (k Key) String() string {
	keyName, ok := Keys[k]
	if !ok {
		return Keys[unknownKey]
	}
	return keyName
}

// Keys represents a mapping from Key to keyName, where keyName are used dynamic config source.
var Keys = map[Key]string{
	unknownKey: "unknownKey",

	// tests keys
	testGetPropertyKey:                                "testGetPropertyKey",
	testCaseInsensitivePropertyKey:                    "testCaseInsensitivePropertyKey",
	testGetIntPropertyKey:                             "testGetIntPropertyKey",
	testGetFloat64PropertyKey:                         "testGetFloat64PropertyKey",
	testGetDurationPropertyKey:                        "testGetDurationPropertyKey",
	testGetBoolPropertyKey:                            "testGetBoolPropertyKey",
	testGetStringPropertyKey:                          "testGetStringPropertyKey",
	testGetMapPropertyKey:                             "testGetMapPropertyKey",
	testGetIntPropertyFilteredByNamespaceKey:          "testGetIntPropertyFilteredByNamespaceKey",
	testGetDurationPropertyFilteredByNamespaceKey:     "testGetDurationPropertyFilteredByNamespaceKey",
	testGetIntPropertyFilteredByTaskQueueInfoKey:      "testGetIntPropertyFilteredByTaskQueueInfoKey",
	testGetDurationPropertyFilteredByTaskQueueInfoKey: "testGetDurationPropertyFilteredByTaskQueueInfoKey",
	testGetBoolPropertyFilteredByNamespaceIDKey:       "testGetBoolPropertyFilteredByNamespaceIDKey",
	testGetBoolPropertyFilteredByTaskQueueInfoKey:     "testGetBoolPropertyFilteredByTaskQueueInfoKey",

	// admin settings
	// NOTE: admin settings are not guaranteed to be compatible across different versions
	AdminMatchingNamespaceToPartitionDispatchRate:          "admin.matchingNamespaceToPartitionDispatchRate",
	AdminMatchingNamespaceTaskqueueToPartitionDispatchRate: "admin.matchingNamespaceTaskqueueToPartitionDispatchRate",

	// system settings
	StandardVisibilityPersistenceMaxReadQPS:  "system.standardVisibilityPersistenceMaxReadQPS",
	StandardVisibilityPersistenceMaxWriteQPS: "system.standardVisibilityPersistenceMaxWriteQPS",
	AdvancedVisibilityPersistenceMaxReadQPS:  "system.advancedVisibilityPersistenceMaxReadQPS",
	AdvancedVisibilityPersistenceMaxWriteQPS: "system.advancedVisibilityPersistenceMaxWriteQPS",
	AdvancedVisibilityWritingMode:            "system.advancedVisibilityWritingMode",
	EnableReadVisibilityFromES:               "system.enableReadVisibilityFromES",

	HistoryArchivalState:                   "system.historyArchivalState",
	EnableReadFromHistoryArchival:          "system.enableReadFromHistoryArchival",
	VisibilityArchivalState:                "system.visibilityArchivalState",
	EnableReadFromVisibilityArchival:       "system.enableReadFromVisibilityArchival",
	EnableNamespaceNotActiveAutoForwarding: "system.enableNamespaceNotActiveAutoForwarding",
	TransactionSizeLimit:                   "system.transactionSizeLimit",
	DisallowQuery:                          "system.disallowQuery",
	EnableBatcher:                          "worker.enableBatcher",
	EnableParentClosePolicyWorker:          "system.enableParentClosePolicyWorker",
	EnableStickyQuery:                      "system.enableStickyQuery",
	EnablePriorityTaskProcessor:            "system.enablePriorityTaskProcessor",
	EnableAuthorization:                    "system.enableAuthorization",
	EnableCrossNamespaceCommands:           "system.enableCrossNamespaceCommands",

	// size limit
	BlobSizeLimitError:     "limit.blobSize.error",
	BlobSizeLimitWarn:      "limit.blobSize.warn",
	MemoSizeLimitError:     "limit.memoSize.error",
	MemoSizeLimitWarn:      "limit.memoSize.warn",
	HistorySizeLimitError:  "limit.historySize.error",
	HistorySizeLimitWarn:   "limit.historySize.warn",
	HistoryCountLimitError: "limit.historyCount.error",
	HistoryCountLimitWarn:  "limit.historyCount.warn",
	MaxIDLengthLimit:       "limit.maxIDLength",

	// frontend settings
	FrontendPersistenceMaxQPS:             "frontend.persistenceMaxQPS",
	FrontendPersistenceGlobalMaxQPS:       "frontend.persistenceGlobalMaxQPS",
	FrontendVisibilityMaxPageSize:         "frontend.visibilityMaxPageSize",
	FrontendMaxBadBinaries:                "frontend.maxBadBinaries",
	FrontendESIndexMaxResultWindow:        "frontend.esIndexMaxResultWindow",
	FrontendHistoryMaxPageSize:            "frontend.historyMaxPageSize",
	FrontendRPS:                           "frontend.rps",
	FrontendMaxNamespaceRPSPerInstance:    "frontend.namespaceRPS",
	FrontendMaxNamespaceBurstPerInstance:  "frontend.namespaceBurst",
	FrontendMaxNamespaceCountPerInstance:  "frontend.namespaceCount",
	FrontendGlobalNamespaceRPS:            "frontend.globalNamespacerps",
	FrontendShutdownDrainDuration:         "frontend.shutdownDrainDuration",
	DisableListVisibilityByFilter:         "frontend.disableListVisibilityByFilter",
	FrontendThrottledLogRPS:               "frontend.throttledLogRPS",
	EnableClientVersionCheck:              "frontend.enableClientVersionCheck",
	SendRawWorkflowHistory:                "frontend.sendRawWorkflowHistory",
	SearchAttributesNumberOfKeysLimit:     "frontend.searchAttributesNumberOfKeysLimit",
	SearchAttributesSizeOfValueLimit:      "frontend.searchAttributesSizeOfValueLimit",
	SearchAttributesTotalSizeLimit:        "frontend.searchAttributesTotalSizeLimit",
	VisibilityArchivalQueryMaxPageSize:    "frontend.visibilityArchivalQueryMaxPageSize",
	VisibilityArchivalQueryMaxRangeInDays: "frontend.visibilityArchivalQueryMaxRangeInDays",
	VisibilityArchivalQueryMaxQPS:         "frontend.visibilityArchivalQueryMaxQPS",
	EnableServerVersionCheck:              "frontend.enableServerVersionCheck",
	EnableTokenNamespaceEnforcement:       "frontend.enableTokenNamespaceEnforcement",
	KeepAliveMinTime:                      "frontend.keepAliveMinTime",
	KeepAlivePermitWithoutStream:          "frontend.keepAlivePermitWithoutStream",
	KeepAliveMaxConnectionIdle:            "frontend.keepAliveMaxConnectionIdle",
	KeepAliveMaxConnectionAge:             "frontend.keepAliveMaxConnectionAge",
	KeepAliveMaxConnectionAgeGrace:        "frontend.keepAliveMaxConnectionAgeGrace",
	KeepAliveTime:                         "frontend.keepAliveTime",
	KeepAliveTimeout:                      "frontend.keepAliveTimeout",

	// matching settings
	MatchingRPS:                             "matching.rps",
	MatchingPersistenceMaxQPS:               "matching.persistenceMaxQPS",
	MatchingPersistenceGlobalMaxQPS:         "matching.persistenceGlobalMaxQPS",
	MatchingMinTaskThrottlingBurstSize:      "matching.minTaskThrottlingBurstSize",
	MatchingGetTasksBatchSize:               "matching.getTasksBatchSize",
	MatchingLongPollExpirationInterval:      "matching.longPollExpirationInterval",
	MatchingSyncMatchWaitDuration:           "matching.syncMatchWaitDuration",
	MatchingUpdateAckInterval:               "matching.updateAckInterval",
	MatchingIdleTaskqueueCheckInterval:      "matching.idleTaskqueueCheckInterval",
	MaxTaskqueueIdleTime:                    "matching.maxTaskqueueIdleTime",
	MatchingOutstandingTaskAppendsThreshold: "matching.outstandingTaskAppendsThreshold",
	MatchingMaxTaskBatchSize:                "matching.maxTaskBatchSize",
	MatchingMaxTaskDeleteBatchSize:          "matching.maxTaskDeleteBatchSize",
	MatchingThrottledLogRPS:                 "matching.throttledLogRPS",
	MatchingNumTaskqueueWritePartitions:     "matching.numTaskqueueWritePartitions",
	MatchingNumTaskqueueReadPartitions:      "matching.numTaskqueueReadPartitions",
	MatchingForwarderMaxOutstandingPolls:    "matching.forwarderMaxOutstandingPolls",
	MatchingForwarderMaxOutstandingTasks:    "matching.forwarderMaxOutstandingTasks",
	MatchingForwarderMaxRatePerSecond:       "matching.forwarderMaxRatePerSecond",
	MatchingForwarderMaxChildrenPerNode:     "matching.forwarderMaxChildrenPerNode",
	MatchingShutdownDrainDuration:           "matching.shutdownDrainDuration",

	// history settings
	HistoryRPS:                                           "history.rps",
	HistoryPersistenceMaxQPS:                             "history.persistenceMaxQPS",
	HistoryPersistenceGlobalMaxQPS:                       "history.persistenceGlobalMaxQPS",
	HistoryLongPollExpirationInterval:                    "history.longPollExpirationInterval",
	HistoryCacheInitialSize:                              "history.cacheInitialSize",
	HistoryMaxAutoResetPoints:                            "history.historyMaxAutoResetPoints",
	HistoryCacheMaxSize:                                  "history.cacheMaxSize",
	HistoryCacheTTL:                                      "history.cacheTTL",
	HistoryShutdownDrainDuration:                         "history.shutdownDrainDuration",
	EventsCacheInitialSize:                               "history.eventsCacheInitialSize",
	EventsCacheMaxSize:                                   "history.eventsCacheMaxSize",
	EventsCacheTTL:                                       "history.eventsCacheTTL",
	AcquireShardInterval:                                 "history.acquireShardInterval",
	AcquireShardConcurrency:                              "history.acquireShardConcurrency",
	StandbyClusterDelay:                                  "history.standbyClusterDelay",
	StandbyTaskMissingEventsResendDelay:                  "history.standbyTaskMissingEventsResendDelay",
	StandbyTaskMissingEventsDiscardDelay:                 "history.standbyTaskMissingEventsDiscardDelay",
	TaskProcessRPS:                                       "history.taskProcessRPS",
	TaskSchedulerType:                                    "history.taskSchedulerType",
	TaskSchedulerWorkerCount:                             "history.taskSchedulerWorkerCount",
	TaskSchedulerQueueSize:                               "history.taskSchedulerQueueSize",
	TaskSchedulerRoundRobinWeights:                       "history.taskSchedulerRoundRobinWeight",
	TimerTaskBatchSize:                                   "history.timerTaskBatchSize",
	TimerTaskWorkerCount:                                 "history.timerTaskWorkerCount",
	TimerTaskMaxRetryCount:                               "history.timerTaskMaxRetryCount",
	TimerProcessorGetFailureRetryCount:                   "history.timerProcessorGetFailureRetryCount",
	TimerProcessorCompleteTimerFailureRetryCount:         "history.timerProcessorCompleteTimerFailureRetryCount",
	TimerProcessorUpdateShardTaskCount:                   "history.timerProcessorUpdateShardTaskCount",
	TimerProcessorUpdateAckInterval:                      "history.timerProcessorUpdateAckInterval",
	TimerProcessorUpdateAckIntervalJitterCoefficient:     "history.timerProcessorUpdateAckIntervalJitterCoefficient",
	TimerProcessorCompleteTimerInterval:                  "history.timerProcessorCompleteTimerInterval",
	TimerProcessorFailoverMaxPollRPS:                     "history.timerProcessorFailoverMaxPollRPS",
	TimerProcessorMaxPollRPS:                             "history.timerProcessorMaxPollRPS",
	TimerProcessorMaxPollInterval:                        "history.timerProcessorMaxPollInterval",
	TimerProcessorMaxPollIntervalJitterCoefficient:       "history.timerProcessorMaxPollIntervalJitterCoefficient",
	TimerProcessorRedispatchInterval:                     "history.timerProcessorRedispatchInterval",
	TimerProcessorRedispatchIntervalJitterCoefficient:    "history.timerProcessorRedispatchIntervalJitterCoefficient",
	TimerProcessorMaxRedispatchQueueSize:                 "history.timerProcessorMaxRedispatchQueueSize",
	TimerProcessorEnablePriorityTaskProcessor:            "history.timerProcessorEnablePriorityTaskProcessor",
	TimerProcessorMaxTimeShift:                           "history.timerProcessorMaxTimeShift",
	TimerProcessorHistoryArchivalSizeLimit:               "history.timerProcessorHistoryArchivalSizeLimit",
	TimerProcessorArchivalTimeLimit:                      "history.timerProcessorArchivalTimeLimit",
	TransferTaskBatchSize:                                "history.transferTaskBatchSize",
	TransferProcessorFailoverMaxPollRPS:                  "history.transferProcessorFailoverMaxPollRPS",
	TransferProcessorMaxPollRPS:                          "history.transferProcessorMaxPollRPS",
	TransferTaskWorkerCount:                              "history.transferTaskWorkerCount",
	TransferTaskMaxRetryCount:                            "history.transferTaskMaxRetryCount",
	TransferProcessorCompleteTransferFailureRetryCount:   "history.transferProcessorCompleteTransferFailureRetryCount",
	TransferProcessorUpdateShardTaskCount:                "history.transferProcessorUpdateShardTaskCount",
	TransferProcessorMaxPollInterval:                     "history.transferProcessorMaxPollInterval",
	TransferProcessorMaxPollIntervalJitterCoefficient:    "history.transferProcessorMaxPollIntervalJitterCoefficient",
	TransferProcessorUpdateAckInterval:                   "history.transferProcessorUpdateAckInterval",
	TransferProcessorUpdateAckIntervalJitterCoefficient:  "history.transferProcessorUpdateAckIntervalJitterCoefficient",
	TransferProcessorCompleteTransferInterval:            "history.transferProcessorCompleteTransferInterval",
	TransferProcessorRedispatchInterval:                  "history.transferProcessorRedispatchInterval",
	TransferProcessorRedispatchIntervalJitterCoefficient: "history.transferProcessorRedispatchIntervalJitterCoefficient",
	TransferProcessorMaxRedispatchQueueSize:              "history.transferProcessorMaxRedispatchQueueSize",
	TransferProcessorEnablePriorityTaskProcessor:         "history.transferProcessorEnablePriorityTaskProcessor",
	TransferProcessorVisibilityArchivalTimeLimit:         "history.transferProcessorVisibilityArchivalTimeLimit",

	VisibilityTaskBatchSize:                                "history.visibilityTaskBatchSize",
	VisibilityProcessorFailoverMaxPollRPS:                  "history.visibilityProcessorFailoverMaxPollRPS",
	VisibilityProcessorMaxPollRPS:                          "history.visibilityProcessorMaxPollRPS",
	VisibilityTaskWorkerCount:                              "history.visibilityTaskWorkerCount",
	VisibilityTaskMaxRetryCount:                            "history.visibilityTaskMaxRetryCount",
	VisibilityProcessorCompleteTaskFailureRetryCount:       "history.visibilityProcessorCompleteTaskFailureRetryCount",
	VisibilityProcessorUpdateShardTaskCount:                "history.visibilityProcessorUpdateShardTaskCount",
	VisibilityProcessorMaxPollInterval:                     "history.visibilityProcessorMaxPollInterval",
	VisibilityProcessorMaxPollIntervalJitterCoefficient:    "history.visibilityProcessorMaxPollIntervalJitterCoefficient",
	VisibilityProcessorUpdateAckInterval:                   "history.visibilityProcessorUpdateAckInterval",
	VisibilityProcessorUpdateAckIntervalJitterCoefficient:  "history.visibilityProcessorUpdateAckIntervalJitterCoefficient",
	VisibilityProcessorCompleteTaskInterval:                "history.visibilityProcessorCompleteTaskInterval",
	VisibilityProcessorRedispatchInterval:                  "history.visibilityProcessorRedispatchInterval",
	VisibilityProcessorRedispatchIntervalJitterCoefficient: "history.visibilityProcessorRedispatchIntervalJitterCoefficient",
	VisibilityProcessorMaxRedispatchQueueSize:              "history.visibilityProcessorMaxRedispatchQueueSize",
	VisibilityProcessorEnablePriorityTaskProcessor:         "history.visibilityProcessorEnablePriorityTaskProcessor",
	VisibilityProcessorVisibilityArchivalTimeLimit:         "history.visibilityProcessorVisibilityArchivalTimeLimit",

	TieredStorageTaskBatchSize:                                "history.tieredStorageTaskBatchSize",
	TieredStorageProcessorFailoverMaxPollRPS:                  "history.tieredStorageProcessorFailoverMaxPollRPS",
	TieredStorageProcessorMaxPollRPS:                          "history.tieredStorageProcessorMaxPollRPS",
	TieredStorageTaskWorkerCount:                              "history.tieredStorageTaskWorkerCount",
	TieredStorageTaskMaxRetryCount:                            "history.tieredStorageTaskMaxRetryCount",
	TieredStorageProcessorCompleteTaskFailureRetryCount:       "history.tieredStorageProcessorCompleteTaskFailureRetryCount",
	TieredStorageProcessorUpdateShardTaskCount:                "history.tieredStorageProcessorUpdateShardTaskCount",
	TieredStorageProcessorMaxPollInterval:                     "history.tieredStorageProcessorMaxPollInterval",
	TieredStorageProcessorMaxPollIntervalJitterCoefficient:    "history.tieredStorageProcessorMaxPollIntervalJitterCoefficient",
	TieredStorageProcessorUpdateAckInterval:                   "history.tieredStorageProcessorUpdateAckInterval",
	TieredStorageProcessorUpdateAckIntervalJitterCoefficient:  "history.tieredStorageProcessorUpdateAckIntervalJitterCoefficient",
	TieredStorageProcessorCompleteTaskInterval:                "history.tieredStorageProcessorCompleteTaskInterval",
	TieredStorageProcessorRedispatchInterval:                  "history.tieredStorageProcessorRedispatchInterval",
	TieredStorageProcessorRedispatchIntervalJitterCoefficient: "history.tieredStorageProcessorRedispatchIntervalJitterCoefficient",
	TieredStorageProcessorMaxRedispatchQueueSize:              "history.tieredStorageProcessorMaxRedispatchQueueSize",
	TieredStorageProcessorEnablePriorityTaskProcessor:         "history.tieredStorageProcessorEnablePriorityTaskProcessor",
	TieredStorageProcessorArchivalTimeLimit:                   "history.tieredStorageProcessorArchivalTimeLimit",

	ReplicatorTaskBatchSize:                                "history.replicatorTaskBatchSize",
	ReplicatorTaskWorkerCount:                              "history.replicatorTaskWorkerCount",
	ReplicatorTaskMaxRetryCount:                            "history.replicatorTaskMaxRetryCount",
	ReplicatorProcessorMaxPollRPS:                          "history.replicatorProcessorMaxPollRPS",
	ReplicatorProcessorUpdateShardTaskCount:                "history.replicatorProcessorUpdateShardTaskCount",
	ReplicatorProcessorMaxPollInterval:                     "history.replicatorProcessorMaxPollInterval",
	ReplicatorProcessorMaxPollIntervalJitterCoefficient:    "history.replicatorProcessorMaxPollIntervalJitterCoefficient",
	ReplicatorProcessorUpdateAckInterval:                   "history.replicatorProcessorUpdateAckInterval",
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient:  "history.replicatorProcessorUpdateAckIntervalJitterCoefficient",
	ReplicatorProcessorRedispatchInterval:                  "history.replicatorProcessorRedispatchInterval",
	ReplicatorProcessorRedispatchIntervalJitterCoefficient: "history.replicatorProcessorRedispatchIntervalJitterCoefficient",
	ReplicatorProcessorMaxRedispatchQueueSize:              "history.replicatorProcessorMaxRedispatchQueueSize",
	ReplicatorProcessorEnablePriorityTaskProcessor:         "history.replicatorProcessorEnablePriorityTaskProcessor",
	MaximumBufferedEventsBatch:                             "history.maximumBufferedEventsBatch",
	MaximumSignalsPerExecution:                             "history.maximumSignalsPerExecution",
	ShardUpdateMinInterval:                                 "history.shardUpdateMinInterval",
	ShardSyncMinInterval:                                   "history.shardSyncMinInterval",
	ShardSyncTimerJitterCoefficient:                        "history.shardSyncMinInterval",
	DefaultEventEncoding:                                   "history.defaultEventEncoding",
	EnableParentClosePolicy:                                "history.enableParentClosePolicy",
	NumArchiveSystemWorkflows:                              "history.numArchiveSystemWorkflows",
	ArchiveRequestRPS:                                      "history.archiveRequestRPS",
	EmitShardDiffLog:                                       "history.emitShardDiffLog",
	HistoryThrottledLogRPS:                                 "history.throttledLogRPS",
	StickyTTL:                                              "history.stickyTTL",
	WorkflowTaskHeartbeatTimeout:                           "history.workflowTaskHeartbeatTimeout",
	DefaultWorkflowTaskTimeout:                             "history.defaultWorkflowTaskTimeout",
	ParentClosePolicyThreshold:                             "history.parentClosePolicyThreshold",
	NumParentClosePolicySystemWorkflows:                    "history.numParentClosePolicySystemWorkflows",
	ReplicationTaskFetcherParallelism:                      "history.ReplicationTaskFetcherParallelism",
	ReplicationTaskFetcherAggregationInterval:              "history.ReplicationTaskFetcherAggregationInterval",
	ReplicationTaskFetcherTimerJitterCoefficient:           "history.ReplicationTaskFetcherTimerJitterCoefficient",
	ReplicationTaskFetcherErrorRetryWait:                   "history.ReplicationTaskFetcherErrorRetryWait",
	ReplicationTaskProcessorErrorRetryWait:                 "history.ReplicationTaskProcessorErrorRetryWait",
	ReplicationTaskProcessorErrorRetryBackoffCoefficient:   "history.ReplicationTaskProcessorErrorRetryBackoffCoefficient",
	ReplicationTaskProcessorErrorRetryMaxInterval:          "history.ReplicationTaskProcessorErrorRetryMaxInterval",
	ReplicationTaskProcessorErrorRetryMaxAttempts:          "history.ReplicationTaskProcessorErrorRetryMaxAttempts",
	ReplicationTaskProcessorErrorRetryExpiration:           "history.ReplicationTaskProcessorErrorRetryExpiration",
	ReplicationTaskProcessorNoTaskInitialWait:              "history.ReplicationTaskProcessorNoTaskInitialWait",
	ReplicationTaskProcessorCleanupInterval:                "history.ReplicationTaskProcessorCleanupInterval",
	ReplicationTaskProcessorCleanupJitterCoefficient:       "history.ReplicationTaskProcessorCleanupJitterCoefficient",
	ReplicationTaskProcessorStartWait:                      "history.ReplicationTaskProcessorStartWait",
	ReplicationTaskProcessorStartWaitJitterCoefficient:     "history.ReplicationTaskProcessorStartWaitJitterCoefficient",
	ReplicationTaskProcessorHostQPS:                        "history.ReplicationTaskProcessorHostQPS",
	ReplicationTaskProcessorShardQPS:                       "history.ReplicationTaskProcessorShardQPS",
	MaxBufferedQueryCount:                                  "history.MaxBufferedQueryCount",
	MutableStateChecksumGenProbability:                     "history.mutableStateChecksumGenProbability",
	MutableStateChecksumVerifyProbability:                  "history.mutableStateChecksumVerifyProbability",
	MutableStateChecksumInvalidateBefore:                   "history.mutableStateChecksumInvalidateBefore",
	ReplicationEventsFromCurrentCluster:                    "history.ReplicationEventsFromCurrentCluster",
	StandbyTaskReReplicationContextTimeout:                 "history.standbyTaskReReplicationContextTimeout",
	EnableDropStuckTaskByNamespaceID:                       "history.DropStuckTaskByNamespace",
	SkipReapplicationByNamespaceID:                         "history.SkipReapplicationByNamespaceID",
	DefaultActivityRetryPolicy:                             "history.defaultActivityRetryPolicy",
	DefaultWorkflowRetryPolicy:                             "history.defaultWorkflowRetryPolicy",

	// worker settings
	WorkerPersistenceMaxQPS:       "worker.persistenceMaxQPS",
	WorkerPersistenceGlobalMaxQPS: "worker.persistenceGlobalMaxQPS",

	WorkerIndexerConcurrency:       "worker.indexerConcurrency",
	WorkerESProcessorNumOfWorkers:  "worker.ESProcessorNumOfWorkers",
	WorkerESProcessorBulkActions:   "worker.ESProcessorBulkActions",
	WorkerESProcessorBulkSize:      "worker.ESProcessorBulkSize",
	WorkerESProcessorFlushInterval: "worker.ESProcessorFlushInterval",
	WorkerESProcessorAckTimeout:    "worker.ESProcessorAckTimeout",

	WorkerArchiverMaxConcurrentActivityExecutionSize:     "worker.ArchiverMaxConcurrentActivityExecutionSize",
	WorkerArchiverMaxConcurrentWorkflowTaskExecutionSize: "worker.ArchiverMaxConcurrentWorkflowTaskExecutionSize",
	WorkerArchiverMaxConcurrentActivityTaskPollers:       "worker.ArchiverMaxConcurrentActivityTaskPollers",
	WorkerArchiverMaxConcurrentWorkflowTaskPollers:       "worker.ArchiverMaxConcurrentWorkflowTaskPollers",
	WorkerArchiverConcurrency:                            "worker.ArchiverConcurrency",
	WorkerArchivalsPerIteration:                          "worker.ArchivalsPerIteration",

	WorkerScannerMaxConcurrentActivityExecutionSize:     "worker.ScannerMaxConcurrentActivityExecutionSize",
	WorkerScannerMaxConcurrentWorkflowTaskExecutionSize: "worker.ScannerMaxConcurrentWorkflowTaskExecutionSize",
	WorkerScannerMaxConcurrentActivityTaskPollers:       "worker.ScannerMaxConcurrentActivityTaskPollers",
	WorkerScannerMaxConcurrentWorkflowTaskPollers:       "worker.ScannerMaxConcurrentWorkflowTaskPollers",

	WorkerBatcherMaxConcurrentActivityExecutionSize:     "worker.BatcherMaxConcurrentActivityExecutionSize",
	WorkerBatcherMaxConcurrentWorkflowTaskExecutionSize: "worker.BatcherMaxConcurrentWorkflowTaskExecutionSize",
	WorkerBatcherMaxConcurrentActivityTaskPollers:       "worker.BatcherMaxConcurrentActivityTaskPollers",
	WorkerBatcherMaxConcurrentWorkflowTaskPollers:       "worker.BatcherMaxConcurrentWorkflowTaskPollers",

	WorkerParentCloseMaxConcurrentActivityExecutionSize:     "worker.ParentCloseMaxConcurrentActivityExecutionSize",
	WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize: "worker.ParentCloseMaxConcurrentWorkflowTaskExecutionSize",
	WorkerParentCloseMaxConcurrentActivityTaskPollers:       "worker.ParentCloseMaxConcurrentActivityTaskPollers",
	WorkerParentCloseMaxConcurrentWorkflowTaskPollers:       "worker.ParentCloseMaxConcurrentWorkflowTaskPollers",

	WorkerTimeLimitPerArchivalIteration: "worker.TimeLimitPerArchivalIteration",
	WorkerThrottledLogRPS:               "worker.throttledLogRPS",
	ScannerPersistenceMaxQPS:            "worker.scannerPersistenceMaxQPS",
	TaskQueueScannerEnabled:             "worker.taskQueueScannerEnabled",
	HistoryScannerEnabled:               "worker.historyScannerEnabled",
	ExecutionsScannerEnabled:            "worker.executionsScannerEnabled",

	EnableRingpopTLS: "system.enableRingpopTLS",
}

const (
	unknownKey Key = iota

	// key for tests
	testGetPropertyKey
	testCaseInsensitivePropertyKey
	testGetIntPropertyKey
	testGetFloat64PropertyKey
	testGetDurationPropertyKey
	testGetBoolPropertyKey
	testGetStringPropertyKey
	testGetMapPropertyKey
	testGetIntPropertyFilteredByNamespaceKey
	testGetDurationPropertyFilteredByNamespaceKey
	testGetIntPropertyFilteredByTaskQueueInfoKey
	testGetDurationPropertyFilteredByTaskQueueInfoKey
	testGetBoolPropertyFilteredByNamespaceIDKey
	testGetBoolPropertyFilteredByTaskQueueInfoKey

	// AdminMatchingNamespaceToPartitionDispatchRate is the max qps of any task queue partition for a given namespace
	AdminMatchingNamespaceToPartitionDispatchRate
	// AdminMatchingNamespaceTaskqueueToPartitionDispatchRate is the max qps of a task queue partition for a given namespace & task queue
	AdminMatchingNamespaceTaskqueueToPartitionDispatchRate

	// StandardVisibilityPersistenceMaxReadQPS is the max QPC system host can query standard visibility DB (SQL or Cassandra) for read.
	StandardVisibilityPersistenceMaxReadQPS
	// StandardVisibilityPersistenceMaxWriteQPS is the max QPC system host can query standard visibility DB (SQL or Cassandra) for write.
	StandardVisibilityPersistenceMaxWriteQPS
	// AdvancedVisibilityPersistenceMaxReadQPS is the max QPC system host can query advanced visibility DB (Elasticsearch) for read.
	AdvancedVisibilityPersistenceMaxReadQPS
	// AdvancedVisibilityPersistenceMaxWriteQPS is the max QPC system host can query advanced visibility DB (Elasticsearch) for write.
	AdvancedVisibilityPersistenceMaxWriteQPS
	// AdvancedVisibilityWritingMode is key for how to write to advanced visibility
	AdvancedVisibilityWritingMode
	// EnableReadVisibilityFromES is key for enable read from elastic search
	EnableReadVisibilityFromES
	// DisableListVisibilityByFilter is config to disable list open/close workflow using filter
	DisableListVisibilityByFilter

	// EmitShardDiffLog whether emit the shard diff log
	EmitShardDiffLog
	// HistoryArchivalState is key for the state of history archival
	HistoryArchivalState
	// EnableReadFromHistoryArchival is key for enabling reading history from archival store
	EnableReadFromHistoryArchival
	// VisibilityArchivalState is key for the state of visibility archival
	VisibilityArchivalState
	// EnableReadFromVisibilityArchival is key for enabling reading visibility from archival store
	EnableReadFromVisibilityArchival
	// EnableNamespaceNotActiveAutoForwarding whether enabling DC auto forwarding to active cluster
	// for signal / start / signal with start API if namespace is not active
	EnableNamespaceNotActiveAutoForwarding
	// TransactionSizeLimit is the largest allowed transaction size to persistence
	TransactionSizeLimit
	// DisallowQuery is the key to disallow query for a namespace
	DisallowQuery
	// EnablePriorityTaskProcessor is the key for enabling priority task processor
	EnablePriorityTaskProcessor
	// EnableAuthorization is the key to enable authorization for a namespace
	EnableAuthorization
	// EnableCrossNamespaceCommands is the key to enable commands for external namespaces
	EnableCrossNamespaceCommands
	// BlobSizeLimitError is the per event blob size limit
	BlobSizeLimitError
	// BlobSizeLimitWarn is the per event blob size limit for warning
	BlobSizeLimitWarn
	// MemoSizeLimitError is the per event memo size limit
	MemoSizeLimitError
	// MemoSizeLimitWarn is the per event memo size limit for warning
	MemoSizeLimitWarn
	// HistorySizeLimitError is the per workflow execution history size limit
	HistorySizeLimitError
	// HistorySizeLimitWarn is the per workflow execution history size limit for warning
	HistorySizeLimitWarn
	// HistoryCountLimitError is the per workflow execution history event count limit
	HistoryCountLimitError
	// HistoryCountLimitWarn is the per workflow execution history event count limit for warning
	HistoryCountLimitWarn

	// MaxIDLengthLimit is the length limit for various IDs, including: Namespace, TaskQueue, WorkflowID, ActivityID, TimerID,
	// WorkflowType, ActivityType, SignalName, MarkerName, ErrorReason/FailureReason/CancelCause, Identity, RequestID
	MaxIDLengthLimit

	// key for frontend

	// FrontendPersistenceMaxQPS is the max qps frontend host can query DB
	FrontendPersistenceMaxQPS
	// FrontendPersistenceGlobalMaxQPS is the max qps frontend cluster can query DB
	FrontendPersistenceGlobalMaxQPS

	// FrontendVisibilityMaxPageSize is default max size for ListWorkflowExecutions in one page
	FrontendVisibilityMaxPageSize
	// FrontendESIndexMaxResultWindow is ElasticSearch index setting max_result_window
	FrontendESIndexMaxResultWindow

	// FrontendHistoryMaxPageSize is default max size for GetWorkflowExecutionHistory in one page
	FrontendHistoryMaxPageSize
	// FrontendRPS is workflow rate limit per second
	FrontendRPS
	// FrontendMaxNamespaceRPSPerInstance is workflow namespace rate limit per second
	FrontendMaxNamespaceRPSPerInstance
	// FrontendMaxNamespaceBurstPerInstance is workflow namespace burst limit
	FrontendMaxNamespaceBurstPerInstance
	// FrontendMaxNamespaceCountPerInstance is workflow namespace count limit per second
	FrontendMaxNamespaceCountPerInstance
	// FrontendGlobalNamespaceRPS is workflow namespace rate limit per second for the whole cluster
	FrontendGlobalNamespaceRPS
	// FrontendThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	FrontendThrottledLogRPS
	// FrontendShutdownDrainDuration is the duration of traffic drain during shutdown
	FrontendShutdownDrainDuration
	// EnableClientVersionCheck enables client version check for frontend
	EnableClientVersionCheck

	// FrontendMaxBadBinaries is the max number of bad binaries in namespace config
	FrontendMaxBadBinaries
	// SendRawWorkflowHistory is whether to enable raw history retrieving
	SendRawWorkflowHistory
	// SearchAttributesNumberOfKeysLimit is the limit of number of keys
	SearchAttributesNumberOfKeysLimit
	// SearchAttributesSizeOfValueLimit is the size limit of each value
	SearchAttributesSizeOfValueLimit
	// SearchAttributesTotalSizeLimit is the size limit of the whole map
	SearchAttributesTotalSizeLimit
	// VisibilityArchivalQueryMaxPageSize is the maximum page size for a visibility archival query
	VisibilityArchivalQueryMaxPageSize
	// VisibilityArchivalQueryMaxRangeInDays is the maximum number of days for a visibility archival query
	VisibilityArchivalQueryMaxRangeInDays
	// VisibilityArchivalQueryMaxQPS is the timeout for a visibility archival query
	VisibilityArchivalQueryMaxQPS
	// EnableServerVersionCheck is a flag that controls whether or not periodic version checking is enabled
	EnableServerVersionCheck
	// EnableTokenNamespaceEnforcement enables enforcement that namespace in completion token matches namespace of the request
	EnableTokenNamespaceEnforcement
	// KeepAliveMinTime is the minimum amount of time a client should wait before sending a keepalive ping.
	KeepAliveMinTime
	// KeepAlivePermitWithoutStream If true, server allows keepalive pings even when there are no active
	// streams(RPCs). If false, and client sends ping when there are no active
	// streams, server will send GOAWAY and close the connection.
	KeepAlivePermitWithoutStream
	// KeepAliveMaxConnectionIdle is a duration for the amount of time after which an
	// idle connection would be closed by sending a GoAway. Idleness duration is
	// defined since the most recent time the number of outstanding RPCs became
	// zero or the connection establishment.
	KeepAliveMaxConnectionIdle
	// KeepAliveMaxConnectionAge is a duration for the maximum amount of time a
	// connection may exist before it will be closed by sending a GoAway. A
	// random jitter of +/-10% will be added to MaxConnectionAge to spread out
	// connection storms.
	KeepAliveMaxConnectionAge
	// KeepAliveMaxConnectionAgeGrace is an additive period after MaxConnectionAge after
	// which the connection will be forcibly closed.
	KeepAliveMaxConnectionAgeGrace
	// KeepAliveTime After a duration of this time if the server doesn't see any activity it
	// pings the client to see if the transport is still alive.
	// If set below 1s, a minimum value of 1s will be used instead.
	KeepAliveTime
	// KeepAliveTimeout After having pinged for keepalive check, the server waits for a duration
	// of Timeout and if no activity is seen even after that the connection is closed.
	KeepAliveTimeout

	// key for matching

	// MatchingRPS is request rate per second for each matching host
	MatchingRPS
	// MatchingPersistenceMaxQPS is the max qps matching host can query DB
	MatchingPersistenceMaxQPS
	// MatchingPersistenceGlobalMaxQPS is the max qps matching cluster can query DB
	MatchingPersistenceGlobalMaxQPS
	// MatchingMinTaskThrottlingBurstSize is the minimum burst size for task queue throttling
	MatchingMinTaskThrottlingBurstSize
	// MatchingGetTasksBatchSize is the maximum batch size to fetch from the task buffer
	MatchingGetTasksBatchSize
	// MatchingLongPollExpirationInterval is the long poll expiration interval in the matching service
	MatchingLongPollExpirationInterval
	// MatchingSyncMatchWaitDuration is to wait time for sync match
	MatchingSyncMatchWaitDuration
	// MatchingUpdateAckInterval is the interval for update ack
	MatchingUpdateAckInterval
	// MatchingIdleTaskqueueCheckInterval is the IdleTaskqueueCheckInterval
	MatchingIdleTaskqueueCheckInterval
	// MaxTaskqueueIdleTime is the max time taskqueue being idle
	MaxTaskqueueIdleTime
	// MatchingOutstandingTaskAppendsThreshold is the threshold for outstanding task appends
	MatchingOutstandingTaskAppendsThreshold
	// MatchingMaxTaskBatchSize is max batch size for task writer
	MatchingMaxTaskBatchSize
	// MatchingMaxTaskDeleteBatchSize is the max batch size for range deletion of tasks
	MatchingMaxTaskDeleteBatchSize
	// MatchingThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	MatchingThrottledLogRPS
	// MatchingNumTaskqueueWritePartitions is the number of write partitions for a task queue
	MatchingNumTaskqueueWritePartitions
	// MatchingNumTaskqueueReadPartitions is the number of read partitions for a task queue
	MatchingNumTaskqueueReadPartitions
	// MatchingForwarderMaxOutstandingPolls is the max number of inflight polls from the forwarder
	MatchingForwarderMaxOutstandingPolls
	// MatchingForwarderMaxOutstandingTasks is the max number of inflight addTask/queryTask from the forwarder
	MatchingForwarderMaxOutstandingTasks
	// MatchingForwarderMaxRatePerSecond is the max rate at which add/query can be forwarded
	MatchingForwarderMaxRatePerSecond
	// MatchingForwarderMaxChildrenPerNode is the max number of children per node in the task queue partition tree
	MatchingForwarderMaxChildrenPerNode
	// MatchingShutdownDrainDuration is the duration of traffic drain during shutdown
	MatchingShutdownDrainDuration

	// key for history

	// HistoryRPS is request rate per second for each history host
	HistoryRPS
	// HistoryPersistenceMaxQPS is the max qps history host can query DB
	HistoryPersistenceMaxQPS
	// HistoryPersistenceGlobalMaxQPS is the max qps history cluster can query DB
	HistoryPersistenceGlobalMaxQPS
	// HistoryLongPollExpirationInterval is the long poll expiration interval in the history service
	HistoryLongPollExpirationInterval
	// HistoryCacheInitialSize is initial size of history cache
	HistoryCacheInitialSize
	// HistoryCacheMaxSize is max size of history cache
	HistoryCacheMaxSize
	// HistoryCacheTTL is TTL of history cache
	HistoryCacheTTL
	// HistoryShutdownDrainDuration is the duration of traffic drain during shutdown
	HistoryShutdownDrainDuration
	// EventsCacheInitialSize is initial size of events cache
	EventsCacheInitialSize
	// EventsCacheMaxSize is max size of events cache
	EventsCacheMaxSize
	// EventsCacheTTL is TTL of events cache
	EventsCacheTTL
	// AcquireShardInterval is interval that timer used to acquire shard
	AcquireShardInterval
	// AcquireShardConcurrency is number of goroutines that can be used to acquire shards in the shard controller.
	AcquireShardConcurrency
	// StandbyClusterDelay is the artificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay
	// StandbyTaskMissingEventsResendDelay is the amount of time standby cluster's will wait (if events are missing)
	// before calling remote for missing events
	StandbyTaskMissingEventsResendDelay
	// StandbyTaskMissingEventsDiscardDelay is the amount of time standby cluster's will wait (if events are missing)
	// before discarding the task
	StandbyTaskMissingEventsDiscardDelay
	// TaskProcessRPS is the task processing rate per second for each namespace
	TaskProcessRPS
	// TaskSchedulerType is the task scheduler type for priority task processor
	TaskSchedulerType
	// TaskSchedulerWorkerCount is the number of workers per shard in task scheduler
	TaskSchedulerWorkerCount
	// TaskSchedulerQueueSize is the size of task channel size in task scheduler
	TaskSchedulerQueueSize
	// TaskSchedulerRoundRobinWeights is the priority weight for weighted round robin task scheduler
	TaskSchedulerRoundRobinWeights
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
	// TimerProcessorRedispatchInterval is the redispatch interval for timer processor
	TimerProcessorRedispatchInterval
	// TimerProcessorRedispatchIntervalJitterCoefficient is the redispatch interval jitter coefficient
	TimerProcessorRedispatchIntervalJitterCoefficient
	// TimerProcessorMaxRedispatchQueueSize is the threshold of the number of tasks in the redispatch queue for timer processor
	TimerProcessorMaxRedispatchQueueSize
	// TimerProcessorEnablePriorityTaskProcessor indicates whether priority task processor should be used for timer processor
	TimerProcessorEnablePriorityTaskProcessor
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
	// TransferProcessorRedispatchInterval is the redispatch interval for transferQueueProcessor
	TransferProcessorRedispatchInterval
	// TransferProcessorRedispatchIntervalJitterCoefficient is the redispatch interval jitter coefficient
	TransferProcessorRedispatchIntervalJitterCoefficient
	// TransferProcessorMaxRedispatchQueueSize is the threshold of the number of tasks in the redispatch queue for transferQueueProcessor
	TransferProcessorMaxRedispatchQueueSize
	// TransferProcessorEnablePriorityTaskProcessor indicates whether priority task processor should be used for transferQueueProcessor
	TransferProcessorEnablePriorityTaskProcessor
	// TransferProcessorVisibilityArchivalTimeLimit is the upper time limit for archiving visibility records
	TransferProcessorVisibilityArchivalTimeLimit

	// VisibilityTaskBatchSize is batch size for visibilityQueueProcessor
	VisibilityTaskBatchSize
	// VisibilityProcessorFailoverMaxPollRPS is max poll rate per second for visibilityQueueProcessor
	VisibilityProcessorFailoverMaxPollRPS
	// VisibilityProcessorMaxPollRPS is max poll rate per second for visibilityQueueProcessor
	VisibilityProcessorMaxPollRPS
	// VisibilityTaskWorkerCount is number of worker for visibilityQueueProcessor
	VisibilityTaskWorkerCount
	// VisibilityTaskMaxRetryCount is max times of retry for visibilityQueueProcessor
	VisibilityTaskMaxRetryCount
	// VisibilityProcessorCompleteTaskFailureRetryCount is times of retry for failure
	VisibilityProcessorCompleteTaskFailureRetryCount
	// VisibilityProcessorUpdateShardTaskCount is update shard count for visibilityQueueProcessor
	VisibilityProcessorUpdateShardTaskCount
	// VisibilityProcessorMaxPollInterval max poll interval for visibilityQueueProcessor
	VisibilityProcessorMaxPollInterval
	// VisibilityProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	VisibilityProcessorMaxPollIntervalJitterCoefficient
	// VisibilityProcessorUpdateAckInterval is update interval for visibilityQueueProcessor
	VisibilityProcessorUpdateAckInterval
	// VisibilityProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	VisibilityProcessorUpdateAckIntervalJitterCoefficient
	// VisibilityProcessorCompleteTaskInterval is complete timer interval for visibilityQueueProcessor
	VisibilityProcessorCompleteTaskInterval
	// VisibilityProcessorRedispatchInterval is the redispatch interval for visibilityQueueProcessor
	VisibilityProcessorRedispatchInterval
	// VisibilityProcessorRedispatchIntervalJitterCoefficient is the redispatch interval jitter coefficient
	VisibilityProcessorRedispatchIntervalJitterCoefficient
	// VisibilityProcessorMaxRedispatchQueueSize is the threshold of the number of tasks in the redispatch queue for visibilityQueueProcessor
	VisibilityProcessorMaxRedispatchQueueSize
	// VisibilityProcessorEnablePriorityTaskProcessor indicates whether priority task processor should be used for visibilityQueueProcessor
	VisibilityProcessorEnablePriorityTaskProcessor
	// VisibilityProcessorVisibilityArchivalTimeLimit is the upper time limit for archiving visibility records
	VisibilityProcessorVisibilityArchivalTimeLimit

	// TieredStorageTaskBatchSize is batch size for TieredStorageQueueProcessor
	TieredStorageTaskBatchSize
	// TieredStorageProcessorFailoverMaxPollRPS is max poll rate per second for TieredStorageQueueProcessor
	TieredStorageProcessorFailoverMaxPollRPS
	// TieredStorageProcessorMaxPollRPS is max poll rate per second for TieredStorageQueueProcessor
	TieredStorageProcessorMaxPollRPS
	// TieredStorageTaskWorkerCount is number of worker for TieredStorageQueueProcessor
	TieredStorageTaskWorkerCount
	// TieredStorageTaskMaxRetryCount is max times of retry for TieredStorageQueueProcessor
	TieredStorageTaskMaxRetryCount
	// TieredStorageProcessorCompleteTaskFailureRetryCount is times of retry for failure
	TieredStorageProcessorCompleteTaskFailureRetryCount
	// TieredStorageProcessorUpdateShardTaskCount is update shard count for TieredStorageQueueProcessor
	TieredStorageProcessorUpdateShardTaskCount
	// TieredStorageProcessorMaxPollInterval max poll interval for TieredStorageQueueProcessor
	TieredStorageProcessorMaxPollInterval
	// TieredStorageProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	TieredStorageProcessorMaxPollIntervalJitterCoefficient
	// TieredStorageProcessorUpdateAckInterval is update interval for TieredStorageQueueProcessor
	TieredStorageProcessorUpdateAckInterval
	// TieredStorageProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	TieredStorageProcessorUpdateAckIntervalJitterCoefficient
	// TieredStorageProcessorCompleteTaskInterval is complete timer interval for TieredStorageQueueProcessor
	TieredStorageProcessorCompleteTaskInterval
	// TieredStorageProcessorRedispatchInterval is the redispatch interval for TieredStorageQueueProcessor
	TieredStorageProcessorRedispatchInterval
	// TieredStorageProcessorRedispatchIntervalJitterCoefficient is the redispatch interval jitter coefficient
	TieredStorageProcessorRedispatchIntervalJitterCoefficient
	// TieredStorageProcessorMaxRedispatchQueueSize is the threshold of the number of tasks in the redispatch queue for TieredStorageQueueProcessor
	TieredStorageProcessorMaxRedispatchQueueSize
	// TieredStorageProcessorEnablePriorityTaskProcessor indicates whether priority task processor should be used for TieredStorageQueueProcessor
	TieredStorageProcessorEnablePriorityTaskProcessor
	// TieredStorageProcessorArchivalTimeLimit is the upper time limit for archiving TieredStorage records
	TieredStorageProcessorArchivalTimeLimit

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
	// ReplicatorProcessorRedispatchInterval is the redispatch interval for ReplicatorProcessor
	ReplicatorProcessorRedispatchInterval
	// ReplicatorProcessorRedispatchIntervalJitterCoefficient is the redispatch interval jitter coefficient
	ReplicatorProcessorRedispatchIntervalJitterCoefficient
	// ReplicatorProcessorMaxRedispatchQueueSize is the threshold of the number of tasks in the redispatch queue for ReplicatorProcessor
	ReplicatorProcessorMaxRedispatchQueueSize
	// ReplicatorProcessorEnablePriorityTaskProcessor indicates whether priority task processor should be used for ReplicatorProcessor
	ReplicatorProcessorEnablePriorityTaskProcessor
	// MaximumBufferedEventsBatch is max number of buffer event in mutable state
	MaximumBufferedEventsBatch
	// MaximumSignalsPerExecution is max number of signals supported by single execution
	MaximumSignalsPerExecution
	// ShardUpdateMinInterval is the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval
	// ShardSyncMinInterval is the minimal time interval which the shard info should be sync to remote
	ShardSyncMinInterval
	// ShardSyncTimerJitterCoefficient is the sync shard jitter coefficient
	ShardSyncTimerJitterCoefficient
	// DefaultEventEncoding is the encoding type for history events
	DefaultEventEncoding
	// NumArchiveSystemWorkflows is key for number of archive system workflows running in total
	NumArchiveSystemWorkflows
	// ArchiveRequestRPS is the rate limit on the number of archive request per second
	ArchiveRequestRPS
	// DefaultActivityRetryPolicy represents the out-of-box retry policy for activities where
	// the user has not specified an explicit RetryPolicy
	DefaultActivityRetryPolicy
	// DefaultWorkflowRetryPolicy represents the out-of-box retry policy for unset fields
	// where the user has set an explicit RetryPolicy, but not specified all the fields
	DefaultWorkflowRetryPolicy

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
	// StickyTTL is to expire a sticky taskqueue if no update more than this duration
	StickyTTL
	// WorkflowTaskHeartbeatTimeout for workflow task heartbeat
	WorkflowTaskHeartbeatTimeout
	// DefaultWorkflowTaskTimeout for a workflow task
	DefaultWorkflowTaskTimeout

	// EnableDropStuckTaskByNamespaceID is whether stuck timer/transfer task should be dropped for a namespace
	EnableDropStuckTaskByNamespaceID
	// SkipReapplicationByNamespaceID is whether skipping a event re-application for a namespace
	SkipReapplicationByNamespaceID

	// key for worker

	// WorkerPersistenceMaxQPS is the max qps worker host can query DB
	WorkerPersistenceMaxQPS
	// WorkerPersistenceGlobalMaxQPS is the max qps worker cluster can query DB
	WorkerPersistenceGlobalMaxQPS
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
	// WorkerESProcessorAckTimeout is the timeout that store will wait to get ack signal from ES processor.
	// Should be at least WorkerESProcessorFlushInterval+<time to process request>.
	WorkerESProcessorAckTimeout
	// WorkerArchiverMaxConcurrentActivityExecutionSize indicates worker archiver max concurrent activity execution size
	WorkerArchiverMaxConcurrentActivityExecutionSize
	// WorkerArchiverMaxConcurrentWorkflowTaskExecutionSize indicates worker archiver max concurrent workflow execution size
	WorkerArchiverMaxConcurrentWorkflowTaskExecutionSize
	// WorkerArchiverMaxConcurrentActivityTaskPollers indicates worker archiver max concurrent activity pollers
	WorkerArchiverMaxConcurrentActivityTaskPollers
	// WorkerArchiverMaxConcurrentWorkflowTaskPollers indicates worker archiver max concurrent workflow pollers
	WorkerArchiverMaxConcurrentWorkflowTaskPollers
	// WorkerArchiverConcurrency controls the number of coroutines handling archival work per archival workflow
	WorkerArchiverConcurrency
	// WorkerArchivalsPerIteration controls the number of archivals handled in each iteration of archival workflow
	WorkerArchivalsPerIteration
	// WorkerTimeLimitPerArchivalIteration controls the time limit of each iteration of archival workflow
	WorkerTimeLimitPerArchivalIteration
	// WorkerThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	WorkerThrottledLogRPS
	// WorkerScannerMaxConcurrentActivityExecutionSize indicates worker scanner max concurrent activity execution size
	WorkerScannerMaxConcurrentActivityExecutionSize
	// WorkerScannerMaxConcurrentWorkflowTaskExecutionSize indicates worker scanner max concurrent workflow execution size
	WorkerScannerMaxConcurrentWorkflowTaskExecutionSize
	// WorkerScannerMaxConcurrentActivityTaskPollers indicates worker scanner max concurrent activity pollers
	WorkerScannerMaxConcurrentActivityTaskPollers
	// WorkerScannerMaxConcurrentWorkflowTaskPollers indicates worker scanner max concurrent workflow pollers
	WorkerScannerMaxConcurrentWorkflowTaskPollers
	// ScannerPersistenceMaxQPS is the maximum rate of persistence calls from worker.Scanner
	ScannerPersistenceMaxQPS
	// TaskQueueScannerEnabled indicates if task queue scanner should be started as part of worker.Scanner
	TaskQueueScannerEnabled
	// HistoryScannerEnabled indicates if history scanner should be started as part of worker.Scanner
	HistoryScannerEnabled
	// ExecutionsScannerEnabled indicates if executions scanner should be started as part of worker.Scanner
	ExecutionsScannerEnabled
	// WorkerBatcherMaxConcurrentActivityExecutionSize indicates worker batcher max concurrent activity execution size
	WorkerBatcherMaxConcurrentActivityExecutionSize
	// WorkerBatcherMaxConcurrentWorkflowTaskExecutionSize indicates worker batcher max concurrent workflow execution size
	WorkerBatcherMaxConcurrentWorkflowTaskExecutionSize
	// WorkerBatcherMaxConcurrentActivityTaskPollers indicates worker batcher max concurrent activity pollers
	WorkerBatcherMaxConcurrentActivityTaskPollers
	// WorkerBatcherMaxConcurrentWorkflowTaskPollers indicates worker batcher max concurrent workflow pollers
	WorkerBatcherMaxConcurrentWorkflowTaskPollers
	// EnableBatcher decides whether start batcher in our worker
	EnableBatcher
	// WorkerParentCloseMaxConcurrentActivityExecutionSize indicates worker parent close worker max concurrent activity execution size
	WorkerParentCloseMaxConcurrentActivityExecutionSize
	// WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize indicates worker parent close worker max concurrent workflow execution size
	WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize
	// WorkerParentCloseMaxConcurrentActivityTaskPollers indicates worker parent close worker max concurrent activity pollers
	WorkerParentCloseMaxConcurrentActivityTaskPollers
	// WorkerParentCloseMaxConcurrentWorkflowTaskPollers indicates worker parent close worker max concurrent workflow pollers
	WorkerParentCloseMaxConcurrentWorkflowTaskPollers
	// EnableParentClosePolicyWorker decides whether or not enable system workers for processing parent close policy task
	EnableParentClosePolicyWorker
	// EnableStickyQuery indicates if sticky query should be enabled per namespace
	EnableStickyQuery

	// ReplicationTaskFetcherParallelism determines how many go routines we spin up for fetching tasks
	ReplicationTaskFetcherParallelism
	// ReplicationTaskFetcherAggregationInterval determines how frequently the fetch requests are sent
	ReplicationTaskFetcherAggregationInterval
	// ReplicationTaskFetcherTimerJitterCoefficient is the jitter for fetcher timer
	ReplicationTaskFetcherTimerJitterCoefficient
	// ReplicationTaskFetcherErrorRetryWait is the wait time when fetcher encounters error
	ReplicationTaskFetcherErrorRetryWait
	// ReplicationTaskProcessorErrorRetryWait is the initial retry wait when we see errors in applying replication tasks
	ReplicationTaskProcessorErrorRetryWait
	// ReplicationTaskProcessorErrorRetryBackoffCoefficient is the retry wait backoff time coefficient
	ReplicationTaskProcessorErrorRetryBackoffCoefficient
	// ReplicationTaskProcessorErrorRetryMaxInterval is the retry wait backoff max duration
	ReplicationTaskProcessorErrorRetryMaxInterval
	// ReplicationTaskProcessorErrorRetryMaxAttempts is the max retry attempts for applying replication tasks
	ReplicationTaskProcessorErrorRetryMaxAttempts
	// ReplicationTaskProcessorErrorRetryExpiration is the max retry duration for applying replication tasks
	ReplicationTaskProcessorErrorRetryExpiration
	// ReplicationTaskProcessorNoTaskInitialWait is the wait time when not ask is returned
	ReplicationTaskProcessorNoTaskInitialWait
	// ReplicationTaskProcessorCleanupInterval determines how frequently the cleanup replication queue
	ReplicationTaskProcessorCleanupInterval
	// ReplicationTaskProcessorCleanupJitterCoefficient is the jitter for cleanup timer
	ReplicationTaskProcessorCleanupJitterCoefficient
	// ReplicationTaskProcessorStartWait is the wait time before each task processing batch
	ReplicationTaskProcessorStartWait
	// ReplicationTaskProcessorStartWaitJitterCoefficient is the jitter for batch start wait timer
	ReplicationTaskProcessorStartWaitJitterCoefficient
	// ReplicationTaskProcessorHostQPS is the qps of task processing rate limiter on host level
	ReplicationTaskProcessorHostQPS
	// ReplicationTaskProcessorShardQPS is the qps of task processing rate limiter on shard level
	ReplicationTaskProcessorShardQPS
	// MaxBufferedQueryCount indicates max buffer query count
	MaxBufferedQueryCount
	// MutableStateChecksumGenProbability is the probability [0-100] that checksum will be generated for mutable state
	MutableStateChecksumGenProbability
	// MutableStateChecksumVerifyProbability is the probability [0-100] that checksum will be verified for mutable state
	MutableStateChecksumVerifyProbability
	// MutableStateChecksumInvalidateBefore is the epoch timestamp before which all checksums are to be discarded
	MutableStateChecksumInvalidateBefore

	// ReplicationEventsFromCurrentCluster is a feature flag to allow cross DC replicate events that generated from the current cluster
	ReplicationEventsFromCurrentCluster

	// StandbyTaskReReplicationContextTimeout is the context timeout for standby task re-replication
	StandbyTaskReReplicationContextTimeout

	EnableRingpopTLS

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
	"namespace",
	"namespaceID",
	"taskQueueName",
	"taskType",
	"shardID",
}

const (
	unknownFilter Filter = iota
	// Namespace is the namespace name
	Namespace
	// NamespaceID is the namespace Id
	NamespaceID
	// TaskQueueName is the taskqueue name
	TaskQueueName
	// TaskType is the task type (0:Workflow, 1:Activity)
	TaskType
	// ShardID is the shard id
	ShardID

	// lastFilterTypeForTest must be the last one in this const group for testing purpose
	lastFilterTypeForTest
)

const DefaultNumTaskQueuePartitions = 4

// FilterOption is used to provide filters for dynamic config keys
type FilterOption func(filterMap map[Filter]interface{})

// TaskQueueFilter filters by task queue name
func TaskQueueFilter(name string) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[TaskQueueName] = name
	}
}

// NamespaceFilter filters by namespace name
func NamespaceFilter(name string) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[Namespace] = name
	}
}

// NamespaceIDFilter filters by namespace id
func NamespaceIDFilter(namespaceID string) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[NamespaceID] = namespaceID
	}
}

// TaskTypeFilter filters by task type
func TaskTypeFilter(taskType enumspb.TaskQueueType) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[TaskType] = enumspb.TaskQueueType_name[int32(taskType)]
	}
}

// ShardIDFilter filters by shard id
func ShardIDFilter(shardID int32) FilterOption {
	return func(filterMap map[Filter]interface{}) {
		filterMap[ShardID] = shardID
	}
}
