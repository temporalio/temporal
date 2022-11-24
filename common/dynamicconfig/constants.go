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

func (k Key) String() string {
	return string(k)
}

const (
	// keys for admin

	// AdminMatchingNamespaceToPartitionDispatchRate is the max qps of any task queue partition for a given namespace
	AdminMatchingNamespaceToPartitionDispatchRate = "admin.matchingNamespaceToPartitionDispatchRate"
	// AdminMatchingNamespaceTaskqueueToPartitionDispatchRate is the max qps of a task queue partition for a given namespace & task queue
	AdminMatchingNamespaceTaskqueueToPartitionDispatchRate = "admin.matchingNamespaceTaskqueueToPartitionDispatchRate"

	// keys for system

	// StandardVisibilityPersistenceMaxReadQPS is the max QPC system host can query standard visibility DB (SQL or Cassandra) for read.
	StandardVisibilityPersistenceMaxReadQPS = "system.standardVisibilityPersistenceMaxReadQPS"
	// StandardVisibilityPersistenceMaxWriteQPS is the max QPC system host can query standard visibility DB (SQL or Cassandra) for write.
	StandardVisibilityPersistenceMaxWriteQPS = "system.standardVisibilityPersistenceMaxWriteQPS"
	// AdvancedVisibilityPersistenceMaxReadQPS is the max QPC system host can query advanced visibility DB (Elasticsearch) for read.
	AdvancedVisibilityPersistenceMaxReadQPS = "system.advancedVisibilityPersistenceMaxReadQPS"
	// AdvancedVisibilityPersistenceMaxWriteQPS is the max QPC system host can query advanced visibility DB (Elasticsearch) for write.
	AdvancedVisibilityPersistenceMaxWriteQPS = "system.advancedVisibilityPersistenceMaxWriteQPS"
	// AdvancedVisibilityWritingMode is key for how to write to advanced visibility
	AdvancedVisibilityWritingMode = "system.advancedVisibilityWritingMode"
	// EnableWriteToSecondaryAdvancedVisibility is the config to enable write to secondary visibility for Elasticsearch
	EnableWriteToSecondaryAdvancedVisibility = "system.enableWriteToSecondaryAdvancedVisibility"
	// EnableReadVisibilityFromES is key for enable read from Elasticsearch
	EnableReadVisibilityFromES = "system.enableReadVisibilityFromES"
	// EnableReadFromSecondaryAdvancedVisibility is the config to enable read from secondary Elasticsearch
	EnableReadFromSecondaryAdvancedVisibility = "system.enableReadFromSecondaryAdvancedVisibility"
	// VisibilityDisableOrderByClause is the config to disable ORDERY BY clause for Elasticsearch
	VisibilityDisableOrderByClause = "system.visibilityDisableOrderByClause"

	// HistoryArchivalState is key for the state of history archival
	HistoryArchivalState = "system.historyArchivalState"
	// EnableReadFromHistoryArchival is key for enabling reading history from archival store
	EnableReadFromHistoryArchival = "system.enableReadFromHistoryArchival"
	// VisibilityArchivalState is key for the state of visibility archival
	VisibilityArchivalState = "system.visibilityArchivalState"
	// EnableReadFromVisibilityArchival is key for enabling reading visibility from archival store
	EnableReadFromVisibilityArchival = "system.enableReadFromVisibilityArchival"
	// EnableNamespaceNotActiveAutoForwarding whether enabling DC auto forwarding to active cluster
	// for signal / start / signal with start API if namespace is not active
	EnableNamespaceNotActiveAutoForwarding = "system.enableNamespaceNotActiveAutoForwarding"
	// TransactionSizeLimit is the largest allowed transaction size to persistence
	TransactionSizeLimit = "system.transactionSizeLimit"
	// DisallowQuery is the key to disallow query for a namespace
	DisallowQuery = "system.disallowQuery"
	// EnableAuthorization is the key to enable authorization for a namespace
	EnableAuthorization = "system.enableAuthorization"
	// EnableCrossNamespaceCommands is the key to enable commands for external namespaces
	EnableCrossNamespaceCommands = "system.enableCrossNamespaceCommands"
	// ClusterMetadataRefreshInterval is config to manage cluster metadata table refresh interval
	ClusterMetadataRefreshInterval = "system.clusterMetadataRefreshInterval"
	// ForceSearchAttributesCacheRefreshOnRead forces refreshing search attributes cache on a read operation, so we always
	// get the latest data from DB. This effectively bypasses cache value and is used to facilitate testing of changes in
	// search attributes. This should not be turned on in production.
	ForceSearchAttributesCacheRefreshOnRead = "system.forceSearchAttributesCacheRefreshOnRead"
	EnableRingpopTLS                        = "system.enableRingpopTLS"
	// EnableParentClosePolicyWorker decides whether or not enable system workers for processing parent close policy task
	EnableParentClosePolicyWorker = "system.enableParentClosePolicyWorker"
	// EnableStickyQuery indicates if sticky query should be enabled per namespace
	EnableStickyQuery = "system.enableStickyQuery"
	// EnableActivityEagerExecution indicates if acitivty eager execution is enabled per namespace
	EnableActivityEagerExecution = "system.enableActivityEagerExecution"
	// NamespaceCacheRefreshInterval is the key for namespace cache refresh interval dynamic config
	NamespaceCacheRefreshInterval = "system.namespaceCacheRefreshInterval"

	// Whether the deadlock detector should dump goroutines
	DeadlockDumpGoroutines = "system.deadlock.DumpGoroutines"
	// Whether the deadlock detector should cause the grpc server to fail health checks
	DeadlockFailHealthCheck = "system.deadlock.FailHealthCheck"
	// Whether the deadlock detector should abort the process
	DeadlockAbortProcess = "system.deadlock.AbortProcess"
	// How often the detector checks each root.
	DeadlockInterval = "system.deadlock.Interval"
	// How many extra goroutines can be created per root.
	DeadlockMaxWorkersPerRoot = "system.deadlock.MaxWorkersPerRoot"

	// keys for size limit

	// BlobSizeLimitError is the per event blob size limit
	BlobSizeLimitError = "limit.blobSize.error"
	// BlobSizeLimitWarn is the per event blob size limit for warning
	BlobSizeLimitWarn = "limit.blobSize.warn"
	// MemoSizeLimitError is the per event memo size limit
	MemoSizeLimitError = "limit.memoSize.error"
	// MemoSizeLimitWarn is the per event memo size limit for warning
	MemoSizeLimitWarn = "limit.memoSize.warn"
	// NumPendingChildExecutionsLimitError is the maximum number of pending child workflows a workflow can have before
	// StartChildWorkflowExecution commands will fail.
	NumPendingChildExecutionsLimitError = "limit.numPendingChildExecutions.error"
	// NumPendingActivitiesLimitError is the maximum number of pending activities a workflow can have before
	// ScheduleActivityTask will fail.
	NumPendingActivitiesLimitError = "limit.numPendingActivities.error"
	// NumPendingSignalsLimitError is the maximum number of pending signals a workflow can have before
	// SignalExternalWorkflowExecution commands from this workflow will fail.
	NumPendingSignalsLimitError = "limit.numPendingSignals.error"
	// NumPendingCancelRequestsLimitError is the maximum number of pending requests to cancel other workflows a workflow can have before
	// RequestCancelExternalWorkflowExecution commands will fail.
	NumPendingCancelRequestsLimitError = "limit.numPendingCancelRequests.error"
	// HistorySizeLimitError is the per workflow execution history size limit
	HistorySizeLimitError = "limit.historySize.error"
	// HistorySizeLimitWarn is the per workflow execution history size limit for warning
	HistorySizeLimitWarn = "limit.historySize.warn"
	// HistoryCountLimitError is the per workflow execution history event count limit
	HistoryCountLimitError = "limit.historyCount.error"
	// HistoryCountLimitWarn is the per workflow execution history event count limit for warning
	HistoryCountLimitWarn = "limit.historyCount.warn"
	// MaxIDLengthLimit is the length limit for various IDs, including: Namespace, TaskQueue, WorkflowID, ActivityID, TimerID,
	// WorkflowType, ActivityType, SignalName, MarkerName, ErrorReason/FailureReason/CancelCause, Identity, RequestID
	MaxIDLengthLimit = "limit.maxIDLength"
	// WorkerBuildIdSizeLimit is the byte length limit for a worker build id as used in the rpc methods for updating
	// the version graph for a task queue
	WorkerBuildIdSizeLimit = "limit.workerBuildIdSize"
	// VersionGraphNodeLimit is the max number of nodes allowed in the version graph for a task queue. Update requests
	// which would cause the graph size to exceed this number will result in the oldest versions being dropped.
	VersionGraphNodeLimit = "limit.versionGraphNodeSize"

	// keys for frontend

	// FrontendPersistenceMaxQPS is the max qps frontend host can query DB
	FrontendPersistenceMaxQPS = "frontend.persistenceMaxQPS"
	// FrontendPersistenceGlobalMaxQPS is the max qps frontend cluster can query DB
	FrontendPersistenceGlobalMaxQPS = "frontend.persistenceGlobalMaxQPS"
	// FrontendPersistenceNamespaceMaxQPS is the max qps each namespace on frontend host can query DB
	FrontendPersistenceNamespaceMaxQPS = "frontend.persistenceNamespaceMaxQPS"
	// FrontendEnablePersistencePriorityRateLimiting indicates if priority rate limiting is enabled in frontend persistence client
	FrontendEnablePersistencePriorityRateLimiting = "frontend.enablePersistencePriorityRateLimiting"
	// FrontendVisibilityMaxPageSize is default max size for ListWorkflowExecutions in one page
	FrontendVisibilityMaxPageSize = "frontend.visibilityMaxPageSize"
	// FrontendESIndexMaxResultWindow is ElasticSearch index setting max_result_window
	FrontendESIndexMaxResultWindow = "frontend.esIndexMaxResultWindow"
	// FrontendHistoryMaxPageSize is default max size for GetWorkflowExecutionHistory in one page
	FrontendHistoryMaxPageSize = "frontend.historyMaxPageSize"
	// FrontendRPS is workflow rate limit per second
	FrontendRPS = "frontend.rps"
	// FrontendMaxNamespaceRPSPerInstance is workflow namespace rate limit per second
	FrontendMaxNamespaceRPSPerInstance = "frontend.namespaceRPS"
	// FrontendMaxNamespaceBurstPerInstance is workflow namespace burst limit
	FrontendMaxNamespaceBurstPerInstance = "frontend.namespaceBurst"
	// FrontendMaxNamespaceCountPerInstance is workflow namespace count limit per second
	FrontendMaxNamespaceCountPerInstance = "frontend.namespaceCount"
	// FrontendMaxNamespaceVisibilityRPSPerInstance is namespace rate limit per second for visibility APIs.
	// This config is EXPERIMENTAL and may be changed or removed in a later release.
	FrontendMaxNamespaceVisibilityRPSPerInstance = "frontend.namespaceRPS.visibility"
	// FrontendMaxNamespaceVisibilityBurstPerInstance is namespace burst limit for visibility APIs.
	// This config is EXPERIMENTAL and may be changed or removed in a later release.
	FrontendMaxNamespaceVisibilityBurstPerInstance = "frontend.namespaceBurst.visibility"
	// FrontendGlobalNamespaceRPS is workflow namespace rate limit per second for the whole cluster.
	// The limit is evenly distributed among available frontend service instances.
	// If this is set, it overwrites per instance limit "frontend.namespaceRPS".
	FrontendGlobalNamespaceRPS = "frontend.globalNamespaceRPS"
	// FrontendGlobalNamespaceVisibilityRPS is workflow namespace rate limit per second for the whole cluster for visibility API.
	// The limit is evenly distributed among available frontend service instances.
	// If this is set, it overwrites per instance limit "frontend.namespaceRPS.visibility".
	// This config is EXPERIMENTAL and may be changed or removed in a later release.
	FrontendGlobalNamespaceVisibilityRPS = "frontend.globalNamespaceRPS.visibility"
	// FrontendThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	FrontendThrottledLogRPS = "frontend.throttledLogRPS"
	// FrontendShutdownDrainDuration is the duration of traffic drain during shutdown
	FrontendShutdownDrainDuration = "frontend.shutdownDrainDuration"
	// FrontendMaxBadBinaries is the max number of bad binaries in namespace config
	FrontendMaxBadBinaries = "frontend.maxBadBinaries"
	// SendRawWorkflowHistory is whether to enable raw history retrieving
	SendRawWorkflowHistory = "frontend.sendRawWorkflowHistory"
	// SearchAttributesNumberOfKeysLimit is the limit of number of keys
	SearchAttributesNumberOfKeysLimit = "frontend.searchAttributesNumberOfKeysLimit"
	// SearchAttributesSizeOfValueLimit is the size limit of each value
	SearchAttributesSizeOfValueLimit = "frontend.searchAttributesSizeOfValueLimit"
	// SearchAttributesTotalSizeLimit is the size limit of the whole map
	SearchAttributesTotalSizeLimit = "frontend.searchAttributesTotalSizeLimit"
	// VisibilityArchivalQueryMaxPageSize is the maximum page size for a visibility archival query
	VisibilityArchivalQueryMaxPageSize = "frontend.visibilityArchivalQueryMaxPageSize"
	// VisibilityArchivalQueryMaxRangeInDays is the maximum number of days for a visibility archival query
	VisibilityArchivalQueryMaxRangeInDays = "frontend.visibilityArchivalQueryMaxRangeInDays"
	// VisibilityArchivalQueryMaxQPS is the timeout for a visibility archival query
	VisibilityArchivalQueryMaxQPS = "frontend.visibilityArchivalQueryMaxQPS"
	// EnableServerVersionCheck is a flag that controls whether or not periodic version checking is enabled
	EnableServerVersionCheck = "frontend.enableServerVersionCheck"
	// EnableTokenNamespaceEnforcement enables enforcement that namespace in completion token matches namespace of the request
	EnableTokenNamespaceEnforcement = "frontend.enableTokenNamespaceEnforcement"
	// DisableListVisibilityByFilter is config to disable list open/close workflow using filter
	DisableListVisibilityByFilter = "frontend.disableListVisibilityByFilter"
	// KeepAliveMinTime is the minimum amount of time a client should wait before sending a keepalive ping.
	KeepAliveMinTime = "frontend.keepAliveMinTime"
	// KeepAlivePermitWithoutStream If true, server allows keepalive pings even when there are no active
	// streams(RPCs). If false, and client sends ping when there are no active
	// streams, server will send GOAWAY and close the connection.
	KeepAlivePermitWithoutStream = "frontend.keepAlivePermitWithoutStream"
	// KeepAliveMaxConnectionIdle is a duration for the amount of time after which an
	// idle connection would be closed by sending a GoAway. Idleness duration is
	// defined since the most recent time the number of outstanding RPCs became
	// zero or the connection establishment.
	KeepAliveMaxConnectionIdle = "frontend.keepAliveMaxConnectionIdle"
	// KeepAliveMaxConnectionAge is a duration for the maximum amount of time a
	// connection may exist before it will be closed by sending a GoAway. A
	// random jitter of +/-10% will be added to MaxConnectionAge to spread out
	// connection storms.
	KeepAliveMaxConnectionAge = "frontend.keepAliveMaxConnectionAge"
	// KeepAliveMaxConnectionAgeGrace is an additive period after MaxConnectionAge after
	// which the connection will be forcibly closed.
	KeepAliveMaxConnectionAgeGrace = "frontend.keepAliveMaxConnectionAgeGrace"
	// KeepAliveTime After a duration of this time if the server doesn't see any activity it
	// pings the client to see if the transport is still alive.
	// If set below 1s, a minimum value of 1s will be used instead.
	KeepAliveTime = "frontend.keepAliveTime"
	// KeepAliveTimeout After having pinged for keepalive check, the server waits for a duration
	// of Timeout and if no activity is seen even after that the connection is closed.
	KeepAliveTimeout = "frontend.keepAliveTimeout"
	// FrontendEnableSchedules enables schedule-related RPCs in the frontend
	FrontendEnableSchedules = "frontend.enableSchedules"
	// FrontendMaxConcurrentBatchOperationPerNamespace is the max concurrent batch operation job count per namespace
	FrontendMaxConcurrentBatchOperationPerNamespace = "frontend.MaxConcurrentBatchOperationPerNamespace"
	// FrontendEnableBatcher enables batcher-related RPCs in the frontend
	FrontendEnableBatcher = "frontend.enableBatcher"

	// DeleteNamespaceDeleteActivityRPS is an RPS per every parallel delete executions activity.
	// Total RPS is equal to DeleteNamespaceDeleteActivityRPS * DeleteNamespaceConcurrentDeleteExecutionsActivities.
	// Default value is 100.
	DeleteNamespaceDeleteActivityRPS = "frontend.deleteNamespaceDeleteActivityRPS"
	// DeleteNamespacePageSize is a page size to read executions from visibility for delete executions activity.
	// Default value is 1000.
	DeleteNamespacePageSize = "frontend.deleteNamespaceDeletePageSize"
	// DeleteNamespacePagesPerExecution is a number of pages before returning ContinueAsNew from delete executions activity.
	// Default value is 256.
	DeleteNamespacePagesPerExecution = "frontend.deleteNamespacePagesPerExecution"
	// DeleteNamespaceConcurrentDeleteExecutionsActivities is a number of concurrent delete executions activities.
	// Must be not greater than 256 and number of worker cores in the cluster.
	// Default is 4.
	DeleteNamespaceConcurrentDeleteExecutionsActivities = "frontend.deleteNamespaceConcurrentDeleteExecutionsActivities"
	// DeleteNamespaceNamespaceDeleteDelay is a duration for how long namespace stays in database
	// after all namespace resources (i.e. workflow executions) are deleted.
	// Default is 0, means, namespace will be deleted immediately.
	DeleteNamespaceNamespaceDeleteDelay = "frontend.deleteNamespaceNamespaceDeleteDelay"

	// keys for matching

	// MatchingRPS is request rate per second for each matching host
	MatchingRPS = "matching.rps"
	// MatchingPersistenceMaxQPS is the max qps matching host can query DB
	MatchingPersistenceMaxQPS = "matching.persistenceMaxQPS"
	// MatchingPersistenceGlobalMaxQPS is the max qps matching cluster can query DB
	MatchingPersistenceGlobalMaxQPS = "matching.persistenceGlobalMaxQPS"
	// MatchingPersistenceNamespaceMaxQPS is the max qps each namespace on matching host can query DB
	MatchingPersistenceNamespaceMaxQPS = "matching.persistenceNamespaceMaxQPS"
	// MatchingEnablePersistencePriorityRateLimiting indicates if priority rate limiting is enabled in matching persistence client
	MatchingEnablePersistencePriorityRateLimiting = "matching.enablePersistencePriorityRateLimiting"
	// MatchingMinTaskThrottlingBurstSize is the minimum burst size for task queue throttling
	MatchingMinTaskThrottlingBurstSize = "matching.minTaskThrottlingBurstSize"
	// MatchingGetTasksBatchSize is the maximum batch size to fetch from the task buffer
	MatchingGetTasksBatchSize = "matching.getTasksBatchSize"
	// MatchingLongPollExpirationInterval is the long poll expiration interval in the matching service
	MatchingLongPollExpirationInterval = "matching.longPollExpirationInterval"
	// MatchingSyncMatchWaitDuration is to wait time for sync match
	MatchingSyncMatchWaitDuration = "matching.syncMatchWaitDuration"
	// MatchingUpdateAckInterval is the interval for update ack
	MatchingUpdateAckInterval = "matching.updateAckInterval"
	// MatchingIdleTaskqueueCheckInterval is the IdleTaskqueueCheckInterval
	MatchingIdleTaskqueueCheckInterval = "matching.idleTaskqueueCheckInterval"
	// MaxTaskqueueIdleTime is the max time taskqueue being idle
	MaxTaskqueueIdleTime = "matching.maxTaskqueueIdleTime"
	// MatchingOutstandingTaskAppendsThreshold is the threshold for outstanding task appends
	MatchingOutstandingTaskAppendsThreshold = "matching.outstandingTaskAppendsThreshold"
	// MatchingMaxTaskBatchSize is max batch size for task writer
	MatchingMaxTaskBatchSize = "matching.maxTaskBatchSize"
	// MatchingMaxTaskDeleteBatchSize is the max batch size for range deletion of tasks
	MatchingMaxTaskDeleteBatchSize = "matching.maxTaskDeleteBatchSize"
	// MatchingThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	MatchingThrottledLogRPS = "matching.throttledLogRPS"
	// MatchingNumTaskqueueWritePartitions is the number of write partitions for a task queue
	MatchingNumTaskqueueWritePartitions = "matching.numTaskqueueWritePartitions"
	// MatchingNumTaskqueueReadPartitions is the number of read partitions for a task queue
	MatchingNumTaskqueueReadPartitions = "matching.numTaskqueueReadPartitions"
	// MatchingForwarderMaxOutstandingPolls is the max number of inflight polls from the forwarder
	MatchingForwarderMaxOutstandingPolls = "matching.forwarderMaxOutstandingPolls"
	// MatchingForwarderMaxOutstandingTasks is the max number of inflight addTask/queryTask from the forwarder
	MatchingForwarderMaxOutstandingTasks = "matching.forwarderMaxOutstandingTasks"
	// MatchingForwarderMaxRatePerSecond is the max rate at which add/query can be forwarded
	MatchingForwarderMaxRatePerSecond = "matching.forwarderMaxRatePerSecond"
	// MatchingForwarderMaxChildrenPerNode is the max number of children per node in the task queue partition tree
	MatchingForwarderMaxChildrenPerNode = "matching.forwarderMaxChildrenPerNode"
	// MatchingShutdownDrainDuration is the duration of traffic drain during shutdown
	MatchingShutdownDrainDuration = "matching.shutdownDrainDuration"
	// MatchingMetadataPollFrequency is how often non-root partitions will poll the root partition for fresh metadata
	MatchingMetadataPollFrequency = "matching.metadataPollFrequency"
	// MatchingUseOldRouting is whether to use old task queue routing (name only) instead of namespace+name+type.
	MatchingUseOldRouting = "matching.useOldRouting"

	// keys for history

	// HistoryRPS is request rate per second for each history host
	HistoryRPS = "history.rps"
	// HistoryPersistenceMaxQPS is the max qps history host can query DB
	HistoryPersistenceMaxQPS = "history.persistenceMaxQPS"
	// HistoryPersistenceGlobalMaxQPS is the max qps history cluster can query DB
	HistoryPersistenceGlobalMaxQPS = "history.persistenceGlobalMaxQPS"
	// HistoryPersistenceNamespaceMaxQPS is the max qps each namespace on history host can query DB
	// If value less or equal to 0, will fall back to HistoryPersistenceMaxQPS
	HistoryPersistenceNamespaceMaxQPS = "history.persistenceNamespaceMaxQPS"
	// HistoryEnablePersistencePriorityRateLimiting indicates if priority rate limiting is enabled in history persistence client
	HistoryEnablePersistencePriorityRateLimiting = "history.enablePersistencePriorityRateLimiting"
	// HistoryLongPollExpirationInterval is the long poll expiration interval in the history service
	HistoryLongPollExpirationInterval = "history.longPollExpirationInterval"
	// HistoryCacheInitialSize is initial size of history cache
	HistoryCacheInitialSize = "history.cacheInitialSize"
	// HistoryCacheMaxSize is max size of history cache
	HistoryCacheMaxSize = "history.cacheMaxSize"
	// HistoryCacheTTL is TTL of history cache
	HistoryCacheTTL = "history.cacheTTL"
	// HistoryShutdownDrainDuration is the duration of traffic drain during shutdown
	HistoryShutdownDrainDuration = "history.shutdownDrainDuration"
	// EventsCacheInitialSize is initial size of events cache
	EventsCacheInitialSize = "history.eventsCacheInitialSize"
	// EventsCacheMaxSize is max size of events cache
	EventsCacheMaxSize = "history.eventsCacheMaxSize"
	// EventsCacheTTL is TTL of events cache
	EventsCacheTTL = "history.eventsCacheTTL"
	// AcquireShardInterval is interval that timer used to acquire shard
	AcquireShardInterval = "history.acquireShardInterval"
	// AcquireShardConcurrency is number of goroutines that can be used to acquire shards in the shard controller.
	AcquireShardConcurrency = "history.acquireShardConcurrency"
	// StandbyClusterDelay is the artificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay = "history.standbyClusterDelay"
	// StandbyTaskMissingEventsResendDelay is the amount of time standby cluster's will wait (if events are missing)
	// before calling remote for missing events
	StandbyTaskMissingEventsResendDelay = "history.standbyTaskMissingEventsResendDelay"
	// StandbyTaskMissingEventsDiscardDelay is the amount of time standby cluster's will wait (if events are missing)
	// before discarding the task
	StandbyTaskMissingEventsDiscardDelay = "history.standbyTaskMissingEventsDiscardDelay"
	// QueuePendingTaskCriticalCount is the max number of pending task in one queue
	// before triggering queue slice splitting and unloading
	QueuePendingTaskCriticalCount = "history.queuePendingTaskCriticalCount"
	// QueueReaderStuckCriticalAttempts is the max number of task loading attempts for a certain task range
	// before that task range is split into a separate slice to unblock loading for later range.
	// currently only work for scheduled queues and the task range is 1s.
	QueueReaderStuckCriticalAttempts = "history.queueReaderStuckCriticalAttempts"
	// QueueCriticalSlicesCount is the max number of slices in one queue
	// before force compacting slices
	QueueCriticalSlicesCount = "history.queueCriticalSlicesCount"
	// QueuePendingTaskMaxCount is the max number of task pending tasks in one queue before stop
	// loading new tasks into memory. While QueuePendingTaskCriticalCount won't stop task loading
	// for the entire queue but only trigger a queue action to unload tasks. Ideally this max count
	// limit should not be hit and task unloading should happen once critical count is exceeded. But
	// since queue action is async, we need this hard limit.
	QueuePendingTaskMaxCount = "history.queuePendingTasksMaxCount"
	// QueueMaxReaderCount is the max number of readers in one multi-cursor queue
	QueueMaxReaderCount = "history.queueMaxReaderCount"
	// ContinueAsNewMinInterval is the minimal interval between continue_as_new executions.
	// This is needed to prevent tight loop continue_as_new spin. Default is 1s.
	ContinueAsNewMinInterval = "history.continueAsNewMinInterval"

	// TaskSchedulerEnableRateLimiter indicates if rate limiter should be enabled in task scheduler
	TaskSchedulerEnableRateLimiter = "history.taskSchedulerEnableRateLimiter"
	// TaskSchedulerMaxQPS is the max qps task schedulers on a host can schedule tasks
	// If value less or equal to 0, will fall back to HistoryPersistenceMaxQPS
	TaskSchedulerMaxQPS = "history.taskSchedulerMaxQPS"
	// TaskSchedulerNamespaceMaxQPS is the max qps task schedulers on a host can schedule tasks for a certain namespace
	// If value less or equal to 0, will fall back to HistoryPersistenceNamespaceMaxQPS
	TaskSchedulerNamespaceMaxQPS = "history.taskSchedulerNamespaceMaxQPS"

	// TimerTaskBatchSize is batch size for timer processor to process tasks
	TimerTaskBatchSize = "history.timerTaskBatchSize"
	// TimerTaskWorkerCount is number of task workers for timer processor
	TimerTaskWorkerCount = "history.timerTaskWorkerCount"
	// TimerTaskMaxRetryCount is max retry count for timer processor
	TimerTaskMaxRetryCount = "history.timerTaskMaxRetryCount"
	// TimerProcessorEnableSingleProcessor indicates if only one timer processor should be started for both active and standby tasks
	// NOTE: this is an experimental flag for guarantee task execution and will be deprecated after multicursor solution is ready
	TimerProcessorEnableSingleProcessor = "history.timerProcessorEnableSingleProcessor"
	// TimerProcessorEnableMultiCursor indicates if the new single processor multi-cursor timer processor implementation should be used
	// NOTE: this flag only takes effect when history.timerProcessorEnablePriorityTaskScheduler is enabled.
	TimerProcessorEnableMultiCursor = "history.timerProcessorEnableMultiCursor"
	// TimerProcessorEnablePriorityTaskScheduler indicates whether host level priority task scheduler should be used for timer processor
	TimerProcessorEnablePriorityTaskScheduler = "history.timerProcessorEnablePriorityTaskScheduler"
	// TimerProcessorSchedulerWorkerCount is the number of workers in the host level task scheduler for timer processor
	TimerProcessorSchedulerWorkerCount = "history.timerProcessorSchedulerWorkerCount"
	// TimerProcessorSchedulerActiveRoundRobinWeights is the priority round robin weights used by timer task scheduler for active namespaces
	TimerProcessorSchedulerActiveRoundRobinWeights = "history.timerProcessorSchedulerActiveRoundRobinWeights"
	// TimerProcessorSchedulerStandbyRoundRobinWeights is the priority round robin weights used by timer task scheduler for standby namespaces
	TimerProcessorSchedulerStandbyRoundRobinWeights = "history.timerProcessorSchedulerStandbyRoundRobinWeights"
	// TimerProcessorUpdateAckInterval is update interval for timer processor
	TimerProcessorUpdateAckInterval = "history.timerProcessorUpdateAckInterval"
	// TimerProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	TimerProcessorUpdateAckIntervalJitterCoefficient = "history.timerProcessorUpdateAckIntervalJitterCoefficient"
	// TimerProcessorCompleteTimerInterval is complete timer interval for timer processor
	TimerProcessorCompleteTimerInterval = "history.timerProcessorCompleteTimerInterval"
	// TimerProcessorFailoverMaxPollRPS is max poll rate per second for timer processor
	TimerProcessorFailoverMaxPollRPS = "history.timerProcessorFailoverMaxPollRPS"
	// TimerProcessorMaxPollRPS is max poll rate per second for timer processor
	TimerProcessorMaxPollRPS = "history.timerProcessorMaxPollRPS"
	// TimerProcessorMaxPollHostRPS is max poll rate per second for all timer processor on a host
	TimerProcessorMaxPollHostRPS = "history.timerProcessorMaxPollHostRPS"
	// TimerProcessorMaxPollInterval is max poll interval for timer processor
	TimerProcessorMaxPollInterval = "history.timerProcessorMaxPollInterval"
	// TimerProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	TimerProcessorMaxPollIntervalJitterCoefficient = "history.timerProcessorMaxPollIntervalJitterCoefficient"
	// TimerProcessorMaxReschedulerSize is the threshold of the number of tasks in the redispatch queue for timer processor
	TimerProcessorMaxReschedulerSize = "history.timerProcessorMaxReschedulerSize"
	// TimerProcessorPollBackoffInterval is the poll backoff interval if task redispatcher's size exceeds limit for timer processor
	TimerProcessorPollBackoffInterval = "history.timerProcessorPollBackoffInterval"
	// TimerProcessorMaxTimeShift is the max shift timer processor can have
	TimerProcessorMaxTimeShift = "history.timerProcessorMaxTimeShift"
	// TimerProcessorHistoryArchivalSizeLimit is the max history size for inline archival
	TimerProcessorHistoryArchivalSizeLimit = "history.timerProcessorHistoryArchivalSizeLimit"
	// TimerProcessorArchivalTimeLimit is the upper time limit for inline history archival
	TimerProcessorArchivalTimeLimit = "history.timerProcessorArchivalTimeLimit"
	// RetentionTimerJitterDuration is a time duration jitter to distribute timer from T0 to T0 + jitter duration
	RetentionTimerJitterDuration = "history.retentionTimerJitterDuration"

	// TransferTaskBatchSize is batch size for transferQueueProcessor
	TransferTaskBatchSize = "history.transferTaskBatchSize"
	// TransferProcessorFailoverMaxPollRPS is max poll rate per second for transferQueueProcessor
	TransferProcessorFailoverMaxPollRPS = "history.transferProcessorFailoverMaxPollRPS"
	// TransferProcessorMaxPollRPS is max poll rate per second for transferQueueProcessor
	TransferProcessorMaxPollRPS = "history.transferProcessorMaxPollRPS"
	// TransferProcessorMaxPollHostRPS is max poll rate per second for all transferQueueProcessor on a host
	TransferProcessorMaxPollHostRPS = "history.transferProcessorMaxPollHostRPS"
	// TransferTaskWorkerCount is number of worker for transferQueueProcessor
	TransferTaskWorkerCount = "history.transferTaskWorkerCount"
	// TransferTaskMaxRetryCount is max times of retry for transferQueueProcessor
	TransferTaskMaxRetryCount = "history.transferTaskMaxRetryCount"
	// TransferProcessorEnableSingleProcessor indicates if only one transfer processor should be started for both active and standby tasks
	// NOTE: this is an experimental flag for guarantee task execution and will be deprecated after multicursor solution is ready
	TransferProcessorEnableSingleProcessor = "history.transferProcessorEnableSingleProcessor"
	// TransferProcessorEnableMultiCursor indicates if the new single processor multi-cursor transfer processor implementation should be used
	// NOTE: this flag only takes effect when history.transferProcessorEnablePriorityTaskScheduler is enabled.
	TransferProcessorEnableMultiCursor = "history.transferProcessorEnableMultiCursor"
	// TransferProcessorEnablePriorityTaskScheduler indicates whether host level priority task scheduler should be used for transferQueueProcessor
	TransferProcessorEnablePriorityTaskScheduler = "history.transferProcessorEnablePriorityTaskScheduler"
	// TransferProcessorSchedulerWorkerCount is the number of workers in the host level task scheduler for transferQueueProcessor
	TransferProcessorSchedulerWorkerCount = "history.transferProcessorSchedulerWorkerCount"
	// TransferProcessorSchedulerActiveRoundRobinWeights is the priority round robin weights used by transfer task scheduler for active namespaces
	TransferProcessorSchedulerActiveRoundRobinWeights = "history.transferProcessorSchedulerActiveRoundRobinWeights"
	// TransferProcessorSchedulerStandbyRoundRobinWeights is the priority round robin weights used by transfer task scheduler for standby namespaces
	TransferProcessorSchedulerStandbyRoundRobinWeights = "history.transferProcessorSchedulerStandbyRoundRobinWeights"
	// TransferProcessorUpdateShardTaskCount is update shard count for transferQueueProcessor
	TransferProcessorUpdateShardTaskCount = "history.transferProcessorUpdateShardTaskCount"
	// TransferProcessorMaxPollInterval max poll interval for transferQueueProcessor
	TransferProcessorMaxPollInterval = "history.transferProcessorMaxPollInterval"
	// TransferProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	TransferProcessorMaxPollIntervalJitterCoefficient = "history.transferProcessorMaxPollIntervalJitterCoefficient"
	// TransferProcessorUpdateAckInterval is update interval for transferQueueProcessor
	TransferProcessorUpdateAckInterval = "history.transferProcessorUpdateAckInterval"
	// TransferProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	TransferProcessorUpdateAckIntervalJitterCoefficient = "history.transferProcessorUpdateAckIntervalJitterCoefficient"
	// TransferProcessorCompleteTransferInterval is complete timer interval for transferQueueProcessor
	TransferProcessorCompleteTransferInterval = "history.transferProcessorCompleteTransferInterval"
	// TransferProcessorMaxReschedulerSize is the threshold of the number of tasks in the redispatch queue for transferQueueProcessor
	TransferProcessorMaxReschedulerSize = "history.transferProcessorMaxReschedulerSize"
	// TransferProcessorPollBackoffInterval is the poll backoff interval if task redispatcher's size exceeds limit for transferQueueProcessor
	TransferProcessorPollBackoffInterval = "history.transferProcessorPollBackoffInterval"
	// TransferProcessorVisibilityArchivalTimeLimit is the upper time limit for archiving visibility records
	TransferProcessorVisibilityArchivalTimeLimit = "history.transferProcessorVisibilityArchivalTimeLimit"
	// TransferProcessorEnsureCloseBeforeDelete means we ensure the execution is closed before we delete it
	TransferProcessorEnsureCloseBeforeDelete = "history.transferProcessorEnsureCloseBeforeDelete"

	// VisibilityTaskBatchSize is batch size for visibilityQueueProcessor
	VisibilityTaskBatchSize = "history.visibilityTaskBatchSize"
	// VisibilityProcessorMaxPollRPS is max poll rate per second for visibilityQueueProcessor
	VisibilityProcessorMaxPollRPS = "history.visibilityProcessorMaxPollRPS"
	// VisibilityProcessorMaxPollHostRPS is max poll rate per second for all visibilityQueueProcessor on a host
	VisibilityProcessorMaxPollHostRPS = "history.visibilityProcessorMaxPollHostRPS"
	// VisibilityTaskWorkerCount is number of worker for visibilityQueueProcessor
	VisibilityTaskWorkerCount = "history.visibilityTaskWorkerCount"
	// VisibilityTaskMaxRetryCount is max times of retry for visibilityQueueProcessor
	VisibilityTaskMaxRetryCount = "history.visibilityTaskMaxRetryCount"
	// VisibilityProcessorEnableMultiCursor indicates if the new single processor multi-cursor visibility processor implementation should be used
	// NOTE: this flag only takes effect when history.visibilityProcessorEnablePriorityTaskScheduler is enabled.
	VisibilityProcessorEnableMultiCursor = "history.visibilityProcessorEnableMultiCursor"
	// VisibilityProcessorEnablePriorityTaskScheduler indicates whether host level priority task scheduler should be used for visibilityQueueProcessor
	VisibilityProcessorEnablePriorityTaskScheduler = "history.visibilityProcessorEnablePriorityTaskScheduler"
	// VisibilityProcessorSchedulerWorkerCount is the number of workers in the host level task scheduler for visibilityQueueProcessor
	VisibilityProcessorSchedulerWorkerCount = "history.visibilityProcessorSchedulerWorkerCount"
	// VisibilityProcessorSchedulerActiveRoundRobinWeights is the priority round robin weights by visibility task scheduler for active namespaces
	VisibilityProcessorSchedulerActiveRoundRobinWeights = "history.visibilityProcessorSchedulerActiveRoundRobinWeights"
	// VisibilityProcessorSchedulerStandbyRoundRobinWeights is the priority round robin weights by visibility task scheduler for standby namespaces
	VisibilityProcessorSchedulerStandbyRoundRobinWeights = "history.visibilityProcessorSchedulerStandbyRoundRobinWeights"
	// VisibilityProcessorMaxPollInterval max poll interval for visibilityQueueProcessor
	VisibilityProcessorMaxPollInterval = "history.visibilityProcessorMaxPollInterval"
	// VisibilityProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	VisibilityProcessorMaxPollIntervalJitterCoefficient = "history.visibilityProcessorMaxPollIntervalJitterCoefficient"
	// VisibilityProcessorUpdateAckInterval is update interval for visibilityQueueProcessor
	VisibilityProcessorUpdateAckInterval = "history.visibilityProcessorUpdateAckInterval"
	// VisibilityProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	VisibilityProcessorUpdateAckIntervalJitterCoefficient = "history.visibilityProcessorUpdateAckIntervalJitterCoefficient"
	// VisibilityProcessorCompleteTaskInterval is complete timer interval for visibilityQueueProcessor
	VisibilityProcessorCompleteTaskInterval = "history.visibilityProcessorCompleteTaskInterval"
	// VisibilityProcessorMaxReschedulerSize is the threshold of the number of tasks in the redispatch queue for visibilityQueueProcessor
	VisibilityProcessorMaxReschedulerSize = "history.visibilityProcessorMaxReschedulerSize"
	// VisibilityProcessorPollBackoffInterval is the poll backoff interval if task redispatcher's size exceeds limit for visibilityQueueProcessor
	VisibilityProcessorPollBackoffInterval = "history.visibilityProcessorPollBackoffInterval"
	// VisibilityProcessorVisibilityArchivalTimeLimit is the upper time limit for archiving visibility records
	VisibilityProcessorVisibilityArchivalTimeLimit = "history.visibilityProcessorVisibilityArchivalTimeLimit"
	// VisibilityProcessorEnsureCloseBeforeDelete means we ensure the visibility of an execution is closed before we delete its visibility records
	VisibilityProcessorEnsureCloseBeforeDelete = "history.visibilityProcessorEnsureCloseBeforeDelete"
	// VisibilityProcessorEnableCloseWorkflowCleanup to clean up the mutable state after visibility
	// close task has been processed. Must use Elasticsearch as visibility store, otherwise workflow
	// data (eg: search attributes) will be lost after workflow is closed.
	VisibilityProcessorEnableCloseWorkflowCleanup = "history.visibilityProcessorEnableCloseWorkflowCleanup"

	// ArchivalTaskBatchSize is batch size for archivalQueueProcessor
	ArchivalTaskBatchSize = "history.archivalTaskBatchSize"
	// ArchivalProcessorMaxPollRPS is max poll rate per second for archivalQueueProcessor
	ArchivalProcessorMaxPollRPS = "history.archivalProcessorMaxPollRPS"
	// ArchivalProcessorMaxPollHostRPS is max poll rate per second for all archivalQueueProcessor on a host
	ArchivalProcessorMaxPollHostRPS = "history.archivalProcessorMaxPollHostRPS"
	// ArchivalTaskMaxRetryCount is max times of retry for archivalQueueProcessor
	ArchivalTaskMaxRetryCount = "history.archivalTaskMaxRetryCount"
	// ArchivalProcessorSchedulerWorkerCount is the number of workers in the host level task scheduler for
	// archivalQueueProcessor
	ArchivalProcessorSchedulerWorkerCount = "history.archivalProcessorSchedulerWorkerCount"
	// ArchivalProcessorSchedulerRoundRobinWeights is the priority round robin weights by archival task scheduler for
	// all namespaces
	ArchivalProcessorSchedulerRoundRobinWeights = "history.archivalProcessorSchedulerRoundRobinWeights"
	// ArchivalProcessorMaxPollInterval max poll interval for archivalQueueProcessor
	ArchivalProcessorMaxPollInterval = "history.archivalProcessorMaxPollInterval"
	// ArchivalProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	ArchivalProcessorMaxPollIntervalJitterCoefficient = "history.archivalProcessorMaxPollIntervalJitterCoefficient"
	// ArchivalProcessorUpdateAckInterval is update interval for archivalQueueProcessor
	ArchivalProcessorUpdateAckInterval = "history.archivalProcessorUpdateAckInterval"
	// ArchivalProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	ArchivalProcessorUpdateAckIntervalJitterCoefficient = "history.archivalProcessorUpdateAckIntervalJitterCoefficient"
	// ArchivalProcessorPollBackoffInterval is the poll backoff interval if task redispatcher's size exceeds limit for
	// archivalQueueProcessor
	ArchivalProcessorPollBackoffInterval = "history.archivalProcessorPollBackoffInterval"

	// ReplicatorTaskBatchSize is batch size for ReplicatorProcessor
	ReplicatorTaskBatchSize = "history.replicatorTaskBatchSize"
	// ReplicatorTaskWorkerCount is number of worker for ReplicatorProcessor
	ReplicatorTaskWorkerCount = "history.replicatorTaskWorkerCount"
	// ReplicatorTaskMaxRetryCount is max times of retry for ReplicatorProcessor
	ReplicatorTaskMaxRetryCount = "history.replicatorTaskMaxRetryCount"
	// ReplicatorProcessorMaxPollRPS is max poll rate per second for ReplicatorProcessor
	ReplicatorProcessorMaxPollRPS = "history.replicatorProcessorMaxPollRPS"
	// ReplicatorProcessorMaxPollInterval is max poll interval for ReplicatorProcessor
	ReplicatorProcessorMaxPollInterval = "history.replicatorProcessorMaxPollInterval"
	// ReplicatorProcessorMaxPollIntervalJitterCoefficient is the max poll interval jitter coefficient
	ReplicatorProcessorMaxPollIntervalJitterCoefficient = "history.replicatorProcessorMaxPollIntervalJitterCoefficient"
	// ReplicatorProcessorUpdateAckInterval is update interval for ReplicatorProcessor
	ReplicatorProcessorUpdateAckInterval = "history.replicatorProcessorUpdateAckInterval"
	// ReplicatorProcessorUpdateAckIntervalJitterCoefficient is the update interval jitter coefficient
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient = "history.replicatorProcessorUpdateAckIntervalJitterCoefficient"
	// ReplicatorProcessorMaxReschedulerSize is the threshold of the number of tasks in the redispatch queue for ReplicatorProcessor
	ReplicatorProcessorMaxReschedulerSize = "history.replicatorProcessorMaxReschedulerSize"
	// ReplicatorProcessorEnablePriorityTaskProcessor indicates whether priority task processor should be used for ReplicatorProcessor
	ReplicatorProcessorEnablePriorityTaskProcessor = "history.replicatorProcessorEnablePriorityTaskProcessor"
	// MaximumBufferedEventsBatch is max number of buffer event in mutable state
	MaximumBufferedEventsBatch = "history.maximumBufferedEventsBatch"
	// MaximumSignalsPerExecution is max number of signals supported by single execution
	MaximumSignalsPerExecution = "history.maximumSignalsPerExecution"
	// ShardUpdateMinInterval is the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval = "history.shardUpdateMinInterval"
	// ShardSyncMinInterval is the minimal time interval which the shard info should be sync to remote
	ShardSyncMinInterval = "history.shardSyncMinInterval"
	// EmitShardDiffLog whether emit the shard diff log
	EmitShardDiffLog = "history.emitShardDiffLog"
	// DefaultEventEncoding is the encoding type for history events
	DefaultEventEncoding = "history.defaultEventEncoding"
	// NumArchiveSystemWorkflows is key for number of archive system workflows running in total
	NumArchiveSystemWorkflows = "history.numArchiveSystemWorkflows"
	// ArchiveRequestRPS is the rate limit on the number of archive request per second
	ArchiveRequestRPS = "history.archiveRequestRPS"
	// ArchiveSignalTimeout is the signal timeout used when starting an archive system workflow
	ArchiveSignalTimeout = "history.archiveSignalTimeout"
	// DefaultActivityRetryPolicy represents the out-of-box retry policy for activities where
	// the user has not specified an explicit RetryPolicy
	DefaultActivityRetryPolicy = "history.defaultActivityRetryPolicy"
	// DefaultWorkflowRetryPolicy represents the out-of-box retry policy for unset fields
	// where the user has set an explicit RetryPolicy, but not specified all the fields
	DefaultWorkflowRetryPolicy = "history.defaultWorkflowRetryPolicy"
	// HistoryMaxAutoResetPoints is the key for max number of auto reset points stored in mutableState
	HistoryMaxAutoResetPoints = "history.historyMaxAutoResetPoints"
	// EnableParentClosePolicy whether to  ParentClosePolicy
	EnableParentClosePolicy = "history.enableParentClosePolicy"
	// ParentClosePolicyThreshold decides that parent close policy will be processed by sys workers(if enabled) if
	// the number of children greater than or equal to this threshold
	ParentClosePolicyThreshold = "history.parentClosePolicyThreshold"
	// NumParentClosePolicySystemWorkflows is key for number of parentClosePolicy system workflows running in total
	NumParentClosePolicySystemWorkflows = "history.numParentClosePolicySystemWorkflows"
	// HistoryThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	HistoryThrottledLogRPS = "history.throttledLogRPS"
	// StickyTTL is to expire a sticky taskqueue if no update more than this duration
	StickyTTL = "history.stickyTTL"
	// WorkflowTaskHeartbeatTimeout for workflow task heartbeat
	WorkflowTaskHeartbeatTimeout = "history.workflowTaskHeartbeatTimeout"
	// WorkflowTaskCriticalAttempts is the number of attempts for a workflow task that's regarded as critical
	WorkflowTaskCriticalAttempts = "history.workflowTaskCriticalAttempt"
	// WorkflowTaskRetryMaxInterval is the maximum interval added to a workflow task's startToClose timeout for slowing down retry
	WorkflowTaskRetryMaxInterval = "history.workflowTaskRetryMaxInterval"
	// DefaultWorkflowTaskTimeout for a workflow task
	DefaultWorkflowTaskTimeout = "history.defaultWorkflowTaskTimeout"
	// SkipReapplicationByNamespaceID is whether skipping a event re-application for a namespace
	SkipReapplicationByNamespaceID = "history.SkipReapplicationByNamespaceID"
	// StandbyTaskReReplicationContextTimeout is the context timeout for standby task re-replication
	StandbyTaskReReplicationContextTimeout = "history.standbyTaskReReplicationContextTimeout"
	// MaxBufferedQueryCount indicates max buffer query count
	MaxBufferedQueryCount = "history.MaxBufferedQueryCount"
	// MutableStateChecksumGenProbability is the probability [0-100] that checksum will be generated for mutable state
	MutableStateChecksumGenProbability = "history.mutableStateChecksumGenProbability"
	// MutableStateChecksumVerifyProbability is the probability [0-100] that checksum will be verified for mutable state
	MutableStateChecksumVerifyProbability = "history.mutableStateChecksumVerifyProbability"
	// MutableStateChecksumInvalidateBefore is the epoch timestamp before which all checksums are to be discarded
	MutableStateChecksumInvalidateBefore = "history.mutableStateChecksumInvalidateBefore"

	// ReplicationTaskFetcherParallelism determines how many go routines we spin up for fetching tasks
	ReplicationTaskFetcherParallelism = "history.ReplicationTaskFetcherParallelism"
	// ReplicationTaskFetcherAggregationInterval determines how frequently the fetch requests are sent
	ReplicationTaskFetcherAggregationInterval = "history.ReplicationTaskFetcherAggregationInterval"
	// ReplicationTaskFetcherTimerJitterCoefficient is the jitter for fetcher timer
	ReplicationTaskFetcherTimerJitterCoefficient = "history.ReplicationTaskFetcherTimerJitterCoefficient"
	// ReplicationTaskFetcherErrorRetryWait is the wait time when fetcher encounters error
	ReplicationTaskFetcherErrorRetryWait = "history.ReplicationTaskFetcherErrorRetryWait"
	// ReplicationTaskProcessorErrorRetryWait is the initial retry wait when we see errors in applying replication tasks
	ReplicationTaskProcessorErrorRetryWait = "history.ReplicationTaskProcessorErrorRetryWait"
	// ReplicationTaskProcessorErrorRetryBackoffCoefficient is the retry wait backoff time coefficient
	ReplicationTaskProcessorErrorRetryBackoffCoefficient = "history.ReplicationTaskProcessorErrorRetryBackoffCoefficient"
	// ReplicationTaskProcessorErrorRetryMaxInterval is the retry wait backoff max duration
	ReplicationTaskProcessorErrorRetryMaxInterval = "history.ReplicationTaskProcessorErrorRetryMaxInterval"
	// ReplicationTaskProcessorErrorRetryMaxAttempts is the max retry attempts for applying replication tasks
	ReplicationTaskProcessorErrorRetryMaxAttempts = "history.ReplicationTaskProcessorErrorRetryMaxAttempts"
	// ReplicationTaskProcessorErrorRetryExpiration is the max retry duration for applying replication tasks
	ReplicationTaskProcessorErrorRetryExpiration = "history.ReplicationTaskProcessorErrorRetryExpiration"
	// ReplicationTaskProcessorNoTaskInitialWait is the wait time when not ask is returned
	ReplicationTaskProcessorNoTaskInitialWait = "history.ReplicationTaskProcessorNoTaskInitialWait"
	// ReplicationTaskProcessorCleanupInterval determines how frequently the cleanup replication queue
	ReplicationTaskProcessorCleanupInterval = "history.ReplicationTaskProcessorCleanupInterval"
	// ReplicationTaskProcessorCleanupJitterCoefficient is the jitter for cleanup timer
	ReplicationTaskProcessorCleanupJitterCoefficient = "history.ReplicationTaskProcessorCleanupJitterCoefficient"
	// ReplicationTaskProcessorStartWait is the wait time before each task processing batch
	ReplicationTaskProcessorStartWait = "history.ReplicationTaskProcessorStartWait"
	// ReplicationTaskProcessorHostQPS is the qps of task processing rate limiter on host level
	ReplicationTaskProcessorHostQPS = "history.ReplicationTaskProcessorHostQPS"
	// ReplicationTaskProcessorShardQPS is the qps of task processing rate limiter on shard level
	ReplicationTaskProcessorShardQPS = "history.ReplicationTaskProcessorShardQPS"

	// keys for worker

	// WorkerPersistenceMaxQPS is the max qps worker host can query DB
	WorkerPersistenceMaxQPS = "worker.persistenceMaxQPS"
	// WorkerPersistenceGlobalMaxQPS is the max qps worker cluster can query DB
	WorkerPersistenceGlobalMaxQPS = "worker.persistenceGlobalMaxQPS"
	// WorkerPersistenceNamespaceMaxQPS is the max qps each namespace on worker host can query DB
	WorkerPersistenceNamespaceMaxQPS = "worker.persistenceNamespaceMaxQPS"
	// WorkerEnablePersistencePriorityRateLimiting indicates if priority rate limiting is enabled in worker persistence client
	WorkerEnablePersistencePriorityRateLimiting = "worker.enablePersistencePriorityRateLimiting"
	// WorkerIndexerConcurrency is the max concurrent messages to be processed at any given time
	WorkerIndexerConcurrency = "worker.indexerConcurrency"
	// WorkerESProcessorNumOfWorkers is num of workers for esProcessor
	WorkerESProcessorNumOfWorkers = "worker.ESProcessorNumOfWorkers"
	// WorkerESProcessorBulkActions is max number of requests in bulk for esProcessor
	WorkerESProcessorBulkActions = "worker.ESProcessorBulkActions"
	// WorkerESProcessorBulkSize is max total size of bulk in bytes for esProcessor
	WorkerESProcessorBulkSize = "worker.ESProcessorBulkSize"
	// WorkerESProcessorFlushInterval is flush interval for esProcessor
	WorkerESProcessorFlushInterval = "worker.ESProcessorFlushInterval"
	// WorkerESProcessorAckTimeout is the timeout that store will wait to get ack signal from ES processor.
	// Should be at least WorkerESProcessorFlushInterval+<time to process request>.
	WorkerESProcessorAckTimeout = "worker.ESProcessorAckTimeout"
	// WorkerArchiverMaxConcurrentActivityExecutionSize indicates worker archiver max concurrent activity execution size
	WorkerArchiverMaxConcurrentActivityExecutionSize = "worker.ArchiverMaxConcurrentActivityExecutionSize"
	// WorkerArchiverMaxConcurrentWorkflowTaskExecutionSize indicates worker archiver max concurrent workflow execution size
	WorkerArchiverMaxConcurrentWorkflowTaskExecutionSize = "worker.ArchiverMaxConcurrentWorkflowTaskExecutionSize"
	// WorkerArchiverMaxConcurrentActivityTaskPollers indicates worker archiver max concurrent activity pollers
	WorkerArchiverMaxConcurrentActivityTaskPollers = "worker.ArchiverMaxConcurrentActivityTaskPollers"
	// WorkerArchiverMaxConcurrentWorkflowTaskPollers indicates worker archiver max concurrent workflow pollers
	WorkerArchiverMaxConcurrentWorkflowTaskPollers = "worker.ArchiverMaxConcurrentWorkflowTaskPollers"
	// WorkerArchiverConcurrency controls the number of coroutines handling archival work per archival workflow
	WorkerArchiverConcurrency = "worker.ArchiverConcurrency"
	// WorkerArchivalsPerIteration controls the number of archivals handled in each iteration of archival workflow
	WorkerArchivalsPerIteration = "worker.ArchivalsPerIteration"
	// WorkerTimeLimitPerArchivalIteration controls the time limit of each iteration of archival workflow
	WorkerTimeLimitPerArchivalIteration = "worker.TimeLimitPerArchivalIteration"
	// WorkerThrottledLogRPS is the rate limit on number of log messages emitted per second for throttled logger
	WorkerThrottledLogRPS = "worker.throttledLogRPS"
	// WorkerScannerMaxConcurrentActivityExecutionSize indicates worker scanner max concurrent activity execution size
	WorkerScannerMaxConcurrentActivityExecutionSize = "worker.ScannerMaxConcurrentActivityExecutionSize"
	// WorkerScannerMaxConcurrentWorkflowTaskExecutionSize indicates worker scanner max concurrent workflow execution size
	WorkerScannerMaxConcurrentWorkflowTaskExecutionSize = "worker.ScannerMaxConcurrentWorkflowTaskExecutionSize"
	// WorkerScannerMaxConcurrentActivityTaskPollers indicates worker scanner max concurrent activity pollers
	WorkerScannerMaxConcurrentActivityTaskPollers = "worker.ScannerMaxConcurrentActivityTaskPollers"
	// WorkerScannerMaxConcurrentWorkflowTaskPollers indicates worker scanner max concurrent workflow pollers
	WorkerScannerMaxConcurrentWorkflowTaskPollers = "worker.ScannerMaxConcurrentWorkflowTaskPollers"
	// ScannerPersistenceMaxQPS is the maximum rate of persistence calls from worker.Scanner
	ScannerPersistenceMaxQPS = "worker.scannerPersistenceMaxQPS"
	// ExecutionScannerPerHostQPS is the maximum rate of calls per host from executions.Scanner
	ExecutionScannerPerHostQPS = "worker.executionScannerPerHostQPS"
	// ExecutionScannerPerShardQPS is the maximum rate of calls per shard from executions.Scanner
	ExecutionScannerPerShardQPS = "worker.executionScannerPerShardQPS"
	// ExecutionDataDurationBuffer is the data TTL duration buffer of execution data
	ExecutionDataDurationBuffer = "worker.executionDataDurationBuffer"
	// ExecutionScannerWorkerCount is the execution scavenger worker count
	ExecutionScannerWorkerCount = "worker.executionScannerWorkerCount"
	// TaskQueueScannerEnabled indicates if task queue scanner should be started as part of worker.Scanner
	TaskQueueScannerEnabled = "worker.taskQueueScannerEnabled"
	// HistoryScannerEnabled indicates if history scanner should be started as part of worker.Scanner
	HistoryScannerEnabled = "worker.historyScannerEnabled"
	// ExecutionsScannerEnabled indicates if executions scanner should be started as part of worker.Scanner
	ExecutionsScannerEnabled = "worker.executionsScannerEnabled"
	// HistoryScannerDataMinAge indicates the history scanner cleanup minimum age.
	HistoryScannerDataMinAge = "worker.historyScannerDataMinAge"
	// HistoryScannerVerifyRetention indicates the history scanner verify data retention.
	// If the service configures with archival feature enabled, update worker.historyScannerVerifyRetention to be double of the data retention.
	HistoryScannerVerifyRetention = "worker.historyScannerVerifyRetention"
	// EnableBatcher decides whether start batcher in our worker
	EnableBatcher = "worker.enableBatcher"
	// BatcherRPS controls number the rps of batch operations
	BatcherRPS = "worker.batcherRPS"
	// BatcherConcurrency controls the concurrency of one batch operation
	BatcherConcurrency = "worker.batcherConcurrency"
	// WorkerParentCloseMaxConcurrentActivityExecutionSize indicates worker parent close worker max concurrent activity execution size
	WorkerParentCloseMaxConcurrentActivityExecutionSize = "worker.ParentCloseMaxConcurrentActivityExecutionSize"
	// WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize indicates worker parent close worker max concurrent workflow execution size
	WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize = "worker.ParentCloseMaxConcurrentWorkflowTaskExecutionSize"
	// WorkerParentCloseMaxConcurrentActivityTaskPollers indicates worker parent close worker max concurrent activity pollers
	WorkerParentCloseMaxConcurrentActivityTaskPollers = "worker.ParentCloseMaxConcurrentActivityTaskPollers"
	// WorkerParentCloseMaxConcurrentWorkflowTaskPollers indicates worker parent close worker max concurrent workflow pollers
	WorkerParentCloseMaxConcurrentWorkflowTaskPollers = "worker.ParentCloseMaxConcurrentWorkflowTaskPollers"
	// WorkerPerNamespaceWorkerCount controls number of per-ns (scheduler, batcher, etc.) workers to run per namespace
	WorkerPerNamespaceWorkerCount = "worker.perNamespaceWorkerCount"
	// WorkerEnableScheduler controls whether to start the worker for scheduled workflows
	WorkerEnableScheduler = "worker.enableScheduler"
)
