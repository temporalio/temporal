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

package configs

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/retrypolicy"
	schedulerhsm "go.temporal.io/server/components/scheduler"
)

// Config represents configuration for history service
type Config struct {
	NumberOfShards int32

	EnableReplicationStream dynamicconfig.BoolPropertyFn
	HistoryReplicationDLQV2 dynamicconfig.BoolPropertyFn

	RPS                                  dynamicconfig.IntPropertyFn
	OperatorRPSRatio                     dynamicconfig.FloatPropertyFn
	MaxIDLengthLimit                     dynamicconfig.IntPropertyFn
	PersistenceMaxQPS                    dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS              dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQPS           dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistenceGlobalNamespaceMaxQPS     dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistencePerShardNamespaceMaxQPS   dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistenceDynamicRateLimitingParams dynamicconfig.TypedPropertyFn[dynamicconfig.DynamicRateLimitingParams]
	PersistenceQPSBurstRatio             dynamicconfig.FloatPropertyFn

	VisibilityPersistenceMaxReadQPS       dynamicconfig.IntPropertyFn
	VisibilityPersistenceMaxWriteQPS      dynamicconfig.IntPropertyFn
	EnableReadFromSecondaryVisibility     dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityEnableShadowReadMode        dynamicconfig.BoolPropertyFn
	SecondaryVisibilityWritingMode        dynamicconfig.StringPropertyFn
	VisibilityDisableOrderByClause        dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityEnableManualPagination      dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityAllowList                   dynamicconfig.BoolPropertyFnWithNamespaceFilter
	SuppressErrorSetSystemSearchAttribute dynamicconfig.BoolPropertyFnWithNamespaceFilter

	EmitShardLagLog            dynamicconfig.BoolPropertyFn
	MaxAutoResetPoints         dynamicconfig.IntPropertyFnWithNamespaceFilter
	ThrottledLogRPS            dynamicconfig.IntPropertyFn
	EnableStickyQuery          dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration      dynamicconfig.DurationPropertyFn
	StartupMembershipJoinDelay dynamicconfig.DurationPropertyFn

	// HistoryCache settings
	// Change of these configs require shard restart
	HistoryCacheLimitSizeBased            bool
	HistoryCacheInitialSize               dynamicconfig.IntPropertyFn
	HistoryShardLevelCacheMaxSize         dynamicconfig.IntPropertyFn
	HistoryShardLevelCacheMaxSizeBytes    dynamicconfig.IntPropertyFn
	HistoryHostLevelCacheMaxSize          dynamicconfig.IntPropertyFn
	HistoryHostLevelCacheMaxSizeBytes     dynamicconfig.IntPropertyFn
	HistoryCacheTTL                       dynamicconfig.DurationPropertyFn
	HistoryCacheNonUserContextLockTimeout dynamicconfig.DurationPropertyFn
	EnableHostLevelHistoryCache           dynamicconfig.BoolPropertyFn
	EnableNexus                           dynamicconfig.BoolPropertyFn
	EnableWorkflowExecutionTimeoutTimer   dynamicconfig.BoolPropertyFn
	EnableTransitionHistory               dynamicconfig.BoolPropertyFn

	// EventsCache settings
	// Change of these configs require shard restart
	EventsShardLevelCacheMaxSizeBytes dynamicconfig.IntPropertyFn
	EventsCacheTTL                    dynamicconfig.DurationPropertyFn
	// Change of these configs require service restart
	EnableHostLevelEventsCache       dynamicconfig.BoolPropertyFn
	EventsHostLevelCacheMaxSizeBytes dynamicconfig.IntPropertyFn

	// ShardController settings
	RangeSizeBits                uint
	AcquireShardInterval         dynamicconfig.DurationPropertyFn
	AcquireShardConcurrency      dynamicconfig.IntPropertyFn
	ShardIOConcurrency           dynamicconfig.IntPropertyFn
	ShardIOTimeout               dynamicconfig.DurationPropertyFn
	ShardLingerOwnershipCheckQPS dynamicconfig.IntPropertyFn
	ShardLingerTimeLimit         dynamicconfig.DurationPropertyFn
	ShardFinalizerTimeout        dynamicconfig.DurationPropertyFn

	HistoryClientOwnershipCachingEnabled dynamicconfig.BoolPropertyFn

	// the artificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay                  dynamicconfig.DurationPropertyFn
	StandbyTaskMissingEventsResendDelay  dynamicconfig.DurationPropertyFnWithTaskTypeFilter
	StandbyTaskMissingEventsDiscardDelay dynamicconfig.DurationPropertyFnWithTaskTypeFilter

	QueuePendingTaskCriticalCount    dynamicconfig.IntPropertyFn
	QueueReaderStuckCriticalAttempts dynamicconfig.IntPropertyFn
	QueueCriticalSlicesCount         dynamicconfig.IntPropertyFn
	QueuePendingTaskMaxCount         dynamicconfig.IntPropertyFn
	QueueMaxPredicateSize            dynamicconfig.IntPropertyFn

	TaskDLQEnabled                 dynamicconfig.BoolPropertyFn
	TaskDLQUnexpectedErrorAttempts dynamicconfig.IntPropertyFn
	TaskDLQErrorPattern            dynamicconfig.StringPropertyFn

	TaskSchedulerEnableRateLimiter           dynamicconfig.BoolPropertyFn
	TaskSchedulerEnableRateLimiterShadowMode dynamicconfig.BoolPropertyFn
	TaskSchedulerRateLimiterStartupDelay     dynamicconfig.DurationPropertyFn
	TaskSchedulerGlobalMaxQPS                dynamicconfig.IntPropertyFn
	TaskSchedulerMaxQPS                      dynamicconfig.IntPropertyFn
	TaskSchedulerGlobalNamespaceMaxQPS       dynamicconfig.IntPropertyFnWithNamespaceFilter
	TaskSchedulerNamespaceMaxQPS             dynamicconfig.IntPropertyFnWithNamespaceFilter

	// TimerQueueProcessor settings
	TimerTaskBatchSize                               dynamicconfig.IntPropertyFn
	TimerProcessorSchedulerWorkerCount               dynamicconfig.TypedSubscribable[int]
	TimerProcessorSchedulerActiveRoundRobinWeights   dynamicconfig.MapPropertyFnWithNamespaceFilter
	TimerProcessorSchedulerStandbyRoundRobinWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
	TimerProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	TimerProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TimerProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	TimerProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	TimerProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	TimerProcessorMaxTimeShift                       dynamicconfig.DurationPropertyFn
	TimerQueueMaxReaderCount                         dynamicconfig.IntPropertyFn
	RetentionTimerJitterDuration                     dynamicconfig.DurationPropertyFn

	MemoryTimerProcessorSchedulerWorkerCount dynamicconfig.TypedSubscribable[int]

	// TransferQueueProcessor settings
	TransferTaskBatchSize                               dynamicconfig.IntPropertyFn
	TransferProcessorSchedulerWorkerCount               dynamicconfig.TypedSubscribable[int]
	TransferProcessorSchedulerActiveRoundRobinWeights   dynamicconfig.MapPropertyFnWithNamespaceFilter
	TransferProcessorSchedulerStandbyRoundRobinWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
	TransferProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	TransferProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	TransferProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	TransferProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TransferProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	TransferProcessorEnsureCloseBeforeDelete            dynamicconfig.BoolPropertyFn
	TransferQueueMaxReaderCount                         dynamicconfig.IntPropertyFn

	// OutboundQueueProcessor settings
	OutboundTaskBatchSize                               dynamicconfig.IntPropertyFn
	OutboundProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	OutboundProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	OutboundProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	OutboundProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	OutboundProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	OutboundProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	OutboundProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	OutboundQueuePendingTaskCriticalCount               dynamicconfig.IntPropertyFn
	OutboundQueuePendingTaskMaxCount                    dynamicconfig.IntPropertyFn
	OutboundQueueMaxPredicateSize                       dynamicconfig.IntPropertyFn
	OutboundQueueMaxReaderCount                         dynamicconfig.IntPropertyFn
	OutboundQueueGroupLimiterBufferSize                 dynamicconfig.IntPropertyFnWithDestinationFilter
	OutboundQueueGroupLimiterConcurrency                dynamicconfig.IntPropertyFnWithDestinationFilter
	OutboundQueueHostSchedulerMaxTaskRPS                dynamicconfig.FloatPropertyFnWithDestinationFilter
	OutboundQueueCircuitBreakerSettings                 dynamicconfig.TypedSubscribableWithDestinationFilter[dynamicconfig.CircuitBreakerSettings]
	OutboundStandbyTaskMissingEventsDiscardDelay        dynamicconfig.DurationPropertyFnWithDestinationFilter
	OutboundStandbyTaskMissingEventsDestinationDownErr  dynamicconfig.BoolPropertyFnWithDestinationFilter

	// ReplicatorQueueProcessor settings
	ReplicatorProcessorMaxPollInterval                  dynamicconfig.DurationPropertyFn
	ReplicatorProcessorMaxPollIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	ReplicatorProcessorFetchTasksBatchSize              dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxSkipTaskCount                 dynamicconfig.IntPropertyFn

	// System Limits
	MaximumBufferedEventsBatch       dynamicconfig.IntPropertyFn
	MaximumBufferedEventsSizeInBytes dynamicconfig.IntPropertyFn
	MaximumSignalsPerExecution       dynamicconfig.IntPropertyFnWithNamespaceFilter

	// ShardUpdateMinInterval is the minimum time interval within which the shard info can be updated.
	ShardUpdateMinInterval dynamicconfig.DurationPropertyFn

	// ShardFirstUpdateMinInterval defines how soon _first_ hard update should happen.
	ShardFirstUpdateInterval dynamicconfig.DurationPropertyFn

	// ShardUpdateMinTasksCompleted is the minimum number of tasks which must be completed before the shard info can be updated before
	// history.shardUpdateMinInterval has passed
	ShardUpdateMinTasksCompleted dynamicconfig.IntPropertyFn
	// ShardSyncMinInterval is the minimum time interval within which the shard info can be synced to the remote.
	ShardSyncMinInterval            dynamicconfig.DurationPropertyFn
	ShardSyncTimerJitterCoefficient dynamicconfig.FloatPropertyFn

	// Time to hold a poll request before returning an empty response
	// right now only used by GetMutableState
	LongPollExpirationInterval dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// encoding the history events
	EventEncodingType dynamicconfig.StringPropertyFnWithNamespaceFilter
	// whether or not using ParentClosePolicy
	EnableParentClosePolicy dynamicconfig.BoolPropertyFnWithNamespaceFilter
	// whether or not enable system workers for processing parent close policy task
	EnableParentClosePolicyWorker dynamicconfig.BoolPropertyFn
	// parent close policy will be processed by sys workers(if enabled) if
	// the number of children greater than or equal to this threshold
	ParentClosePolicyThreshold dynamicconfig.IntPropertyFnWithNamespaceFilter
	// total number of parentClosePolicy system workflows
	NumParentClosePolicySystemWorkflows dynamicconfig.IntPropertyFn

	// Size limit related settings
	BlobSizeLimitError                        dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	MemoSizeLimitError                        dynamicconfig.IntPropertyFnWithNamespaceFilter
	MemoSizeLimitWarn                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeLimitError                     dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeLimitWarn                      dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeSuggestContinueAsNew           dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountLimitError                    dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountLimitWarn                     dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountSuggestContinueAsNew          dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryMaxPageSize                        dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateActivityFailureSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateActivityFailureSizeLimitWarn  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateSizeLimitError                dynamicconfig.IntPropertyFn
	MutableStateSizeLimitWarn                 dynamicconfig.IntPropertyFn
	MutableStateTombstoneCountLimit           dynamicconfig.IntPropertyFn
	NumPendingChildExecutionsLimit            dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingActivitiesLimit                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingSignalsLimit                    dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingCancelsRequestLimit             dynamicconfig.IntPropertyFnWithNamespaceFilter

	// DefaultActivityRetryOptions specifies the out-of-box retry policy if
	// none is configured on the Activity by the user.
	DefaultActivityRetryPolicy dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]

	// DefaultWorkflowRetryPolicy specifies the out-of-box retry policy for
	// any unset fields on a RetryPolicy configured on a Workflow
	DefaultWorkflowRetryPolicy dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]

	// Workflow task settings
	// DefaultWorkflowTaskTimeout the default workflow task timeout
	DefaultWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// WorkflowTaskHeartbeatTimeout is to timeout behavior of: RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true
	// without any commands or messages. After this timeout workflow task will be scheduled to another worker(by clear stickyness).
	WorkflowTaskHeartbeatTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	WorkflowTaskCriticalAttempts dynamicconfig.IntPropertyFn
	WorkflowTaskRetryMaxInterval dynamicconfig.DurationPropertyFn

	// The following is used by the new RPC replication stack
	ReplicationTaskApplyTimeout                          dynamicconfig.DurationPropertyFn
	ReplicationTaskFetcherParallelism                    dynamicconfig.IntPropertyFn
	ReplicationTaskFetcherAggregationInterval            dynamicconfig.DurationPropertyFn
	ReplicationTaskFetcherTimerJitterCoefficient         dynamicconfig.FloatPropertyFn
	ReplicationTaskFetcherErrorRetryWait                 dynamicconfig.DurationPropertyFn
	ReplicationTaskProcessorErrorRetryWait               dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorErrorRetryBackoffCoefficient dynamicconfig.FloatPropertyFnWithShardIDFilter
	ReplicationTaskProcessorErrorRetryMaxInterval        dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorErrorRetryMaxAttempts        dynamicconfig.IntPropertyFnWithShardIDFilter
	ReplicationTaskProcessorErrorRetryExpiration         dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorNoTaskRetryWait              dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorCleanupInterval              dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorCleanupJitterCoefficient     dynamicconfig.FloatPropertyFnWithShardIDFilter
	ReplicationTaskProcessorHostQPS                      dynamicconfig.FloatPropertyFn
	ReplicationTaskProcessorShardQPS                     dynamicconfig.FloatPropertyFn
	ReplicationEnableDLQMetrics                          dynamicconfig.BoolPropertyFn
	ReplicationEnableUpdateWithNewTaskMerge              dynamicconfig.BoolPropertyFn
	ReplicationMultipleBatches                           dynamicconfig.BoolPropertyFn

	ReplicationStreamSyncStatusDuration                 dynamicconfig.DurationPropertyFn
	ReplicationProcessorSchedulerQueueSize              dynamicconfig.IntPropertyFn
	ReplicationProcessorSchedulerWorkerCount            dynamicconfig.TypedSubscribable[int]
	ReplicationLowPriorityProcessorSchedulerWorkerCount dynamicconfig.TypedSubscribable[int]
	ReplicationLowPriorityTaskParallelism               dynamicconfig.IntPropertyFn
	EnableReplicationEagerRefreshNamespace              dynamicconfig.BoolPropertyFn
	EnableReplicationTaskBatching                       dynamicconfig.BoolPropertyFn
	EnableReplicateLocalGeneratedEvent                  dynamicconfig.BoolPropertyFn
	EnableReplicationTaskTieredProcessing               dynamicconfig.BoolPropertyFn
	ReplicationStreamSenderHighPriorityQPS              dynamicconfig.IntPropertyFn
	ReplicationStreamSenderLowPriorityQPS               dynamicconfig.IntPropertyFn
	ReplicationReceiverMaxOutstandingTaskCount          dynamicconfig.IntPropertyFn
	ReplicationResendMaxBatchCount                      dynamicconfig.IntPropertyFn
	ReplicationProgressCacheMaxSize                     dynamicconfig.IntPropertyFn
	ReplicationProgressCacheTTL                         dynamicconfig.DurationPropertyFn

	// The following are used by consistent query
	MaxBufferedQueryCount dynamicconfig.IntPropertyFn

	// Data integrity check related config knobs
	MutableStateChecksumGenProbability    dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateChecksumVerifyProbability dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateChecksumInvalidateBefore  dynamicconfig.FloatPropertyFn

	// NDC Replication configuration
	StandbyTaskReReplicationContextTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter

	SkipReapplicationByNamespaceID dynamicconfig.BoolPropertyFnWithNamespaceIDFilter

	// ===== Visibility related =====
	// VisibilityQueueProcessor settings
	VisibilityTaskBatchSize                               dynamicconfig.IntPropertyFn
	VisibilityProcessorSchedulerWorkerCount               dynamicconfig.TypedSubscribable[int]
	VisibilityProcessorSchedulerActiveRoundRobinWeights   dynamicconfig.MapPropertyFnWithNamespaceFilter
	VisibilityProcessorSchedulerStandbyRoundRobinWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
	VisibilityProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	VisibilityProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	VisibilityProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	VisibilityProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	VisibilityProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	VisibilityProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	VisibilityProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	VisibilityProcessorEnsureCloseBeforeDelete            dynamicconfig.BoolPropertyFn
	VisibilityProcessorEnableCloseWorkflowCleanup         dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityProcessorRelocateAttributesMinBlobSize      dynamicconfig.IntPropertyFnWithNamespaceFilter
	VisibilityQueueMaxReaderCount                         dynamicconfig.IntPropertyFn

	// Disable fetching memo and search attributes from visibility in the event that they were removed
	// from the mutable state in the close execution visibility task clean up.
	DisableFetchRelocatableAttributesFromVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter

	SearchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	SearchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
	SearchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter
	IndexerConcurrency                dynamicconfig.IntPropertyFn
	ESProcessorNumOfWorkers           dynamicconfig.IntPropertyFn
	ESProcessorBulkActions            dynamicconfig.IntPropertyFn // max number of requests in bulk
	ESProcessorBulkSize               dynamicconfig.IntPropertyFn // max total size of bytes in bulk
	ESProcessorFlushInterval          dynamicconfig.DurationPropertyFn
	ESProcessorAckTimeout             dynamicconfig.DurationPropertyFn

	EnableCrossNamespaceCommands  dynamicconfig.BoolPropertyFn
	EnableActivityEagerExecution  dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableEagerWorkflowStart      dynamicconfig.BoolPropertyFnWithNamespaceFilter
	NamespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn

	// ArchivalQueueProcessor settings
	ArchivalProcessorSchedulerWorkerCount               dynamicconfig.TypedSubscribable[int]
	ArchivalProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	ArchivalTaskBatchSize                               dynamicconfig.IntPropertyFn
	ArchivalProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	ArchivalProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	ArchivalProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	ArchivalProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	ArchivalProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	ArchivalProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	ArchivalProcessorArchiveDelay                       dynamicconfig.DurationPropertyFn
	ArchivalBackendMaxRPS                               dynamicconfig.FloatPropertyFn
	ArchivalQueueMaxReaderCount                         dynamicconfig.IntPropertyFn

	WorkflowExecutionMaxInFlightUpdates dynamicconfig.IntPropertyFnWithNamespaceFilter
	WorkflowExecutionMaxTotalUpdates    dynamicconfig.IntPropertyFnWithNamespaceFilter

	SendRawWorkflowHistory dynamicconfig.BoolPropertyFnWithNamespaceFilter

	WorkflowIdReuseMinimalInterval           dynamicconfig.DurationPropertyFnWithNamespaceFilter
	EnableWorkflowIdReuseStartTimeValidation dynamicconfig.BoolPropertyFnWithNamespaceFilter

	UseExperimentalHsmScheduler dynamicconfig.BoolPropertyFnWithNamespaceFilter
	HsmSchedulerTweakables      dynamicconfig.TypedPropertyFnWithNamespaceFilter[schedulerhsm.Tweakables]

	HealthPersistenceLatencyFailure dynamicconfig.FloatPropertyFn
	HealthPersistenceErrorRatio     dynamicconfig.FloatPropertyFn

	BreakdownMetricsByTaskQueue dynamicconfig.BoolPropertyFnWithTaskQueueFilter

	LogAllReqErrors dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// NewConfig returns new service config with default values
func NewConfig(
	dc *dynamicconfig.Collection,
	numberOfShards int32,
) *Config {
	cfg := &Config{
		NumberOfShards: numberOfShards,

		EnableReplicationStream: dynamicconfig.EnableReplicationStream.Get(dc),
		HistoryReplicationDLQV2: dynamicconfig.EnableHistoryReplicationDLQV2.Get(dc),

		RPS:                                  dynamicconfig.HistoryRPS.Get(dc),
		OperatorRPSRatio:                     dynamicconfig.OperatorRPSRatio.Get(dc),
		MaxIDLengthLimit:                     dynamicconfig.MaxIDLengthLimit.Get(dc),
		PersistenceMaxQPS:                    dynamicconfig.HistoryPersistenceMaxQPS.Get(dc),
		PersistenceGlobalMaxQPS:              dynamicconfig.HistoryPersistenceGlobalMaxQPS.Get(dc),
		PersistenceNamespaceMaxQPS:           dynamicconfig.HistoryPersistenceNamespaceMaxQPS.Get(dc),
		PersistenceGlobalNamespaceMaxQPS:     dynamicconfig.HistoryPersistenceGlobalNamespaceMaxQPS.Get(dc),
		PersistencePerShardNamespaceMaxQPS:   dynamicconfig.HistoryPersistencePerShardNamespaceMaxQPS.Get(dc),
		PersistenceDynamicRateLimitingParams: dynamicconfig.HistoryPersistenceDynamicRateLimitingParams.Get(dc),
		PersistenceQPSBurstRatio:             dynamicconfig.PersistenceQPSBurstRatio.Get(dc),
		ShutdownDrainDuration:                dynamicconfig.HistoryShutdownDrainDuration.Get(dc),
		StartupMembershipJoinDelay:           dynamicconfig.HistoryStartupMembershipJoinDelay.Get(dc),
		MaxAutoResetPoints:                   dynamicconfig.HistoryMaxAutoResetPoints.Get(dc),
		DefaultWorkflowTaskTimeout:           dynamicconfig.DefaultWorkflowTaskTimeout.Get(dc),

		VisibilityPersistenceMaxReadQPS:       dynamicconfig.VisibilityPersistenceMaxReadQPS.Get(dc),
		VisibilityPersistenceMaxWriteQPS:      dynamicconfig.VisibilityPersistenceMaxWriteQPS.Get(dc),
		EnableReadFromSecondaryVisibility:     dynamicconfig.EnableReadFromSecondaryVisibility.Get(dc),
		VisibilityEnableShadowReadMode:        dynamicconfig.VisibilityEnableShadowReadMode.Get(dc),
		SecondaryVisibilityWritingMode:        dynamicconfig.SecondaryVisibilityWritingMode.Get(dc),
		VisibilityDisableOrderByClause:        dynamicconfig.VisibilityDisableOrderByClause.Get(dc),
		VisibilityEnableManualPagination:      dynamicconfig.VisibilityEnableManualPagination.Get(dc),
		VisibilityAllowList:                   dynamicconfig.VisibilityAllowList.Get(dc),
		SuppressErrorSetSystemSearchAttribute: dynamicconfig.SuppressErrorSetSystemSearchAttribute.Get(dc),

		EmitShardLagLog: dynamicconfig.EmitShardLagLog.Get(dc),
		// HistoryCacheLimitSizeBased should not change during runtime.
		HistoryCacheLimitSizeBased:            dynamicconfig.HistoryCacheSizeBasedLimit.Get(dc)(),
		HistoryCacheInitialSize:               dynamicconfig.HistoryCacheInitialSize.Get(dc),
		HistoryShardLevelCacheMaxSize:         dynamicconfig.HistoryCacheMaxSize.Get(dc),
		HistoryShardLevelCacheMaxSizeBytes:    dynamicconfig.HistoryCacheMaxSizeBytes.Get(dc),
		HistoryHostLevelCacheMaxSize:          dynamicconfig.HistoryCacheHostLevelMaxSize.Get(dc),
		HistoryHostLevelCacheMaxSizeBytes:     dynamicconfig.HistoryCacheHostLevelMaxSizeBytes.Get(dc),
		HistoryCacheTTL:                       dynamicconfig.HistoryCacheTTL.Get(dc),
		HistoryCacheNonUserContextLockTimeout: dynamicconfig.HistoryCacheNonUserContextLockTimeout.Get(dc),
		EnableHostLevelHistoryCache:           dynamicconfig.EnableHostHistoryCache.Get(dc),
		EnableNexus:                           dynamicconfig.EnableNexus.Get(dc),
		EnableWorkflowExecutionTimeoutTimer:   dynamicconfig.EnableWorkflowExecutionTimeoutTimer.Get(dc),
		EnableTransitionHistory:               dynamicconfig.EnableTransitionHistory.Get(dc),

		EventsShardLevelCacheMaxSizeBytes: dynamicconfig.EventsCacheMaxSizeBytes.Get(dc),          // 512KB
		EventsHostLevelCacheMaxSizeBytes:  dynamicconfig.EventsHostLevelCacheMaxSizeBytes.Get(dc), // 256MB
		EventsCacheTTL:                    dynamicconfig.EventsCacheTTL.Get(dc),
		EnableHostLevelEventsCache:        dynamicconfig.EnableHostLevelEventsCache.Get(dc),

		RangeSizeBits: 20, // 20 bits for sequencer, 2^20 sequence number for any range

		AcquireShardInterval:         dynamicconfig.AcquireShardInterval.Get(dc),
		AcquireShardConcurrency:      dynamicconfig.AcquireShardConcurrency.Get(dc),
		ShardIOConcurrency:           dynamicconfig.ShardIOConcurrency.Get(dc),
		ShardIOTimeout:               dynamicconfig.ShardIOTimeout.Get(dc),
		ShardLingerOwnershipCheckQPS: dynamicconfig.ShardLingerOwnershipCheckQPS.Get(dc),
		ShardLingerTimeLimit:         dynamicconfig.ShardLingerTimeLimit.Get(dc),
		ShardFinalizerTimeout:        dynamicconfig.ShardFinalizerTimeout.Get(dc),

		HistoryClientOwnershipCachingEnabled: dynamicconfig.HistoryClientOwnershipCachingEnabled.Get(dc),

		StandbyClusterDelay:                  dynamicconfig.StandbyClusterDelay.Get(dc),
		StandbyTaskMissingEventsResendDelay:  dynamicconfig.StandbyTaskMissingEventsResendDelay.Get(dc),
		StandbyTaskMissingEventsDiscardDelay: dynamicconfig.StandbyTaskMissingEventsDiscardDelay.Get(dc),

		QueuePendingTaskCriticalCount:    dynamicconfig.QueuePendingTaskCriticalCount.Get(dc),
		QueueReaderStuckCriticalAttempts: dynamicconfig.QueueReaderStuckCriticalAttempts.Get(dc),
		QueueCriticalSlicesCount:         dynamicconfig.QueueCriticalSlicesCount.Get(dc),
		QueuePendingTaskMaxCount:         dynamicconfig.QueuePendingTaskMaxCount.Get(dc),
		QueueMaxPredicateSize:            dynamicconfig.QueueMaxPredicateSize.Get(dc),

		TaskDLQEnabled:                 dynamicconfig.HistoryTaskDLQEnabled.Get(dc),
		TaskDLQUnexpectedErrorAttempts: dynamicconfig.HistoryTaskDLQUnexpectedErrorAttempts.Get(dc),
		TaskDLQErrorPattern:            dynamicconfig.HistoryTaskDLQErrorPattern.Get(dc),

		TaskSchedulerEnableRateLimiter:           dynamicconfig.TaskSchedulerEnableRateLimiter.Get(dc),
		TaskSchedulerEnableRateLimiterShadowMode: dynamicconfig.TaskSchedulerEnableRateLimiterShadowMode.Get(dc),
		TaskSchedulerRateLimiterStartupDelay:     dynamicconfig.TaskSchedulerRateLimiterStartupDelay.Get(dc),
		TaskSchedulerGlobalMaxQPS:                dynamicconfig.TaskSchedulerGlobalMaxQPS.Get(dc),
		TaskSchedulerMaxQPS:                      dynamicconfig.TaskSchedulerMaxQPS.Get(dc),
		TaskSchedulerNamespaceMaxQPS:             dynamicconfig.TaskSchedulerNamespaceMaxQPS.Get(dc),
		TaskSchedulerGlobalNamespaceMaxQPS:       dynamicconfig.TaskSchedulerGlobalNamespaceMaxQPS.Get(dc),

		TimerTaskBatchSize:                               dynamicconfig.TimerTaskBatchSize.Get(dc),
		TimerProcessorSchedulerWorkerCount:               dynamicconfig.TimerProcessorSchedulerWorkerCount.Subscribe(dc),
		TimerProcessorSchedulerActiveRoundRobinWeights:   dynamicconfig.TimerProcessorSchedulerActiveRoundRobinWeights.WithDefault(ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)).Get(dc),
		TimerProcessorSchedulerStandbyRoundRobinWeights:  dynamicconfig.TimerProcessorSchedulerStandbyRoundRobinWeights.WithDefault(ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)).Get(dc),
		TimerProcessorUpdateAckInterval:                  dynamicconfig.TimerProcessorUpdateAckInterval.Get(dc),
		TimerProcessorUpdateAckIntervalJitterCoefficient: dynamicconfig.TimerProcessorUpdateAckIntervalJitterCoefficient.Get(dc),
		TimerProcessorMaxPollRPS:                         dynamicconfig.TimerProcessorMaxPollRPS.Get(dc),
		TimerProcessorMaxPollHostRPS:                     dynamicconfig.TimerProcessorMaxPollHostRPS.Get(dc),
		TimerProcessorMaxPollInterval:                    dynamicconfig.TimerProcessorMaxPollInterval.Get(dc),
		TimerProcessorMaxPollIntervalJitterCoefficient:   dynamicconfig.TimerProcessorMaxPollIntervalJitterCoefficient.Get(dc),
		TimerProcessorPollBackoffInterval:                dynamicconfig.TimerProcessorPollBackoffInterval.Get(dc),
		TimerProcessorMaxTimeShift:                       dynamicconfig.TimerProcessorMaxTimeShift.Get(dc),
		TransferQueueMaxReaderCount:                      dynamicconfig.TransferQueueMaxReaderCount.Get(dc),
		RetentionTimerJitterDuration:                     dynamicconfig.RetentionTimerJitterDuration.Get(dc),

		MemoryTimerProcessorSchedulerWorkerCount: dynamicconfig.MemoryTimerProcessorSchedulerWorkerCount.Subscribe(dc),

		TransferTaskBatchSize:                               dynamicconfig.TransferTaskBatchSize.Get(dc),
		TransferProcessorSchedulerWorkerCount:               dynamicconfig.TransferProcessorSchedulerWorkerCount.Subscribe(dc),
		TransferProcessorSchedulerActiveRoundRobinWeights:   dynamicconfig.TransferProcessorSchedulerActiveRoundRobinWeights.WithDefault(ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)).Get(dc),
		TransferProcessorSchedulerStandbyRoundRobinWeights:  dynamicconfig.TransferProcessorSchedulerStandbyRoundRobinWeights.WithDefault(ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)).Get(dc),
		TransferProcessorMaxPollRPS:                         dynamicconfig.TransferProcessorMaxPollRPS.Get(dc),
		TransferProcessorMaxPollHostRPS:                     dynamicconfig.TransferProcessorMaxPollHostRPS.Get(dc),
		TransferProcessorMaxPollInterval:                    dynamicconfig.TransferProcessorMaxPollInterval.Get(dc),
		TransferProcessorMaxPollIntervalJitterCoefficient:   dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient.Get(dc),
		TransferProcessorUpdateAckInterval:                  dynamicconfig.TransferProcessorUpdateAckInterval.Get(dc),
		TransferProcessorUpdateAckIntervalJitterCoefficient: dynamicconfig.TransferProcessorUpdateAckIntervalJitterCoefficient.Get(dc),
		TransferProcessorPollBackoffInterval:                dynamicconfig.TransferProcessorPollBackoffInterval.Get(dc),
		TransferProcessorEnsureCloseBeforeDelete:            dynamicconfig.TransferProcessorEnsureCloseBeforeDelete.Get(dc),
		TimerQueueMaxReaderCount:                            dynamicconfig.TimerQueueMaxReaderCount.Get(dc),

		OutboundTaskBatchSize:                               dynamicconfig.OutboundTaskBatchSize.Get(dc),
		OutboundProcessorMaxPollRPS:                         dynamicconfig.OutboundProcessorMaxPollRPS.Get(dc),
		OutboundProcessorMaxPollHostRPS:                     dynamicconfig.OutboundProcessorMaxPollHostRPS.Get(dc),
		OutboundProcessorMaxPollInterval:                    dynamicconfig.OutboundProcessorMaxPollInterval.Get(dc),
		OutboundProcessorMaxPollIntervalJitterCoefficient:   dynamicconfig.OutboundProcessorMaxPollIntervalJitterCoefficient.Get(dc),
		OutboundProcessorUpdateAckInterval:                  dynamicconfig.OutboundProcessorUpdateAckInterval.Get(dc),
		OutboundProcessorUpdateAckIntervalJitterCoefficient: dynamicconfig.OutboundProcessorUpdateAckIntervalJitterCoefficient.Get(dc),
		OutboundProcessorPollBackoffInterval:                dynamicconfig.OutboundProcessorPollBackoffInterval.Get(dc),
		OutboundQueuePendingTaskCriticalCount:               dynamicconfig.OutboundQueuePendingTaskCriticalCount.Get(dc),
		OutboundQueuePendingTaskMaxCount:                    dynamicconfig.OutboundQueuePendingTaskMaxCount.Get(dc),
		OutboundQueueMaxPredicateSize:                       dynamicconfig.OutboundQueueMaxPredicateSize.Get(dc),
		OutboundQueueMaxReaderCount:                         dynamicconfig.OutboundQueueMaxReaderCount.Get(dc),
		OutboundQueueGroupLimiterBufferSize:                 dynamicconfig.OutboundQueueGroupLimiterBufferSize.Get(dc),
		OutboundQueueGroupLimiterConcurrency:                dynamicconfig.OutboundQueueGroupLimiterConcurrency.Get(dc),
		OutboundQueueHostSchedulerMaxTaskRPS:                dynamicconfig.OutboundQueueHostSchedulerMaxTaskRPS.Get(dc),
		OutboundQueueCircuitBreakerSettings:                 dynamicconfig.OutboundQueueCircuitBreakerSettings.Subscribe(dc),
		OutboundStandbyTaskMissingEventsDestinationDownErr:  dynamicconfig.OutboundStandbyTaskMissingEventsDestinationDownErr.Get(dc),
		OutboundStandbyTaskMissingEventsDiscardDelay:        dynamicconfig.OutboundStandbyTaskMissingEventsDiscardDelay.Get(dc),

		ReplicatorProcessorMaxPollInterval:                  dynamicconfig.ReplicatorProcessorMaxPollInterval.Get(dc),
		ReplicatorProcessorMaxPollIntervalJitterCoefficient: dynamicconfig.ReplicatorProcessorMaxPollIntervalJitterCoefficient.Get(dc),
		ReplicatorProcessorFetchTasksBatchSize:              dynamicconfig.ReplicatorTaskBatchSize.Get(dc),
		ReplicatorProcessorMaxSkipTaskCount:                 dynamicconfig.ReplicatorMaxSkipTaskCount.Get(dc),
		ReplicationTaskProcessorHostQPS:                     dynamicconfig.ReplicationTaskProcessorHostQPS.Get(dc),
		ReplicationTaskProcessorShardQPS:                    dynamicconfig.ReplicationTaskProcessorShardQPS.Get(dc),
		ReplicationEnableDLQMetrics:                         dynamicconfig.ReplicationEnableDLQMetrics.Get(dc),
		ReplicationEnableUpdateWithNewTaskMerge:             dynamicconfig.ReplicationEnableUpdateWithNewTaskMerge.Get(dc),
		ReplicationStreamSyncStatusDuration:                 dynamicconfig.ReplicationStreamSyncStatusDuration.Get(dc),
		ReplicationProcessorSchedulerQueueSize:              dynamicconfig.ReplicationProcessorSchedulerQueueSize.Get(dc),
		ReplicationProcessorSchedulerWorkerCount:            dynamicconfig.ReplicationProcessorSchedulerWorkerCount.Subscribe(dc),
		ReplicationLowPriorityProcessorSchedulerWorkerCount: dynamicconfig.ReplicationLowPriorityProcessorSchedulerWorkerCount.Subscribe(dc),
		ReplicationLowPriorityTaskParallelism:               dynamicconfig.ReplicationLowPriorityTaskParallelism.Get(dc),
		EnableReplicationEagerRefreshNamespace:              dynamicconfig.EnableEagerNamespaceRefresher.Get(dc),
		EnableReplicationTaskBatching:                       dynamicconfig.EnableReplicationTaskBatching.Get(dc),
		EnableReplicateLocalGeneratedEvent:                  dynamicconfig.EnableReplicateLocalGeneratedEvents.Get(dc),
		EnableReplicationTaskTieredProcessing:               dynamicconfig.EnableReplicationTaskTieredProcessing.Get(dc),
		ReplicationStreamSenderHighPriorityQPS:              dynamicconfig.ReplicationStreamSenderHighPriorityQPS.Get(dc),
		ReplicationStreamSenderLowPriorityQPS:               dynamicconfig.ReplicationStreamSenderLowPriorityQPS.Get(dc),
		ReplicationReceiverMaxOutstandingTaskCount:          dynamicconfig.ReplicationReceiverMaxOutstandingTaskCount.Get(dc),
		ReplicationResendMaxBatchCount:                      dynamicconfig.ReplicationResendMaxBatchCount.Get(dc),
		ReplicationProgressCacheMaxSize:                     dynamicconfig.ReplicationProgressCacheMaxSize.Get(dc),
		ReplicationProgressCacheTTL:                         dynamicconfig.ReplicationProgressCacheTTL.Get(dc),

		MaximumBufferedEventsBatch:       dynamicconfig.MaximumBufferedEventsBatch.Get(dc),
		MaximumBufferedEventsSizeInBytes: dynamicconfig.MaximumBufferedEventsSizeInBytes.Get(dc),
		MaximumSignalsPerExecution:       dynamicconfig.MaximumSignalsPerExecution.Get(dc),
		ShardUpdateMinInterval:           dynamicconfig.ShardUpdateMinInterval.Get(dc),
		ShardFirstUpdateInterval:         dynamicconfig.ShardFirstUpdateInterval.Get(dc),
		ShardUpdateMinTasksCompleted:     dynamicconfig.ShardUpdateMinTasksCompleted.Get(dc),
		ShardSyncMinInterval:             dynamicconfig.ShardSyncMinInterval.Get(dc),
		ShardSyncTimerJitterCoefficient:  dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient.Get(dc),

		// history client: client/history/client.go set the client timeout 30s
		// TODO: Return this value to the client: go.temporal.io/server/issues/294
		LongPollExpirationInterval:          dynamicconfig.HistoryLongPollExpirationInterval.Get(dc),
		EventEncodingType:                   dynamicconfig.DefaultEventEncoding.Get(dc),
		EnableParentClosePolicy:             dynamicconfig.EnableParentClosePolicy.Get(dc),
		NumParentClosePolicySystemWorkflows: dynamicconfig.NumParentClosePolicySystemWorkflows.Get(dc),
		EnableParentClosePolicyWorker:       dynamicconfig.EnableParentClosePolicyWorker.Get(dc),
		ParentClosePolicyThreshold:          dynamicconfig.ParentClosePolicyThreshold.Get(dc),

		BlobSizeLimitError:                        dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:                         dynamicconfig.BlobSizeLimitWarn.Get(dc),
		MemoSizeLimitError:                        dynamicconfig.MemoSizeLimitError.Get(dc),
		MemoSizeLimitWarn:                         dynamicconfig.MemoSizeLimitWarn.Get(dc),
		NumPendingChildExecutionsLimit:            dynamicconfig.NumPendingChildExecutionsLimitError.Get(dc),
		NumPendingActivitiesLimit:                 dynamicconfig.NumPendingActivitiesLimitError.Get(dc),
		NumPendingSignalsLimit:                    dynamicconfig.NumPendingSignalsLimitError.Get(dc),
		NumPendingCancelsRequestLimit:             dynamicconfig.NumPendingCancelRequestsLimitError.Get(dc),
		HistorySizeLimitError:                     dynamicconfig.HistorySizeLimitError.Get(dc),
		HistorySizeLimitWarn:                      dynamicconfig.HistorySizeLimitWarn.Get(dc),
		HistorySizeSuggestContinueAsNew:           dynamicconfig.HistorySizeSuggestContinueAsNew.Get(dc),
		HistoryCountLimitError:                    dynamicconfig.HistoryCountLimitError.Get(dc),
		HistoryCountLimitWarn:                     dynamicconfig.HistoryCountLimitWarn.Get(dc),
		HistoryCountSuggestContinueAsNew:          dynamicconfig.HistoryCountSuggestContinueAsNew.Get(dc),
		HistoryMaxPageSize:                        dynamicconfig.HistoryMaxPageSize.Get(dc),
		MutableStateActivityFailureSizeLimitError: dynamicconfig.MutableStateActivityFailureSizeLimitError.Get(dc),
		MutableStateActivityFailureSizeLimitWarn:  dynamicconfig.MutableStateActivityFailureSizeLimitWarn.Get(dc),
		MutableStateSizeLimitError:                dynamicconfig.MutableStateSizeLimitError.Get(dc),
		MutableStateSizeLimitWarn:                 dynamicconfig.MutableStateSizeLimitWarn.Get(dc),
		MutableStateTombstoneCountLimit:           dynamicconfig.MutableStateTombstoneCountLimit.Get(dc),

		ThrottledLogRPS:   dynamicconfig.HistoryThrottledLogRPS.Get(dc),
		EnableStickyQuery: dynamicconfig.EnableStickyQuery.Get(dc),

		DefaultActivityRetryPolicy:   dynamicconfig.DefaultActivityRetryPolicy.Get(dc),
		DefaultWorkflowRetryPolicy:   dynamicconfig.DefaultWorkflowRetryPolicy.Get(dc),
		WorkflowTaskHeartbeatTimeout: dynamicconfig.WorkflowTaskHeartbeatTimeout.Get(dc),
		WorkflowTaskCriticalAttempts: dynamicconfig.WorkflowTaskCriticalAttempts.Get(dc),
		WorkflowTaskRetryMaxInterval: dynamicconfig.WorkflowTaskRetryMaxInterval.Get(dc),

		ReplicationTaskApplyTimeout:                  dynamicconfig.ReplicationTaskApplyTimeout.Get(dc),
		ReplicationTaskFetcherParallelism:            dynamicconfig.ReplicationTaskFetcherParallelism.Get(dc),
		ReplicationTaskFetcherAggregationInterval:    dynamicconfig.ReplicationTaskFetcherAggregationInterval.Get(dc),
		ReplicationTaskFetcherTimerJitterCoefficient: dynamicconfig.ReplicationTaskFetcherTimerJitterCoefficient.Get(dc),
		ReplicationTaskFetcherErrorRetryWait:         dynamicconfig.ReplicationTaskFetcherErrorRetryWait.Get(dc),

		ReplicationTaskProcessorErrorRetryWait:               dynamicconfig.ReplicationTaskProcessorErrorRetryWait.Get(dc),
		ReplicationTaskProcessorErrorRetryBackoffCoefficient: dynamicconfig.ReplicationTaskProcessorErrorRetryBackoffCoefficient.Get(dc),
		ReplicationTaskProcessorErrorRetryMaxInterval:        dynamicconfig.ReplicationTaskProcessorErrorRetryMaxInterval.Get(dc),
		ReplicationTaskProcessorErrorRetryMaxAttempts:        dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts.Get(dc),
		ReplicationTaskProcessorErrorRetryExpiration:         dynamicconfig.ReplicationTaskProcessorErrorRetryExpiration.Get(dc),
		ReplicationTaskProcessorNoTaskRetryWait:              dynamicconfig.ReplicationTaskProcessorNoTaskInitialWait.Get(dc),
		ReplicationTaskProcessorCleanupInterval:              dynamicconfig.ReplicationTaskProcessorCleanupInterval.Get(dc),
		ReplicationTaskProcessorCleanupJitterCoefficient:     dynamicconfig.ReplicationTaskProcessorCleanupJitterCoefficient.Get(dc),
		ReplicationMultipleBatches:                           dynamicconfig.ReplicationMultipleBatches.Get(dc),

		MaxBufferedQueryCount:                 dynamicconfig.MaxBufferedQueryCount.Get(dc),
		MutableStateChecksumGenProbability:    dynamicconfig.MutableStateChecksumGenProbability.Get(dc),
		MutableStateChecksumVerifyProbability: dynamicconfig.MutableStateChecksumVerifyProbability.Get(dc),
		MutableStateChecksumInvalidateBefore:  dynamicconfig.MutableStateChecksumInvalidateBefore.Get(dc),

		StandbyTaskReReplicationContextTimeout: dynamicconfig.StandbyTaskReReplicationContextTimeout.Get(dc),

		SkipReapplicationByNamespaceID: dynamicconfig.SkipReapplicationByNamespaceID.Get(dc),

		// ===== Visibility related =====
		VisibilityTaskBatchSize:                               dynamicconfig.VisibilityTaskBatchSize.Get(dc),
		VisibilityProcessorMaxPollRPS:                         dynamicconfig.VisibilityProcessorMaxPollRPS.Get(dc),
		VisibilityProcessorMaxPollHostRPS:                     dynamicconfig.VisibilityProcessorMaxPollHostRPS.Get(dc),
		VisibilityProcessorSchedulerWorkerCount:               dynamicconfig.VisibilityProcessorSchedulerWorkerCount.Subscribe(dc),
		VisibilityProcessorSchedulerActiveRoundRobinWeights:   dynamicconfig.VisibilityProcessorSchedulerActiveRoundRobinWeights.WithDefault(ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)).Get(dc),
		VisibilityProcessorSchedulerStandbyRoundRobinWeights:  dynamicconfig.VisibilityProcessorSchedulerStandbyRoundRobinWeights.WithDefault(ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)).Get(dc),
		VisibilityProcessorMaxPollInterval:                    dynamicconfig.VisibilityProcessorMaxPollInterval.Get(dc),
		VisibilityProcessorMaxPollIntervalJitterCoefficient:   dynamicconfig.VisibilityProcessorMaxPollIntervalJitterCoefficient.Get(dc),
		VisibilityProcessorUpdateAckInterval:                  dynamicconfig.VisibilityProcessorUpdateAckInterval.Get(dc),
		VisibilityProcessorUpdateAckIntervalJitterCoefficient: dynamicconfig.VisibilityProcessorUpdateAckIntervalJitterCoefficient.Get(dc),
		VisibilityProcessorPollBackoffInterval:                dynamicconfig.VisibilityProcessorPollBackoffInterval.Get(dc),
		VisibilityProcessorEnsureCloseBeforeDelete:            dynamicconfig.VisibilityProcessorEnsureCloseBeforeDelete.Get(dc),
		VisibilityProcessorEnableCloseWorkflowCleanup:         dynamicconfig.VisibilityProcessorEnableCloseWorkflowCleanup.Get(dc),
		VisibilityProcessorRelocateAttributesMinBlobSize:      dynamicconfig.VisibilityProcessorRelocateAttributesMinBlobSize.Get(dc),
		VisibilityQueueMaxReaderCount:                         dynamicconfig.VisibilityQueueMaxReaderCount.Get(dc),

		DisableFetchRelocatableAttributesFromVisibility: dynamicconfig.DisableFetchRelocatableAttributesFromVisibility.Get(dc),

		SearchAttributesNumberOfKeysLimit: dynamicconfig.SearchAttributesNumberOfKeysLimit.Get(dc),
		SearchAttributesSizeOfValueLimit:  dynamicconfig.SearchAttributesSizeOfValueLimit.Get(dc),
		SearchAttributesTotalSizeLimit:    dynamicconfig.SearchAttributesTotalSizeLimit.Get(dc),
		IndexerConcurrency:                dynamicconfig.WorkerIndexerConcurrency.Get(dc),
		ESProcessorNumOfWorkers:           dynamicconfig.WorkerESProcessorNumOfWorkers.Get(dc),
		// Should not be greater than number of visibility task queue workers VisibilityProcessorSchedulerWorkerCount (default 512)
		// Otherwise, visibility queue processors won't be able to fill up bulk with documents (even under heavy load) and bulk will flush due to interval, not number of actions.
		ESProcessorBulkActions: dynamicconfig.WorkerESProcessorBulkActions.Get(dc),
		// 16MB - just a sanity check. With ES document size ~1Kb it should never be reached.
		ESProcessorBulkSize: dynamicconfig.WorkerESProcessorBulkSize.Get(dc),
		// Bulk processor will flush every this interval regardless of last flush due to bulk actions.
		ESProcessorFlushInterval: dynamicconfig.WorkerESProcessorFlushInterval.Get(dc),
		ESProcessorAckTimeout:    dynamicconfig.WorkerESProcessorAckTimeout.Get(dc),

		EnableCrossNamespaceCommands:  dynamicconfig.EnableCrossNamespaceCommands.Get(dc),
		EnableActivityEagerExecution:  dynamicconfig.EnableActivityEagerExecution.Get(dc),
		EnableEagerWorkflowStart:      dynamicconfig.EnableEagerWorkflowStart.Get(dc),
		NamespaceCacheRefreshInterval: dynamicconfig.NamespaceCacheRefreshInterval.Get(dc),

		// Archival related
		ArchivalTaskBatchSize:                               dynamicconfig.ArchivalTaskBatchSize.Get(dc),
		ArchivalProcessorMaxPollRPS:                         dynamicconfig.ArchivalProcessorMaxPollRPS.Get(dc),
		ArchivalProcessorMaxPollHostRPS:                     dynamicconfig.ArchivalProcessorMaxPollHostRPS.Get(dc),
		ArchivalProcessorSchedulerWorkerCount:               dynamicconfig.ArchivalProcessorSchedulerWorkerCount.Subscribe(dc),
		ArchivalProcessorMaxPollInterval:                    dynamicconfig.ArchivalProcessorMaxPollInterval.Get(dc),
		ArchivalProcessorMaxPollIntervalJitterCoefficient:   dynamicconfig.ArchivalProcessorMaxPollIntervalJitterCoefficient.Get(dc),
		ArchivalProcessorUpdateAckInterval:                  dynamicconfig.ArchivalProcessorUpdateAckInterval.Get(dc),
		ArchivalProcessorUpdateAckIntervalJitterCoefficient: dynamicconfig.ArchivalProcessorUpdateAckIntervalJitterCoefficient.Get(dc),
		ArchivalProcessorPollBackoffInterval:                dynamicconfig.ArchivalProcessorPollBackoffInterval.Get(dc),
		ArchivalProcessorArchiveDelay:                       dynamicconfig.ArchivalProcessorArchiveDelay.Get(dc),
		ArchivalBackendMaxRPS:                               dynamicconfig.ArchivalBackendMaxRPS.Get(dc),
		ArchivalQueueMaxReaderCount:                         dynamicconfig.ArchivalQueueMaxReaderCount.Get(dc),

		// workflow update related
		WorkflowExecutionMaxInFlightUpdates: dynamicconfig.WorkflowExecutionMaxInFlightUpdates.Get(dc),
		WorkflowExecutionMaxTotalUpdates:    dynamicconfig.WorkflowExecutionMaxTotalUpdates.Get(dc),

		SendRawWorkflowHistory:                   dynamicconfig.SendRawWorkflowHistory.Get(dc),
		WorkflowIdReuseMinimalInterval:           dynamicconfig.WorkflowIdReuseMinimalInterval.Get(dc),
		EnableWorkflowIdReuseStartTimeValidation: dynamicconfig.EnableWorkflowIdReuseStartTimeValidation.Get(dc),

		UseExperimentalHsmScheduler: schedulerhsm.UseExperimentalHsmScheduler.Get(dc),
		HsmSchedulerTweakables:      schedulerhsm.CurrentTweakables.Get(dc),

		HealthPersistenceLatencyFailure: dynamicconfig.HealthPersistenceLatencyFailure.Get(dc),
		HealthPersistenceErrorRatio:     dynamicconfig.HealthPersistenceErrorRatio.Get(dc),

		BreakdownMetricsByTaskQueue: dynamicconfig.MetricsBreakdownByTaskQueue.Get(dc),

		LogAllReqErrors: dynamicconfig.LogAllReqErrors.Get(dc),
	}

	return cfg
}

// GetShardID return the corresponding shard ID for a given namespaceID and workflowID pair
func (config *Config) GetShardID(namespaceID namespace.ID, workflowID string) int32 {
	return common.WorkflowIDToHistoryShard(namespaceID.String(), workflowID, config.NumberOfShards)
}
