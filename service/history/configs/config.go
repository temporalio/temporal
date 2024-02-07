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
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility"
)

// Config represents configuration for history service
type Config struct {
	NumberOfShards int32

	EnableReplicationStream dynamicconfig.BoolPropertyFn
	HistoryReplicationDLQV2 dynamicconfig.BoolPropertyFn

	RPS                                   dynamicconfig.IntPropertyFn
	OperatorRPSRatio                      dynamicconfig.FloatPropertyFn
	MaxIDLengthLimit                      dynamicconfig.IntPropertyFn
	PersistenceMaxQPS                     dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS               dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQPS            dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistenceGlobalNamespaceMaxQPS      dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistencePerShardNamespaceMaxQPS    dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnablePersistencePriorityRateLimiting dynamicconfig.BoolPropertyFn
	PersistenceDynamicRateLimitingParams  dynamicconfig.MapPropertyFn

	VisibilityPersistenceMaxReadQPS   dynamicconfig.IntPropertyFn
	VisibilityPersistenceMaxWriteQPS  dynamicconfig.IntPropertyFn
	EnableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
	SecondaryVisibilityWritingMode    dynamicconfig.StringPropertyFn
	VisibilityDisableOrderByClause    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityEnableManualPagination  dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityAllowList               dynamicconfig.BoolPropertyFnWithNamespaceFilter

	EmitShardLagLog            dynamicconfig.BoolPropertyFn
	MaxAutoResetPoints         dynamicconfig.IntPropertyFnWithNamespaceFilter
	ThrottledLogRPS            dynamicconfig.IntPropertyFn
	EnableStickyQuery          dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration      dynamicconfig.DurationPropertyFn
	StartupMembershipJoinDelay dynamicconfig.DurationPropertyFn

	// HistoryCache settings
	// Change of these configs require shard restart
	HistoryCacheInitialSize               dynamicconfig.IntPropertyFn
	HistoryCacheMaxSize                   dynamicconfig.IntPropertyFn
	HistoryCacheTTL                       dynamicconfig.DurationPropertyFn
	HistoryCacheNonUserContextLockTimeout dynamicconfig.DurationPropertyFn
	EnableHostLevelHistoryCache           dynamicconfig.BoolPropertyFn
	HistoryShardLevelCacheMaxSize         dynamicconfig.IntPropertyFn
	EnableAPIGetCurrentRunIDLock          dynamicconfig.BoolPropertyFn

	// EventsCache settings
	// Change of these configs require shard restart
	EventsCacheMaxSizeBytes dynamicconfig.IntPropertyFn
	EventsCacheTTL          dynamicconfig.DurationPropertyFn

	// ShardController settings
	RangeSizeBits                  uint
	AcquireShardInterval           dynamicconfig.DurationPropertyFn
	AcquireShardConcurrency        dynamicconfig.IntPropertyFn
	ShardIOConcurrency             dynamicconfig.IntPropertyFn
	ShardLingerOwnershipCheckQPS   dynamicconfig.IntPropertyFn
	ShardLingerTimeLimit           dynamicconfig.DurationPropertyFn
	ShardOwnershipAssertionEnabled dynamicconfig.BoolPropertyFn

	HistoryClientOwnershipCachingEnabled dynamicconfig.BoolPropertyFn

	// the artificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay                  dynamicconfig.DurationPropertyFn
	StandbyTaskMissingEventsResendDelay  dynamicconfig.DurationPropertyFnWithTaskTypeFilter
	StandbyTaskMissingEventsDiscardDelay dynamicconfig.DurationPropertyFnWithTaskTypeFilter

	QueuePendingTaskCriticalCount    dynamicconfig.IntPropertyFn
	QueueReaderStuckCriticalAttempts dynamicconfig.IntPropertyFn
	QueueCriticalSlicesCount         dynamicconfig.IntPropertyFn
	QueuePendingTaskMaxCount         dynamicconfig.IntPropertyFn

	TaskDLQEnabled                 dynamicconfig.BoolPropertyFn
	TaskDLQUnexpectedErrorAttempts dynamicconfig.IntPropertyFn
	TaskDLQInternalErrors          dynamicconfig.BoolPropertyFn

	TaskSchedulerEnableRateLimiter           dynamicconfig.BoolPropertyFn
	TaskSchedulerEnableRateLimiterShadowMode dynamicconfig.BoolPropertyFn
	TaskSchedulerRateLimiterStartupDelay     dynamicconfig.DurationPropertyFn
	TaskSchedulerGlobalMaxQPS                dynamicconfig.IntPropertyFn
	TaskSchedulerMaxQPS                      dynamicconfig.IntPropertyFn
	TaskSchedulerGlobalNamespaceMaxQPS       dynamicconfig.IntPropertyFnWithNamespaceFilter
	TaskSchedulerNamespaceMaxQPS             dynamicconfig.IntPropertyFnWithNamespaceFilter

	// TimerQueueProcessor settings
	TimerTaskBatchSize                               dynamicconfig.IntPropertyFn
	TimerProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
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

	MemoryTimerProcessorSchedulerWorkerCount dynamicconfig.IntPropertyFn

	// TransferQueueProcessor settings
	TransferTaskBatchSize                               dynamicconfig.IntPropertyFn
	TransferProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
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

	// CallbackQueueProcessor settings
	CallbackTaskBatchSize                               dynamicconfig.IntPropertyFn
	CallbackProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	CallbackProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	CallbackProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	CallbackProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	CallbackProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	CallbackProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	CallbackProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	CallbackQueueMaxReaderCount                         dynamicconfig.IntPropertyFn
	CallbackTaskTimeout                                 dynamicconfig.DurationPropertyFn

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
	NumPendingChildExecutionsLimit            dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingActivitiesLimit                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingSignalsLimit                    dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingCancelsRequestLimit             dynamicconfig.IntPropertyFnWithNamespaceFilter

	// DefaultActivityRetryOptions specifies the out-of-box retry policy if
	// none is configured on the Activity by the user.
	DefaultActivityRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// DefaultWorkflowRetryPolicy specifies the out-of-box retry policy for
	// any unset fields on a RetryPolicy configured on a Workflow
	DefaultWorkflowRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// Workflow task settings
	// DefaultWorkflowTaskTimeout the default workflow task timeout
	DefaultWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// WorkflowTaskHeartbeatTimeout is to timeout behavior of: RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true
	// without any commands or messages. After this timeout workflow task will be scheduled to another worker(by clear stickyness).
	WorkflowTaskHeartbeatTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	WorkflowTaskCriticalAttempts dynamicconfig.IntPropertyFn
	WorkflowTaskRetryMaxInterval dynamicconfig.DurationPropertyFn

	// ContinueAsNewMinInterval is the minimal interval between continue_as_new to prevent tight continue_as_new loop.
	ContinueAsNewMinInterval dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// The following is used by the new RPC replication stack
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

	ReplicationStreamSyncStatusDuration      dynamicconfig.DurationPropertyFn
	ReplicationProcessorSchedulerQueueSize   dynamicconfig.IntPropertyFn
	ReplicationProcessorSchedulerWorkerCount dynamicconfig.IntPropertyFn
	EnableReplicationEagerRefreshNamespace   dynamicconfig.BoolPropertyFn
	EnableReplicationTaskBatching            dynamicconfig.BoolPropertyFn
	EnableReplicateLocalGeneratedEvent       dynamicconfig.BoolPropertyFn

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
	VisibilityProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
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
	VisibilityQueueMaxReaderCount                         dynamicconfig.IntPropertyFn

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
	ArchivalProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
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

	// FrontendAccessHistoryFraction is an interim flag across 2 minor releases and will be removed once fully enabled.
	FrontendAccessHistoryFraction dynamicconfig.FloatPropertyFn
}

const (
	DefaultHistoryMaxAutoResetPoints = 20
)

// NewConfig returns new service config with default values
func NewConfig(
	dc *dynamicconfig.Collection,
	numberOfShards int32,
) *Config {
	cfg := &Config{
		NumberOfShards: numberOfShards,

		EnableReplicationStream: dc.GetBoolProperty(dynamicconfig.EnableReplicationStream, false),
		HistoryReplicationDLQV2: dc.GetBoolProperty(dynamicconfig.EnableHistoryReplicationDLQV2, false),

		RPS:                                   dc.GetIntProperty(dynamicconfig.HistoryRPS, 3000),
		OperatorRPSRatio:                      dc.GetFloat64Property(dynamicconfig.OperatorRPSRatio, common.DefaultOperatorRPSRatio),
		MaxIDLengthLimit:                      dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		PersistenceMaxQPS:                     dc.GetIntProperty(dynamicconfig.HistoryPersistenceMaxQPS, 9000),
		PersistenceGlobalMaxQPS:               dc.GetIntProperty(dynamicconfig.HistoryPersistenceGlobalMaxQPS, 0),
		PersistenceNamespaceMaxQPS:            dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryPersistenceNamespaceMaxQPS, 0),
		PersistenceGlobalNamespaceMaxQPS:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryPersistenceGlobalNamespaceMaxQPS, 0),
		PersistencePerShardNamespaceMaxQPS:    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryPersistencePerShardNamespaceMaxQPS, 0),
		EnablePersistencePriorityRateLimiting: dc.GetBoolProperty(dynamicconfig.HistoryEnablePersistencePriorityRateLimiting, true),
		PersistenceDynamicRateLimitingParams:  dc.GetMapProperty(dynamicconfig.HistoryPersistenceDynamicRateLimitingParams, dynamicconfig.DefaultDynamicRateLimitingParams),
		ShutdownDrainDuration:                 dc.GetDurationProperty(dynamicconfig.HistoryShutdownDrainDuration, 0*time.Second),
		StartupMembershipJoinDelay:            dc.GetDurationProperty(dynamicconfig.HistoryStartupMembershipJoinDelay, 0*time.Second),
		MaxAutoResetPoints:                    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryMaxAutoResetPoints, DefaultHistoryMaxAutoResetPoints),
		DefaultWorkflowTaskTimeout:            dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		ContinueAsNewMinInterval:              dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.ContinueAsNewMinInterval, time.Second),

		VisibilityPersistenceMaxReadQPS:   visibility.GetVisibilityPersistenceMaxReadQPS(dc),
		VisibilityPersistenceMaxWriteQPS:  visibility.GetVisibilityPersistenceMaxWriteQPS(dc),
		EnableReadFromSecondaryVisibility: visibility.GetEnableReadFromSecondaryVisibilityConfig(dc),
		SecondaryVisibilityWritingMode:    visibility.GetSecondaryVisibilityWritingModeConfig(dc),
		VisibilityDisableOrderByClause:    dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityDisableOrderByClause, true),
		VisibilityEnableManualPagination:  dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityEnableManualPagination, true),
		VisibilityAllowList:               dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityAllowList, true),

		EmitShardLagLog:                       dc.GetBoolProperty(dynamicconfig.EmitShardLagLog, false),
		HistoryCacheInitialSize:               dc.GetIntProperty(dynamicconfig.HistoryCacheInitialSize, 128),
		HistoryCacheMaxSize:                   dc.GetIntProperty(dynamicconfig.HistoryCacheMaxSize, 256000),
		HistoryCacheTTL:                       dc.GetDurationProperty(dynamicconfig.HistoryCacheTTL, time.Hour),
		HistoryCacheNonUserContextLockTimeout: dc.GetDurationProperty(dynamicconfig.HistoryCacheNonUserContextLockTimeout, 500*time.Millisecond),
		EnableHostLevelHistoryCache:           dc.GetBoolProperty(dynamicconfig.EnableHostHistoryCache, false),
		HistoryShardLevelCacheMaxSize:         dc.GetIntProperty(dynamicconfig.HistoryCacheShardLevelMaxSize, 512),
		EnableAPIGetCurrentRunIDLock:          dc.GetBoolProperty(dynamicconfig.EnableAPIGetCurrentRunIDLock, false),

		EventsCacheMaxSizeBytes: dc.GetIntProperty(dynamicconfig.EventsCacheMaxSizeBytes, 512*1024), // 512KB
		EventsCacheTTL:          dc.GetDurationProperty(dynamicconfig.EventsCacheTTL, time.Hour),

		RangeSizeBits:                  20, // 20 bits for sequencer, 2^20 sequence number for any range
		AcquireShardInterval:           dc.GetDurationProperty(dynamicconfig.AcquireShardInterval, time.Minute),
		AcquireShardConcurrency:        dc.GetIntProperty(dynamicconfig.AcquireShardConcurrency, 10),
		ShardIOConcurrency:             dc.GetIntProperty(dynamicconfig.ShardIOConcurrency, 1),
		ShardLingerOwnershipCheckQPS:   dc.GetIntProperty(dynamicconfig.ShardLingerOwnershipCheckQPS, 4),
		ShardLingerTimeLimit:           dc.GetDurationProperty(dynamicconfig.ShardLingerTimeLimit, 0),
		ShardOwnershipAssertionEnabled: dc.GetBoolProperty(dynamicconfig.ShardOwnershipAssertionEnabled, true),

		HistoryClientOwnershipCachingEnabled: dc.GetBoolProperty(dynamicconfig.HistoryClientOwnershipCachingEnabled, false),

		StandbyClusterDelay:                  dc.GetDurationProperty(dynamicconfig.StandbyClusterDelay, 5*time.Minute),
		StandbyTaskMissingEventsResendDelay:  dc.GetDurationPropertyFilteredByTaskType(dynamicconfig.StandbyTaskMissingEventsResendDelay, 10*time.Minute),
		StandbyTaskMissingEventsDiscardDelay: dc.GetDurationPropertyFilteredByTaskType(dynamicconfig.StandbyTaskMissingEventsDiscardDelay, 15*time.Minute),

		QueuePendingTaskCriticalCount:    dc.GetIntProperty(dynamicconfig.QueuePendingTaskCriticalCount, 9000),
		QueueReaderStuckCriticalAttempts: dc.GetIntProperty(dynamicconfig.QueueReaderStuckCriticalAttempts, 3),
		QueueCriticalSlicesCount:         dc.GetIntProperty(dynamicconfig.QueueCriticalSlicesCount, 50),
		QueuePendingTaskMaxCount:         dc.GetIntProperty(dynamicconfig.QueuePendingTaskMaxCount, 10000),

		TaskDLQEnabled:                 dc.GetBoolProperty(dynamicconfig.HistoryTaskDLQEnabled, true),
		TaskDLQUnexpectedErrorAttempts: dc.GetIntProperty(dynamicconfig.HistoryTaskDLQUnexpectedErrorAttempts, 100),
		TaskDLQInternalErrors:          dc.GetBoolProperty(dynamicconfig.HistoryTaskDLQInternalErrors, false),

		TaskSchedulerEnableRateLimiter:           dc.GetBoolProperty(dynamicconfig.TaskSchedulerEnableRateLimiter, false),
		TaskSchedulerEnableRateLimiterShadowMode: dc.GetBoolProperty(dynamicconfig.TaskSchedulerEnableRateLimiterShadowMode, true),
		TaskSchedulerRateLimiterStartupDelay:     dc.GetDurationProperty(dynamicconfig.TaskSchedulerRateLimiterStartupDelay, 5*time.Second),
		TaskSchedulerGlobalMaxQPS:                dc.GetIntProperty(dynamicconfig.TaskSchedulerGlobalMaxQPS, 0),
		TaskSchedulerMaxQPS:                      dc.GetIntProperty(dynamicconfig.TaskSchedulerMaxQPS, 0),
		TaskSchedulerNamespaceMaxQPS:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.TaskSchedulerNamespaceMaxQPS, 0),
		TaskSchedulerGlobalNamespaceMaxQPS:       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.TaskSchedulerGlobalNamespaceMaxQPS, 0),

		TimerTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.TimerTaskBatchSize, 100),
		TimerProcessorSchedulerWorkerCount:               dc.GetIntProperty(dynamicconfig.TimerProcessorSchedulerWorkerCount, 512),
		TimerProcessorSchedulerActiveRoundRobinWeights:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TimerProcessorSchedulerActiveRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		TimerProcessorSchedulerStandbyRoundRobinWeights:  dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TimerProcessorSchedulerStandbyRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)),
		TimerProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.TimerProcessorUpdateAckInterval, 30*time.Second),
		TimerProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TimerProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TimerProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.TimerProcessorMaxPollRPS, 20),
		TimerProcessorMaxPollHostRPS:                     dc.GetIntProperty(dynamicconfig.TimerProcessorMaxPollHostRPS, 0),
		TimerProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxPollInterval, 5*time.Minute),
		TimerProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.TimerProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TimerProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.TimerProcessorPollBackoffInterval, 5*time.Second),
		TimerProcessorMaxTimeShift:                       dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxTimeShift, 1*time.Second),
		TransferQueueMaxReaderCount:                      dc.GetIntProperty(dynamicconfig.TransferQueueMaxReaderCount, 2),
		RetentionTimerJitterDuration:                     dc.GetDurationProperty(dynamicconfig.RetentionTimerJitterDuration, 30*time.Minute),

		MemoryTimerProcessorSchedulerWorkerCount: dc.GetIntProperty(dynamicconfig.MemoryTimerProcessorSchedulerWorkerCount, 64),

		TransferTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.TransferTaskBatchSize, 100),
		TransferProcessorSchedulerWorkerCount:               dc.GetIntProperty(dynamicconfig.TransferProcessorSchedulerWorkerCount, 512),
		TransferProcessorSchedulerActiveRoundRobinWeights:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TransferProcessorSchedulerActiveRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		TransferProcessorSchedulerStandbyRoundRobinWeights:  dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TransferProcessorSchedulerStandbyRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)),
		TransferProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.TransferProcessorMaxPollRPS, 20),
		TransferProcessorMaxPollHostRPS:                     dc.GetIntProperty(dynamicconfig.TransferProcessorMaxPollHostRPS, 0),
		TransferProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.TransferProcessorMaxPollInterval, 1*time.Minute),
		TransferProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TransferProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.TransferProcessorUpdateAckInterval, 30*time.Second),
		TransferProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TransferProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TransferProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.TransferProcessorPollBackoffInterval, 5*time.Second),
		TransferProcessorEnsureCloseBeforeDelete:            dc.GetBoolProperty(dynamicconfig.TransferProcessorEnsureCloseBeforeDelete, true),
		TimerQueueMaxReaderCount:                            dc.GetIntProperty(dynamicconfig.TimerQueueMaxReaderCount, 2),

		CallbackTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.CallbackTaskBatchSize, 100),
		CallbackProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.CallbackProcessorMaxPollRPS, 20),
		CallbackProcessorMaxPollHostRPS:                     dc.GetIntProperty(dynamicconfig.CallbackProcessorMaxPollHostRPS, 0),
		CallbackProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.CallbackProcessorMaxPollInterval, 1*time.Minute),
		CallbackProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.CallbackProcessorMaxPollIntervalJitterCoefficient, 0.15),
		CallbackProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.CallbackProcessorUpdateAckInterval, 30*time.Second),
		CallbackProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.CallbackProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		CallbackProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.CallbackProcessorPollBackoffInterval, 5*time.Second),
		CallbackQueueMaxReaderCount:                         dc.GetIntProperty(dynamicconfig.CallbackQueueMaxReaderCount, 4),
		CallbackTaskTimeout:                                 dc.GetDurationProperty(dynamicconfig.CallbackTaskTimeout, time.Second*10),

		ReplicatorProcessorMaxPollInterval:                  dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorMaxPollInterval, 1*time.Minute),
		ReplicatorProcessorMaxPollIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorMaxPollIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorFetchTasksBatchSize:              dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 25),
		ReplicatorProcessorMaxSkipTaskCount:                 dc.GetIntProperty(dynamicconfig.ReplicatorMaxSkipTaskCount, 250),
		ReplicationTaskProcessorHostQPS:                     dc.GetFloat64Property(dynamicconfig.ReplicationTaskProcessorHostQPS, 1500),
		ReplicationTaskProcessorShardQPS:                    dc.GetFloat64Property(dynamicconfig.ReplicationTaskProcessorShardQPS, 30),
		ReplicationEnableDLQMetrics:                         dc.GetBoolProperty(dynamicconfig.ReplicationEnableDLQMetrics, true),
		ReplicationStreamSyncStatusDuration:                 dc.GetDurationProperty(dynamicconfig.ReplicationStreamSyncStatusDuration, 1*time.Second),
		ReplicationProcessorSchedulerQueueSize:              dc.GetIntProperty(dynamicconfig.ReplicationProcessorSchedulerQueueSize, 128),
		ReplicationProcessorSchedulerWorkerCount:            dc.GetIntProperty(dynamicconfig.ReplicationProcessorSchedulerWorkerCount, 512),
		EnableReplicationEagerRefreshNamespace:              dc.GetBoolProperty(dynamicconfig.EnableEagerNamespaceRefresher, false),
		EnableReplicationTaskBatching:                       dc.GetBoolProperty(dynamicconfig.EnableReplicationTaskBatching, false),
		EnableReplicateLocalGeneratedEvent:                  dc.GetBoolProperty(dynamicconfig.EnableReplicateLocalGeneratedEvents, false),

		MaximumBufferedEventsBatch:       dc.GetIntProperty(dynamicconfig.MaximumBufferedEventsBatch, 100),
		MaximumBufferedEventsSizeInBytes: dc.GetIntProperty(dynamicconfig.MaximumBufferedEventsSizeInBytes, 2*1024*1024),
		MaximumSignalsPerExecution:       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MaximumSignalsPerExecution, 10000),
		ShardUpdateMinInterval:           dc.GetDurationProperty(dynamicconfig.ShardUpdateMinInterval, 5*time.Minute),
		ShardSyncMinInterval:             dc.GetDurationProperty(dynamicconfig.ShardSyncMinInterval, 5*time.Minute),
		ShardSyncTimerJitterCoefficient:  dc.GetFloat64Property(dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient, 0.15),

		// history client: client/history/client.go set the client timeout 30s
		// TODO: Return this value to the client: go.temporal.io/server/issues/294
		LongPollExpirationInterval:          dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.HistoryLongPollExpirationInterval, time.Second*20),
		EventEncodingType:                   dc.GetStringPropertyFnWithNamespaceFilter(dynamicconfig.DefaultEventEncoding, enumspb.ENCODING_TYPE_PROTO3.String()),
		EnableParentClosePolicy:             dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableParentClosePolicy, true),
		NumParentClosePolicySystemWorkflows: dc.GetIntProperty(dynamicconfig.NumParentClosePolicySystemWorkflows, 10),
		EnableParentClosePolicyWorker:       dc.GetBoolProperty(dynamicconfig.EnableParentClosePolicyWorker, true),
		ParentClosePolicyThreshold:          dc.GetIntPropertyFilteredByNamespace(dynamicconfig.ParentClosePolicyThreshold, 10),

		BlobSizeLimitError:                        dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:                         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitWarn, 512*1024),
		MemoSizeLimitError:                        dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MemoSizeLimitError, 2*1024*1024),
		MemoSizeLimitWarn:                         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MemoSizeLimitWarn, 2*1024),
		NumPendingChildExecutionsLimit:            dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingChildExecutionsLimitError, 2000),
		NumPendingActivitiesLimit:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingActivitiesLimitError, 2000),
		NumPendingSignalsLimit:                    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingSignalsLimitError, 2000),
		NumPendingCancelsRequestLimit:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingCancelRequestsLimitError, 2000),
		HistorySizeLimitError:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeLimitError, 50*1024*1024),
		HistorySizeLimitWarn:                      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeLimitWarn, 10*1024*1024),
		HistorySizeSuggestContinueAsNew:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeSuggestContinueAsNew, 4*1024*1024),
		HistoryCountLimitError:                    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountLimitError, 50*1024),
		HistoryCountLimitWarn:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountLimitWarn, 10*1024),
		HistoryCountSuggestContinueAsNew:          dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountSuggestContinueAsNew, 4*1024),
		HistoryMaxPageSize:                        dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryMaxPageSize, common.GetHistoryMaxPageSize),
		MutableStateActivityFailureSizeLimitError: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MutableStateActivityFailureSizeLimitError, 4*1024),
		MutableStateActivityFailureSizeLimitWarn:  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MutableStateActivityFailureSizeLimitWarn, 2*1024),
		MutableStateSizeLimitError:                dc.GetIntProperty(dynamicconfig.MutableStateSizeLimitError, 8*1024*1024),
		MutableStateSizeLimitWarn:                 dc.GetIntProperty(dynamicconfig.MutableStateSizeLimitWarn, 1*1024*1024),

		ThrottledLogRPS:   dc.GetIntProperty(dynamicconfig.HistoryThrottledLogRPS, 4),
		EnableStickyQuery: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableStickyQuery, true),

		DefaultActivityRetryPolicy:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultActivityRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		DefaultWorkflowRetryPolicy:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultWorkflowRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		WorkflowTaskHeartbeatTimeout: dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.WorkflowTaskHeartbeatTimeout, time.Minute*30),
		WorkflowTaskCriticalAttempts: dc.GetIntProperty(dynamicconfig.WorkflowTaskCriticalAttempts, 10),
		WorkflowTaskRetryMaxInterval: dc.GetDurationProperty(dynamicconfig.WorkflowTaskRetryMaxInterval, time.Minute*10),

		ReplicationTaskFetcherParallelism:            dc.GetIntProperty(dynamicconfig.ReplicationTaskFetcherParallelism, 4),
		ReplicationTaskFetcherAggregationInterval:    dc.GetDurationProperty(dynamicconfig.ReplicationTaskFetcherAggregationInterval, 2*time.Second),
		ReplicationTaskFetcherTimerJitterCoefficient: dc.GetFloat64Property(dynamicconfig.ReplicationTaskFetcherTimerJitterCoefficient, 0.15),
		ReplicationTaskFetcherErrorRetryWait:         dc.GetDurationProperty(dynamicconfig.ReplicationTaskFetcherErrorRetryWait, time.Second),

		ReplicationTaskProcessorErrorRetryWait:               dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryWait, 1*time.Second),
		ReplicationTaskProcessorErrorRetryBackoffCoefficient: dc.GetFloat64PropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryBackoffCoefficient, 1.2),
		ReplicationTaskProcessorErrorRetryMaxInterval:        dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryMaxInterval, 5*time.Second),
		ReplicationTaskProcessorErrorRetryMaxAttempts:        dc.GetIntPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts, 80),
		ReplicationTaskProcessorErrorRetryExpiration:         dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryExpiration, 5*time.Minute),
		ReplicationTaskProcessorNoTaskRetryWait:              dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorNoTaskInitialWait, 2*time.Second),
		ReplicationTaskProcessorCleanupInterval:              dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorCleanupInterval, 1*time.Minute),
		ReplicationTaskProcessorCleanupJitterCoefficient:     dc.GetFloat64PropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorCleanupJitterCoefficient, 0.15),

		MaxBufferedQueryCount:                 dc.GetIntProperty(dynamicconfig.MaxBufferedQueryCount, 1),
		MutableStateChecksumGenProbability:    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MutableStateChecksumGenProbability, 0),
		MutableStateChecksumVerifyProbability: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MutableStateChecksumVerifyProbability, 0),
		MutableStateChecksumInvalidateBefore:  dc.GetFloat64Property(dynamicconfig.MutableStateChecksumInvalidateBefore, 0),

		StandbyTaskReReplicationContextTimeout: dc.GetDurationPropertyFilteredByNamespaceID(dynamicconfig.StandbyTaskReReplicationContextTimeout, 30*time.Second),

		SkipReapplicationByNamespaceID: dc.GetBoolPropertyFnWithNamespaceIDFilter(dynamicconfig.SkipReapplicationByNamespaceID, false),

		// ===== Visibility related =====
		VisibilityTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.VisibilityTaskBatchSize, 100),
		VisibilityProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.VisibilityProcessorMaxPollRPS, 20),
		VisibilityProcessorMaxPollHostRPS:                     dc.GetIntProperty(dynamicconfig.VisibilityProcessorMaxPollHostRPS, 0),
		VisibilityProcessorSchedulerWorkerCount:               dc.GetIntProperty(dynamicconfig.VisibilityProcessorSchedulerWorkerCount, 512),
		VisibilityProcessorSchedulerActiveRoundRobinWeights:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityProcessorSchedulerActiveRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		VisibilityProcessorSchedulerStandbyRoundRobinWeights:  dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityProcessorSchedulerStandbyRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)),
		VisibilityProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.VisibilityProcessorMaxPollInterval, 1*time.Minute),
		VisibilityProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.VisibilityProcessorMaxPollIntervalJitterCoefficient, 0.15),
		VisibilityProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.VisibilityProcessorUpdateAckInterval, 30*time.Second),
		VisibilityProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.VisibilityProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		VisibilityProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.VisibilityProcessorPollBackoffInterval, 5*time.Second),
		VisibilityProcessorEnsureCloseBeforeDelete:            dc.GetBoolProperty(dynamicconfig.VisibilityProcessorEnsureCloseBeforeDelete, false),
		VisibilityProcessorEnableCloseWorkflowCleanup:         dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityProcessorEnableCloseWorkflowCleanup, false),
		VisibilityQueueMaxReaderCount:                         dc.GetIntProperty(dynamicconfig.VisibilityQueueMaxReaderCount, 2),

		SearchAttributesNumberOfKeysLimit: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		IndexerConcurrency:                dc.GetIntProperty(dynamicconfig.WorkerIndexerConcurrency, 100),
		ESProcessorNumOfWorkers:           dc.GetIntProperty(dynamicconfig.WorkerESProcessorNumOfWorkers, 2),
		// Should not be greater than number of visibility task queue workers VisibilityProcessorSchedulerWorkerCount (default 512)
		// Otherwise, visibility queue processors won't be able to fill up bulk with documents (even under heavy load) and bulk will flush due to interval, not number of actions.
		ESProcessorBulkActions: dc.GetIntProperty(dynamicconfig.WorkerESProcessorBulkActions, 500),
		// 16MB - just a sanity check. With ES document size ~1Kb it should never be reached.
		ESProcessorBulkSize: dc.GetIntProperty(dynamicconfig.WorkerESProcessorBulkSize, 16*1024*1024),
		// Bulk processor will flush every this interval regardless of last flush due to bulk actions.
		ESProcessorFlushInterval: dc.GetDurationProperty(dynamicconfig.WorkerESProcessorFlushInterval, 1*time.Second),
		ESProcessorAckTimeout:    dc.GetDurationProperty(dynamicconfig.WorkerESProcessorAckTimeout, 30*time.Second),

		EnableCrossNamespaceCommands:  dc.GetBoolProperty(dynamicconfig.EnableCrossNamespaceCommands, true),
		EnableActivityEagerExecution:  dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableActivityEagerExecution, false),
		EnableEagerWorkflowStart:      dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableEagerWorkflowStart, false),
		NamespaceCacheRefreshInterval: dc.GetDurationProperty(dynamicconfig.NamespaceCacheRefreshInterval, 10*time.Second),

		// Archival related
		ArchivalTaskBatchSize:                 dc.GetIntProperty(dynamicconfig.ArchivalTaskBatchSize, 100),
		ArchivalProcessorMaxPollRPS:           dc.GetIntProperty(dynamicconfig.ArchivalProcessorMaxPollRPS, 20),
		ArchivalProcessorMaxPollHostRPS:       dc.GetIntProperty(dynamicconfig.ArchivalProcessorMaxPollHostRPS, 0),
		ArchivalProcessorSchedulerWorkerCount: dc.GetIntProperty(dynamicconfig.ArchivalProcessorSchedulerWorkerCount, 512),
		ArchivalProcessorMaxPollInterval:      dc.GetDurationProperty(dynamicconfig.ArchivalProcessorMaxPollInterval, 5*time.Minute),
		ArchivalProcessorMaxPollIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.
			ArchivalProcessorMaxPollIntervalJitterCoefficient, 0.15),
		ArchivalProcessorUpdateAckInterval: dc.GetDurationProperty(dynamicconfig.ArchivalProcessorUpdateAckInterval, 30*time.Second),
		ArchivalProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.
			ArchivalProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		ArchivalProcessorPollBackoffInterval: dc.GetDurationProperty(dynamicconfig.ArchivalProcessorPollBackoffInterval, 5*time.Second),
		ArchivalProcessorArchiveDelay:        dc.GetDurationProperty(dynamicconfig.ArchivalProcessorArchiveDelay, 5*time.Minute),
		ArchivalBackendMaxRPS:                dc.GetFloat64Property(dynamicconfig.ArchivalBackendMaxRPS, 10000.0),
		ArchivalQueueMaxReaderCount:          dc.GetIntProperty(dynamicconfig.ArchivalQueueMaxReaderCount, 2),

		// workflow update related
		WorkflowExecutionMaxInFlightUpdates: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10),
		WorkflowExecutionMaxTotalUpdates:    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.WorkflowExecutionMaxTotalUpdates, 2000),

		SendRawWorkflowHistory: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.SendRawWorkflowHistory, false),

		FrontendAccessHistoryFraction: dc.GetFloat64Property(dynamicconfig.FrontendAccessHistoryFraction, 0.0),
	}

	return cfg
}

// GetShardID return the corresponding shard ID for a given namespaceID and workflowID pair
func (config *Config) GetShardID(namespaceID namespace.ID, workflowID string) int32 {
	return common.WorkflowIDToHistoryShard(namespaceID.String(), workflowID, config.NumberOfShards)
}
