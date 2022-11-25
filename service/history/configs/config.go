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
	NumberOfShards             int32
	DefaultVisibilityIndexName string

	RPS                                   dynamicconfig.IntPropertyFn
	MaxIDLengthLimit                      dynamicconfig.IntPropertyFn
	PersistenceMaxQPS                     dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS               dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQPS            dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnablePersistencePriorityRateLimiting dynamicconfig.BoolPropertyFn

	StandardVisibilityPersistenceMaxReadQPS   dynamicconfig.IntPropertyFn
	StandardVisibilityPersistenceMaxWriteQPS  dynamicconfig.IntPropertyFn
	AdvancedVisibilityPersistenceMaxReadQPS   dynamicconfig.IntPropertyFn
	AdvancedVisibilityPersistenceMaxWriteQPS  dynamicconfig.IntPropertyFn
	AdvancedVisibilityWritingMode             dynamicconfig.StringPropertyFn
	EnableWriteToSecondaryAdvancedVisibility  dynamicconfig.BoolPropertyFn
	EnableReadVisibilityFromES                dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableReadFromSecondaryAdvancedVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityDisableOrderByClause            dynamicconfig.BoolPropertyFn

	EmitShardDiffLog      dynamicconfig.BoolPropertyFn
	MaxAutoResetPoints    dynamicconfig.IntPropertyFnWithNamespaceFilter
	ThrottledLogRPS       dynamicconfig.IntPropertyFn
	EnableStickyQuery     dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration dynamicconfig.DurationPropertyFn

	// HistoryCache settings
	// Change of these configs require shard restart
	HistoryCacheInitialSize dynamicconfig.IntPropertyFn
	HistoryCacheMaxSize     dynamicconfig.IntPropertyFn
	HistoryCacheTTL         dynamicconfig.DurationPropertyFn

	// EventsCache settings
	// Change of these configs require shard restart
	EventsCacheInitialSize dynamicconfig.IntPropertyFn
	EventsCacheMaxSize     dynamicconfig.IntPropertyFn
	EventsCacheTTL         dynamicconfig.DurationPropertyFn

	// ShardController settings
	RangeSizeBits           uint
	AcquireShardInterval    dynamicconfig.DurationPropertyFn
	AcquireShardConcurrency dynamicconfig.IntPropertyFn

	// the artificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay                  dynamicconfig.DurationPropertyFn
	StandbyTaskMissingEventsResendDelay  dynamicconfig.DurationPropertyFnWithTaskTypeFilter
	StandbyTaskMissingEventsDiscardDelay dynamicconfig.DurationPropertyFnWithTaskTypeFilter

	QueuePendingTaskCriticalCount    dynamicconfig.IntPropertyFn
	QueueReaderStuckCriticalAttempts dynamicconfig.IntPropertyFn
	QueueCriticalSlicesCount         dynamicconfig.IntPropertyFn
	QueuePendingTaskMaxCount         dynamicconfig.IntPropertyFn
	QueueMaxReaderCount              dynamicconfig.IntPropertyFn

	TaskSchedulerEnableRateLimiter dynamicconfig.BoolPropertyFn
	TaskSchedulerMaxQPS            dynamicconfig.IntPropertyFn
	TaskSchedulerNamespaceMaxQPS   dynamicconfig.IntPropertyFnWithNamespaceFilter

	// TimerQueueProcessor settings
	TimerTaskHighPriorityRPS                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	TimerTaskBatchSize                               dynamicconfig.IntPropertyFn
	TimerTaskWorkerCount                             dynamicconfig.IntPropertyFn
	TimerTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	TimerProcessorEnableSingleProcessor              dynamicconfig.BoolPropertyFn
	TimerProcessorEnableMultiCursor                  dynamicconfig.BoolPropertyFn
	TimerProcessorEnablePriorityTaskScheduler        dynamicconfig.BoolPropertyFn
	TimerProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
	TimerProcessorSchedulerActiveRoundRobinWeights   dynamicconfig.MapPropertyFnWithNamespaceFilter
	TimerProcessorSchedulerStandbyRoundRobinWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
	TimerProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	TimerProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TimerProcessorCompleteTimerInterval              dynamicconfig.DurationPropertyFn
	TimerProcessorFailoverMaxPollRPS                 dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	TimerProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	TimerProcessorMaxReschedulerSize                 dynamicconfig.IntPropertyFn
	TimerProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	TimerProcessorMaxTimeShift                       dynamicconfig.DurationPropertyFn
	TimerProcessorHistoryArchivalSizeLimit           dynamicconfig.IntPropertyFn
	TimerProcessorArchivalTimeLimit                  dynamicconfig.DurationPropertyFn
	RetentionTimerJitterDuration                     dynamicconfig.DurationPropertyFn

	// TransferQueueProcessor settings
	TransferTaskHighPriorityRPS                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	TransferTaskBatchSize                               dynamicconfig.IntPropertyFn
	TransferTaskWorkerCount                             dynamicconfig.IntPropertyFn
	TransferTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	TransferProcessorEnableSingleProcessor              dynamicconfig.BoolPropertyFn
	TransferProcessorEnableMultiCursor                  dynamicconfig.BoolPropertyFn
	TransferProcessorEnablePriorityTaskScheduler        dynamicconfig.BoolPropertyFn
	TransferProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
	TransferProcessorSchedulerActiveRoundRobinWeights   dynamicconfig.MapPropertyFnWithNamespaceFilter
	TransferProcessorSchedulerStandbyRoundRobinWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
	TransferProcessorFailoverMaxPollRPS                 dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	TransferProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	TransferProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	TransferProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TransferProcessorCompleteTransferInterval           dynamicconfig.DurationPropertyFn
	TransferProcessorMaxReschedulerSize                 dynamicconfig.IntPropertyFn
	TransferProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	TransferProcessorVisibilityArchivalTimeLimit        dynamicconfig.DurationPropertyFn
	TransferProcessorEnsureCloseBeforeDelete            dynamicconfig.BoolPropertyFn

	// ReplicatorQueueProcessor settings
	// TODO: clean up unused replicator settings
	ReplicatorTaskBatchSize                               dynamicconfig.IntPropertyFn
	ReplicatorTaskWorkerCount                             dynamicconfig.IntPropertyFn
	ReplicatorTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	ReplicatorProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	ReplicatorProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	ReplicatorProcessorMaxReschedulerSize                 dynamicconfig.IntPropertyFn
	ReplicatorProcessorEnablePriorityTaskProcessor        dynamicconfig.BoolPropertyFn
	ReplicatorProcessorFetchTasksBatchSize                dynamicconfig.IntPropertyFn

	// System Limits
	MaximumBufferedEventsBatch dynamicconfig.IntPropertyFn
	MaximumSignalsPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter

	// ShardUpdateMinInterval the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval dynamicconfig.DurationPropertyFn
	// ShardSyncMinInterval the minimal time interval which the shard info should be sync to remote
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

	// Archival settings
	NumArchiveSystemWorkflows dynamicconfig.IntPropertyFn
	ArchiveRequestRPS         dynamicconfig.IntPropertyFn
	ArchiveSignalTimeout      dynamicconfig.DurationPropertyFn
	DurableArchivalEnabled    dynamicconfig.BoolPropertyFn

	// Size limit related settings
	BlobSizeLimitError             dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn              dynamicconfig.IntPropertyFnWithNamespaceFilter
	MemoSizeLimitError             dynamicconfig.IntPropertyFnWithNamespaceFilter
	MemoSizeLimitWarn              dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeLimitError          dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeLimitWarn           dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountLimitError         dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountLimitWarn          dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingChildExecutionsLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingActivitiesLimit      dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingSignalsLimit         dynamicconfig.IntPropertyFnWithNamespaceFilter
	NumPendingCancelsRequestLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter

	// DefaultActivityRetryOptions specifies the out-of-box retry policy if
	// none is configured on the Activity by the user.
	DefaultActivityRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// DefaultWorkflowRetryPolicy specifies the out-of-box retry policy for
	// any unset fields on a RetryPolicy configured on a Workflow
	DefaultWorkflowRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// Workflow task settings
	// DefaultWorkflowTaskTimeout the default workflow task timeout
	DefaultWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// WorkflowTaskHeartbeatTimeout is to timeout behavior of: RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true without any workflow tasks
	// So that workflow task will be scheduled to another worker(by clear stickyness)
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
	VisibilityTaskHighPriorityRPS                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	VisibilityTaskBatchSize                               dynamicconfig.IntPropertyFn
	VisibilityTaskWorkerCount                             dynamicconfig.IntPropertyFn
	VisibilityTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	VisibilityProcessorEnableMultiCursor                  dynamicconfig.BoolPropertyFn
	VisibilityProcessorEnablePriorityTaskScheduler        dynamicconfig.BoolPropertyFn
	VisibilityProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
	VisibilityProcessorSchedulerActiveRoundRobinWeights   dynamicconfig.MapPropertyFnWithNamespaceFilter
	VisibilityProcessorSchedulerStandbyRoundRobinWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
	VisibilityProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	VisibilityProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	VisibilityProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	VisibilityProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	VisibilityProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	VisibilityProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	VisibilityProcessorCompleteTaskInterval               dynamicconfig.DurationPropertyFn
	VisibilityProcessorMaxReschedulerSize                 dynamicconfig.IntPropertyFn
	VisibilityProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	VisibilityProcessorVisibilityArchivalTimeLimit        dynamicconfig.DurationPropertyFn
	VisibilityProcessorEnsureCloseBeforeDelete            dynamicconfig.BoolPropertyFn
	VisibilityProcessorEnableCloseWorkflowCleanup         dynamicconfig.BoolPropertyFnWithNamespaceFilter

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
	NamespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn

	// ArchivalQueueProcessor settings
	ArchivalProcessorSchedulerWorkerCount               dynamicconfig.IntPropertyFn
	ArchivalProcessorSchedulerRoundRobinWeights         dynamicconfig.MapPropertyFnWithNamespaceFilter
	ArchivalProcessorMaxPollHostRPS                     dynamicconfig.IntPropertyFn
	ArchivalTaskBatchSize                               dynamicconfig.IntPropertyFn
	ArchivalProcessorPollBackoffInterval                dynamicconfig.DurationPropertyFn
	ArchivalProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	ArchivalProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	ArchivalProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	ArchivalProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	ArchivalProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
}

const (
	DefaultHistoryMaxAutoResetPoints = 20
)

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numberOfShards int32, isAdvancedVisibilityConfigExist bool, defaultVisibilityIndex string) *Config {
	cfg := &Config{
		NumberOfShards:             numberOfShards,
		DefaultVisibilityIndexName: defaultVisibilityIndex,

		RPS:                                   dc.GetIntProperty(dynamicconfig.HistoryRPS, 3000),
		MaxIDLengthLimit:                      dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		PersistenceMaxQPS:                     dc.GetIntProperty(dynamicconfig.HistoryPersistenceMaxQPS, 9000),
		PersistenceGlobalMaxQPS:               dc.GetIntProperty(dynamicconfig.HistoryPersistenceGlobalMaxQPS, 0),
		PersistenceNamespaceMaxQPS:            dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryPersistenceNamespaceMaxQPS, 0),
		EnablePersistencePriorityRateLimiting: dc.GetBoolProperty(dynamicconfig.HistoryEnablePersistencePriorityRateLimiting, true),
		ShutdownDrainDuration:                 dc.GetDurationProperty(dynamicconfig.HistoryShutdownDrainDuration, 0*time.Second),
		MaxAutoResetPoints:                    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryMaxAutoResetPoints, DefaultHistoryMaxAutoResetPoints),
		DefaultWorkflowTaskTimeout:            dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		ContinueAsNewMinInterval:              dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.ContinueAsNewMinInterval, time.Second),

		StandardVisibilityPersistenceMaxReadQPS:   dc.GetIntProperty(dynamicconfig.StandardVisibilityPersistenceMaxReadQPS, 9000),
		StandardVisibilityPersistenceMaxWriteQPS:  dc.GetIntProperty(dynamicconfig.StandardVisibilityPersistenceMaxWriteQPS, 9000),
		AdvancedVisibilityPersistenceMaxReadQPS:   dc.GetIntProperty(dynamicconfig.AdvancedVisibilityPersistenceMaxReadQPS, 9000),
		AdvancedVisibilityPersistenceMaxWriteQPS:  dc.GetIntProperty(dynamicconfig.AdvancedVisibilityPersistenceMaxWriteQPS, 9000),
		AdvancedVisibilityWritingMode:             dc.GetStringProperty(dynamicconfig.AdvancedVisibilityWritingMode, visibility.DefaultAdvancedVisibilityWritingMode(isAdvancedVisibilityConfigExist)),
		EnableWriteToSecondaryAdvancedVisibility:  dc.GetBoolProperty(dynamicconfig.EnableWriteToSecondaryAdvancedVisibility, false),
		EnableReadVisibilityFromES:                dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableReadVisibilityFromES, isAdvancedVisibilityConfigExist),
		EnableReadFromSecondaryAdvancedVisibility: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableReadFromSecondaryAdvancedVisibility, false),
		VisibilityDisableOrderByClause:            dc.GetBoolProperty(dynamicconfig.VisibilityDisableOrderByClause, false),

		EmitShardDiffLog:                     dc.GetBoolProperty(dynamicconfig.EmitShardDiffLog, false),
		HistoryCacheInitialSize:              dc.GetIntProperty(dynamicconfig.HistoryCacheInitialSize, 128),
		HistoryCacheMaxSize:                  dc.GetIntProperty(dynamicconfig.HistoryCacheMaxSize, 512),
		HistoryCacheTTL:                      dc.GetDurationProperty(dynamicconfig.HistoryCacheTTL, time.Hour),
		EventsCacheInitialSize:               dc.GetIntProperty(dynamicconfig.EventsCacheInitialSize, 128),
		EventsCacheMaxSize:                   dc.GetIntProperty(dynamicconfig.EventsCacheMaxSize, 512),
		EventsCacheTTL:                       dc.GetDurationProperty(dynamicconfig.EventsCacheTTL, time.Hour),
		RangeSizeBits:                        20, // 20 bits for sequencer, 2^20 sequence number for any range
		AcquireShardInterval:                 dc.GetDurationProperty(dynamicconfig.AcquireShardInterval, time.Minute),
		AcquireShardConcurrency:              dc.GetIntProperty(dynamicconfig.AcquireShardConcurrency, 10),
		StandbyClusterDelay:                  dc.GetDurationProperty(dynamicconfig.StandbyClusterDelay, 5*time.Minute),
		StandbyTaskMissingEventsResendDelay:  dc.GetDurationPropertyFilteredByTaskType(dynamicconfig.StandbyTaskMissingEventsResendDelay, 10*time.Minute),
		StandbyTaskMissingEventsDiscardDelay: dc.GetDurationPropertyFilteredByTaskType(dynamicconfig.StandbyTaskMissingEventsDiscardDelay, 15*time.Minute),

		QueuePendingTaskCriticalCount:    dc.GetIntProperty(dynamicconfig.QueuePendingTaskCriticalCount, 9000),
		QueueReaderStuckCriticalAttempts: dc.GetIntProperty(dynamicconfig.QueueReaderStuckCriticalAttempts, 3),
		QueueCriticalSlicesCount:         dc.GetIntProperty(dynamicconfig.QueueCriticalSlicesCount, 50),
		QueuePendingTaskMaxCount:         dc.GetIntProperty(dynamicconfig.QueuePendingTaskMaxCount, 10000),
		QueueMaxReaderCount:              dc.GetIntProperty(dynamicconfig.QueueMaxReaderCount, 2),

		TaskSchedulerEnableRateLimiter: dc.GetBoolProperty(dynamicconfig.TaskSchedulerEnableRateLimiter, false),
		TaskSchedulerMaxQPS:            dc.GetIntProperty(dynamicconfig.TaskSchedulerMaxQPS, 0),
		TaskSchedulerNamespaceMaxQPS:   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.TaskSchedulerNamespaceMaxQPS, 0),

		TimerTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.TimerTaskBatchSize, 100),
		TimerTaskWorkerCount:                             dc.GetIntProperty(dynamicconfig.TimerTaskWorkerCount, 10),
		TimerTaskMaxRetryCount:                           dc.GetIntProperty(dynamicconfig.TimerTaskMaxRetryCount, 20),
		TimerProcessorEnableSingleProcessor:              dc.GetBoolProperty(dynamicconfig.TimerProcessorEnableSingleProcessor, false),
		TimerProcessorEnableMultiCursor:                  dc.GetBoolProperty(dynamicconfig.TimerProcessorEnableMultiCursor, true),
		TimerProcessorEnablePriorityTaskScheduler:        dc.GetBoolProperty(dynamicconfig.TimerProcessorEnablePriorityTaskScheduler, true),
		TimerProcessorSchedulerWorkerCount:               dc.GetIntProperty(dynamicconfig.TimerProcessorSchedulerWorkerCount, 512),
		TimerProcessorSchedulerActiveRoundRobinWeights:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TimerProcessorSchedulerActiveRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		TimerProcessorSchedulerStandbyRoundRobinWeights:  dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TimerProcessorSchedulerStandbyRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)),
		TimerProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.TimerProcessorUpdateAckInterval, 30*time.Second),
		TimerProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TimerProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TimerProcessorCompleteTimerInterval:              dc.GetDurationProperty(dynamicconfig.TimerProcessorCompleteTimerInterval, 60*time.Second),
		TimerProcessorFailoverMaxPollRPS:                 dc.GetIntProperty(dynamicconfig.TimerProcessorFailoverMaxPollRPS, 1),
		TimerProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.TimerProcessorMaxPollRPS, 20),
		TimerProcessorMaxPollHostRPS:                     dc.GetIntProperty(dynamicconfig.TimerProcessorMaxPollHostRPS, 0),
		TimerProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxPollInterval, 5*time.Minute),
		TimerProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.TimerProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TimerProcessorMaxReschedulerSize:                 dc.GetIntProperty(dynamicconfig.TimerProcessorMaxReschedulerSize, 10000),
		TimerProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.TimerProcessorPollBackoffInterval, 5*time.Second),
		TimerProcessorMaxTimeShift:                       dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxTimeShift, 1*time.Second),
		TimerProcessorHistoryArchivalSizeLimit:           dc.GetIntProperty(dynamicconfig.TimerProcessorHistoryArchivalSizeLimit, 500*1024),
		TimerProcessorArchivalTimeLimit:                  dc.GetDurationProperty(dynamicconfig.TimerProcessorArchivalTimeLimit, 1*time.Second),
		RetentionTimerJitterDuration:                     dc.GetDurationProperty(dynamicconfig.RetentionTimerJitterDuration, 30*time.Minute),

		TransferTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.TransferTaskBatchSize, 100),
		TransferTaskWorkerCount:                             dc.GetIntProperty(dynamicconfig.TransferTaskWorkerCount, 10),
		TransferTaskMaxRetryCount:                           dc.GetIntProperty(dynamicconfig.TransferTaskMaxRetryCount, 20),
		TransferProcessorEnableSingleProcessor:              dc.GetBoolProperty(dynamicconfig.TransferProcessorEnableSingleProcessor, false),
		TransferProcessorEnableMultiCursor:                  dc.GetBoolProperty(dynamicconfig.TransferProcessorEnableMultiCursor, true),
		TransferProcessorEnablePriorityTaskScheduler:        dc.GetBoolProperty(dynamicconfig.TransferProcessorEnablePriorityTaskScheduler, true),
		TransferProcessorSchedulerWorkerCount:               dc.GetIntProperty(dynamicconfig.TransferProcessorSchedulerWorkerCount, 512),
		TransferProcessorSchedulerActiveRoundRobinWeights:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TransferProcessorSchedulerActiveRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		TransferProcessorSchedulerStandbyRoundRobinWeights:  dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.TransferProcessorSchedulerStandbyRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)),
		TransferProcessorFailoverMaxPollRPS:                 dc.GetIntProperty(dynamicconfig.TransferProcessorFailoverMaxPollRPS, 1),
		TransferProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.TransferProcessorMaxPollRPS, 20),
		TransferProcessorMaxPollHostRPS:                     dc.GetIntProperty(dynamicconfig.TransferProcessorMaxPollHostRPS, 0),
		TransferProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.TransferProcessorMaxPollInterval, 1*time.Minute),
		TransferProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TransferProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.TransferProcessorUpdateAckInterval, 30*time.Second),
		TransferProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TransferProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TransferProcessorCompleteTransferInterval:           dc.GetDurationProperty(dynamicconfig.TransferProcessorCompleteTransferInterval, 60*time.Second),
		TransferProcessorMaxReschedulerSize:                 dc.GetIntProperty(dynamicconfig.TransferProcessorMaxReschedulerSize, 10000),
		TransferProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.TransferProcessorPollBackoffInterval, 5*time.Second),
		TransferProcessorVisibilityArchivalTimeLimit:        dc.GetDurationProperty(dynamicconfig.TransferProcessorVisibilityArchivalTimeLimit, 200*time.Millisecond),
		TransferProcessorEnsureCloseBeforeDelete:            dc.GetBoolProperty(dynamicconfig.TransferProcessorEnsureCloseBeforeDelete, true),

		ReplicatorTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 100),
		ReplicatorTaskWorkerCount:                             dc.GetIntProperty(dynamicconfig.ReplicatorTaskWorkerCount, 10),
		ReplicatorTaskMaxRetryCount:                           dc.GetIntProperty(dynamicconfig.ReplicatorTaskMaxRetryCount, 100),
		ReplicatorProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.ReplicatorProcessorMaxPollRPS, 20),
		ReplicatorProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorMaxPollInterval, 1*time.Minute),
		ReplicatorProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorMaxPollIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorUpdateAckInterval, 5*time.Second),
		ReplicatorProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorMaxReschedulerSize:                 dc.GetIntProperty(dynamicconfig.ReplicatorProcessorMaxReschedulerSize, 10000),
		ReplicatorProcessorEnablePriorityTaskProcessor:        dc.GetBoolProperty(dynamicconfig.ReplicatorProcessorEnablePriorityTaskProcessor, false),
		ReplicatorProcessorFetchTasksBatchSize:                dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 25),
		ReplicationTaskProcessorHostQPS:                       dc.GetFloat64Property(dynamicconfig.ReplicationTaskProcessorHostQPS, 1500),
		ReplicationTaskProcessorShardQPS:                      dc.GetFloat64Property(dynamicconfig.ReplicationTaskProcessorShardQPS, 30),

		MaximumBufferedEventsBatch:      dc.GetIntProperty(dynamicconfig.MaximumBufferedEventsBatch, 100),
		MaximumSignalsPerExecution:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MaximumSignalsPerExecution, 0),
		ShardUpdateMinInterval:          dc.GetDurationProperty(dynamicconfig.ShardUpdateMinInterval, 5*time.Minute),
		ShardSyncMinInterval:            dc.GetDurationProperty(dynamicconfig.ShardSyncMinInterval, 5*time.Minute),
		ShardSyncTimerJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient, 0.15),

		// history client: client/history/client.go set the client timeout 30s
		// TODO: Return this value to the client: go.temporal.io/server/issues/294
		LongPollExpirationInterval:          dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.HistoryLongPollExpirationInterval, time.Second*20),
		EventEncodingType:                   dc.GetStringPropertyFnWithNamespaceFilter(dynamicconfig.DefaultEventEncoding, enumspb.ENCODING_TYPE_PROTO3.String()),
		EnableParentClosePolicy:             dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableParentClosePolicy, true),
		NumParentClosePolicySystemWorkflows: dc.GetIntProperty(dynamicconfig.NumParentClosePolicySystemWorkflows, 10),
		EnableParentClosePolicyWorker:       dc.GetBoolProperty(dynamicconfig.EnableParentClosePolicyWorker, true),
		ParentClosePolicyThreshold:          dc.GetIntPropertyFilteredByNamespace(dynamicconfig.ParentClosePolicyThreshold, 10),

		NumArchiveSystemWorkflows: dc.GetIntProperty(dynamicconfig.NumArchiveSystemWorkflows, 1000),
		ArchiveRequestRPS:         dc.GetIntProperty(dynamicconfig.ArchiveRequestRPS, 300), // should be much smaller than frontend RPS
		ArchiveSignalTimeout:      dc.GetDurationProperty(dynamicconfig.ArchiveSignalTimeout, 300*time.Millisecond),
		DurableArchivalEnabled: func() bool {
			// Always return false for now until durable archival is tested end-to-end
			return false
		},

		BlobSizeLimitError:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:              dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitWarn, 512*1024),
		MemoSizeLimitError:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MemoSizeLimitError, 2*1024*1024),
		MemoSizeLimitWarn:              dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MemoSizeLimitWarn, 2*1024),
		NumPendingChildExecutionsLimit: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingChildExecutionsLimitError, 50000),
		NumPendingActivitiesLimit:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingActivitiesLimitError, 50000),
		NumPendingSignalsLimit:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingSignalsLimitError, 50000),
		NumPendingCancelsRequestLimit:  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.NumPendingCancelRequestsLimitError, 50000),
		HistorySizeLimitError:          dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeLimitError, 50*1024*1024),
		HistorySizeLimitWarn:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeLimitWarn, 10*1024*1024),
		HistoryCountLimitError:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountLimitError, 50*1024),
		HistoryCountLimitWarn:          dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountLimitWarn, 10*1024),

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
		VisibilityTaskWorkerCount:                             dc.GetIntProperty(dynamicconfig.VisibilityTaskWorkerCount, 10),
		VisibilityTaskMaxRetryCount:                           dc.GetIntProperty(dynamicconfig.VisibilityTaskMaxRetryCount, 20),
		VisibilityProcessorEnableMultiCursor:                  dc.GetBoolProperty(dynamicconfig.VisibilityProcessorEnableMultiCursor, true),
		VisibilityProcessorEnablePriorityTaskScheduler:        dc.GetBoolProperty(dynamicconfig.VisibilityProcessorEnablePriorityTaskScheduler, true),
		VisibilityProcessorSchedulerWorkerCount:               dc.GetIntProperty(dynamicconfig.VisibilityProcessorSchedulerWorkerCount, 512),
		VisibilityProcessorSchedulerActiveRoundRobinWeights:   dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityProcessorSchedulerActiveRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		VisibilityProcessorSchedulerStandbyRoundRobinWeights:  dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityProcessorSchedulerStandbyRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultStandbyTaskPriorityWeight)),
		VisibilityProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.VisibilityProcessorMaxPollInterval, 1*time.Minute),
		VisibilityProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.VisibilityProcessorMaxPollIntervalJitterCoefficient, 0.15),
		VisibilityProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.VisibilityProcessorUpdateAckInterval, 30*time.Second),
		VisibilityProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.VisibilityProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		VisibilityProcessorCompleteTaskInterval:               dc.GetDurationProperty(dynamicconfig.VisibilityProcessorCompleteTaskInterval, 60*time.Second),
		VisibilityProcessorMaxReschedulerSize:                 dc.GetIntProperty(dynamicconfig.VisibilityProcessorMaxReschedulerSize, 10000),
		VisibilityProcessorPollBackoffInterval:                dc.GetDurationProperty(dynamicconfig.VisibilityProcessorPollBackoffInterval, 5*time.Second),
		VisibilityProcessorVisibilityArchivalTimeLimit:        dc.GetDurationProperty(dynamicconfig.VisibilityProcessorVisibilityArchivalTimeLimit, 200*time.Millisecond),
		VisibilityProcessorEnsureCloseBeforeDelete:            dc.GetBoolProperty(dynamicconfig.VisibilityProcessorEnsureCloseBeforeDelete, false),
		VisibilityProcessorEnableCloseWorkflowCleanup:         dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityProcessorEnableCloseWorkflowCleanup, false),

		SearchAttributesNumberOfKeysLimit: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		IndexerConcurrency:                dc.GetIntProperty(dynamicconfig.WorkerIndexerConcurrency, 100),
		ESProcessorNumOfWorkers:           dc.GetIntProperty(dynamicconfig.WorkerESProcessorNumOfWorkers, 1),
		// Should be not greater than NumberOfShards(512)/NumberOfHistoryNodes(4) * VisibilityTaskWorkerCount(10)/ESProcessorNumOfWorkers(1) divided by workflow distribution factor (2 at least).
		// Otherwise, visibility queue processors won't be able to fill up bulk with documents (even under heavy load) and bulk will flush due to interval, not number of actions.
		ESProcessorBulkActions: dc.GetIntProperty(dynamicconfig.WorkerESProcessorBulkActions, 500),
		// 16MB - just a sanity check. With ES document size ~1Kb it should never be reached.
		ESProcessorBulkSize: dc.GetIntProperty(dynamicconfig.WorkerESProcessorBulkSize, 16*1024*1024),
		// Bulk processor will flush every this interval regardless of last flush due to bulk actions.
		ESProcessorFlushInterval: dc.GetDurationProperty(dynamicconfig.WorkerESProcessorFlushInterval, 1*time.Second),
		ESProcessorAckTimeout:    dc.GetDurationProperty(dynamicconfig.WorkerESProcessorAckTimeout, 1*time.Minute),

		EnableCrossNamespaceCommands:  dc.GetBoolProperty(dynamicconfig.EnableCrossNamespaceCommands, true),
		EnableActivityEagerExecution:  dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableActivityEagerExecution, false),
		NamespaceCacheRefreshInterval: dc.GetDurationProperty(dynamicconfig.NamespaceCacheRefreshInterval, 10*time.Second),

		// Archival related
		ArchivalTaskBatchSize:                       dc.GetIntProperty(dynamicconfig.ArchivalTaskBatchSize, 100),
		ArchivalProcessorMaxPollRPS:                 dc.GetIntProperty(dynamicconfig.ArchivalProcessorMaxPollRPS, 20),
		ArchivalProcessorMaxPollHostRPS:             dc.GetIntProperty(dynamicconfig.ArchivalProcessorMaxPollHostRPS, 0),
		ArchivalProcessorSchedulerWorkerCount:       dc.GetIntProperty(dynamicconfig.ArchivalProcessorSchedulerWorkerCount, 512),
		ArchivalProcessorSchedulerRoundRobinWeights: dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.ArchivalProcessorSchedulerRoundRobinWeights, ConvertWeightsToDynamicConfigValue(DefaultActiveTaskPriorityWeight)),
		ArchivalProcessorMaxPollInterval:            dc.GetDurationProperty(dynamicconfig.ArchivalProcessorMaxPollInterval, 1*time.Minute),
		ArchivalProcessorMaxPollIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.
			ArchivalProcessorMaxPollIntervalJitterCoefficient, 0.15),
		ArchivalProcessorUpdateAckInterval: dc.GetDurationProperty(dynamicconfig.ArchivalProcessorUpdateAckInterval, 30*time.Second),
		ArchivalProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.
			ArchivalProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		ArchivalProcessorPollBackoffInterval: dc.GetDurationProperty(dynamicconfig.ArchivalProcessorPollBackoffInterval, 5*time.Second),
	}

	return cfg
}

// GetShardID return the corresponding shard ID for a given namespaceID and workflowID pair
func (config *Config) GetShardID(namespaceID namespace.ID, workflowID string) int32 {
	return common.WorkflowIDToHistoryShard(namespaceID.String(), workflowID, config.NumberOfShards)
}
