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

package history

import (
	"context"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	espersistence "go.temporal.io/server/common/persistence/elasticsearch"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/common/task"
)

// Config represents configuration for history service
type Config struct {
	NumberOfShards int32

	RPS                           dynamicconfig.IntPropertyFn
	MaxIDLengthLimit              dynamicconfig.IntPropertyFn
	PersistenceMaxQPS             dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS       dynamicconfig.IntPropertyFn
	EnableVisibilitySampling      dynamicconfig.BoolPropertyFn
	VisibilityOpenMaxQPS          dynamicconfig.IntPropertyFnWithNamespaceFilter
	VisibilityClosedMaxQPS        dynamicconfig.IntPropertyFnWithNamespaceFilter
	AdvancedVisibilityWritingMode dynamicconfig.StringPropertyFn
	EmitShardDiffLog              dynamicconfig.BoolPropertyFn
	MaxAutoResetPoints            dynamicconfig.IntPropertyFnWithNamespaceFilter
	ThrottledLogRPS               dynamicconfig.IntPropertyFn
	EnableStickyQuery             dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration         dynamicconfig.DurationPropertyFn

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
	StandbyTaskMissingEventsResendDelay  dynamicconfig.DurationPropertyFn
	StandbyTaskMissingEventsDiscardDelay dynamicconfig.DurationPropertyFn

	// Task process settings
	TaskProcessRPS                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnablePriorityTaskProcessor    dynamicconfig.BoolPropertyFn
	TaskSchedulerType              dynamicconfig.IntPropertyFn
	TaskSchedulerWorkerCount       dynamicconfig.IntPropertyFn
	TaskSchedulerQueueSize         dynamicconfig.IntPropertyFn
	TaskSchedulerRoundRobinWeights dynamicconfig.MapPropertyFn

	// TimerQueueProcessor settings
	TimerTaskBatchSize                                dynamicconfig.IntPropertyFn
	TimerTaskWorkerCount                              dynamicconfig.IntPropertyFn
	TimerTaskMaxRetryCount                            dynamicconfig.IntPropertyFn
	TimerProcessorGetFailureRetryCount                dynamicconfig.IntPropertyFn
	TimerProcessorCompleteTimerFailureRetryCount      dynamicconfig.IntPropertyFn
	TimerProcessorUpdateAckInterval                   dynamicconfig.DurationPropertyFn
	TimerProcessorUpdateAckIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
	TimerProcessorCompleteTimerInterval               dynamicconfig.DurationPropertyFn
	TimerProcessorFailoverMaxPollRPS                  dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollRPS                          dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollInterval                     dynamicconfig.DurationPropertyFn
	TimerProcessorMaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
	TimerProcessorRedispatchInterval                  dynamicconfig.DurationPropertyFn
	TimerProcessorRedispatchIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TimerProcessorMaxRedispatchQueueSize              dynamicconfig.IntPropertyFn
	TimerProcessorEnablePriorityTaskProcessor         dynamicconfig.BoolPropertyFn
	TimerProcessorMaxTimeShift                        dynamicconfig.DurationPropertyFn
	TimerProcessorHistoryArchivalSizeLimit            dynamicconfig.IntPropertyFn
	TimerProcessorArchivalTimeLimit                   dynamicconfig.DurationPropertyFn

	// TransferQueueProcessor settings
	TransferTaskBatchSize                                dynamicconfig.IntPropertyFn
	TransferTaskWorkerCount                              dynamicconfig.IntPropertyFn
	TransferTaskMaxRetryCount                            dynamicconfig.IntPropertyFn
	TransferProcessorCompleteTransferFailureRetryCount   dynamicconfig.IntPropertyFn
	TransferProcessorFailoverMaxPollRPS                  dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollRPS                          dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollInterval                     dynamicconfig.DurationPropertyFn
	TransferProcessorMaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
	TransferProcessorUpdateAckInterval                   dynamicconfig.DurationPropertyFn
	TransferProcessorUpdateAckIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
	TransferProcessorCompleteTransferInterval            dynamicconfig.DurationPropertyFn
	TransferProcessorRedispatchInterval                  dynamicconfig.DurationPropertyFn
	TransferProcessorRedispatchIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TransferProcessorMaxRedispatchQueueSize              dynamicconfig.IntPropertyFn
	TransferProcessorEnablePriorityTaskProcessor         dynamicconfig.BoolPropertyFn
	TransferProcessorVisibilityArchivalTimeLimit         dynamicconfig.DurationPropertyFn

	// ReplicatorQueueProcessor settings
	ReplicatorTaskBatchSize                                dynamicconfig.IntPropertyFn
	ReplicatorTaskWorkerCount                              dynamicconfig.IntPropertyFn
	ReplicatorTaskMaxRetryCount                            dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxPollRPS                          dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxPollInterval                     dynamicconfig.DurationPropertyFn
	ReplicatorProcessorMaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
	ReplicatorProcessorUpdateAckInterval                   dynamicconfig.DurationPropertyFn
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
	ReplicatorProcessorRedispatchInterval                  dynamicconfig.DurationPropertyFn
	ReplicatorProcessorRedispatchIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	ReplicatorProcessorMaxRedispatchQueueSize              dynamicconfig.IntPropertyFn
	ReplicatorProcessorEnablePriorityTaskProcessor         dynamicconfig.BoolPropertyFn
	ReplicatorProcessorFetchTasksBatchSize                 dynamicconfig.IntPropertyFn

	// Persistence settings
	ExecutionMgrNumConns dynamicconfig.IntPropertyFn
	HistoryMgrNumConns   dynamicconfig.IntPropertyFn

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

	// Size limit related settings
	BlobSizeLimitError     dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn      dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeLimitError  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistorySizeLimitWarn   dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	HistoryCountLimitWarn  dynamicconfig.IntPropertyFnWithNamespaceFilter

	// ValidSearchAttributes is legal indexed keys that can be used in list APIs
	ValidSearchAttributes             dynamicconfig.MapPropertyFn
	SearchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	SearchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
	SearchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter

	// DefaultActivityRetryOptions specifies the out-of-box retry policy if
	// none is configured on the Activity by the user.
	DefaultActivityRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// DefaultWorkflowRetryPolicy specifies the out-of-box retry policy for
	// any unset fields on a RetryPolicy configured on a Workflow
	DefaultWorkflowRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// Workflow task settings
	// StickyTTL is to expire a sticky taskqueue if no update more than this duration
	// TODO https://go.temporal.io/server/issues/2357
	StickyTTL dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// DefaultWorkflowTaskTimeout the default workflow task timeout
	DefaultWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// WorkflowTaskHeartbeatTimeout is to timeout behavior of: RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true without any workflow tasks
	// So that workflow task will be scheduled to another worker(by clear stickyness)
	WorkflowTaskHeartbeatTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// The execution timeout a workflow execution defaults to if not specified
	DefaultWorkflowExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// The run timeout a workflow run defaults to if not specified
	DefaultWorkflowRunTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// Maximum workflow execution timeout permitted by the service
	MaxWorkflowExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// Maximum workflow run timeout permitted by the service
	MaxWorkflowRunTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// MaxWorkflowTaskTimeout is the maximum allowed value for a workflow task timeout
	MaxWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// The following is used by the new RPC replication stack
	ReplicationTaskFetcherParallelism                  dynamicconfig.IntPropertyFn
	ReplicationTaskFetcherAggregationInterval          dynamicconfig.DurationPropertyFn
	ReplicationTaskFetcherTimerJitterCoefficient       dynamicconfig.FloatPropertyFn
	ReplicationTaskFetcherErrorRetryWait               dynamicconfig.DurationPropertyFn
	ReplicationTaskProcessorErrorRetryWait             dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorErrorRetryMaxAttempts      dynamicconfig.IntPropertyFnWithShardIDFilter
	ReplicationTaskProcessorNoTaskRetryWait            dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorCleanupInterval            dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorCleanupJitterCoefficient   dynamicconfig.FloatPropertyFnWithShardIDFilter
	ReplicationTaskProcessorStartWait                  dynamicconfig.DurationPropertyFnWithShardIDFilter
	ReplicationTaskProcessorStartWaitJitterCoefficient dynamicconfig.FloatPropertyFnWithShardIDFilter
	ReplicationTaskProcessorHostQPS                    dynamicconfig.FloatPropertyFn
	ReplicationTaskProcessorShardQPS                   dynamicconfig.FloatPropertyFn

	EnableKafkaReplication       dynamicconfig.BoolPropertyFn
	EnableRPCReplication         dynamicconfig.BoolPropertyFn
	EnableCleanupReplicationTask dynamicconfig.BoolPropertyFn

	// The following are used by consistent query
	MaxBufferedQueryCount dynamicconfig.IntPropertyFn

	// Data integrity check related config knobs
	MutableStateChecksumGenProbability    dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateChecksumVerifyProbability dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateChecksumInvalidateBefore  dynamicconfig.FloatPropertyFn

	// Crocess DC Replication configuration
	ReplicationEventsFromCurrentCluster    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	StandbyTaskReReplicationContextTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter

	EnableDropStuckTaskByNamespaceID dynamicconfig.BoolPropertyFnWithNamespaceIDFilter
	SkipReapplicationByNamespaceId   dynamicconfig.BoolPropertyFnWithNamespaceIDFilter
}

const (
	defaultHistoryMaxAutoResetPoints = 20
)

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numberOfShards int32, isAdvancedVisConfigExist bool) *Config {
	cfg := &Config{
		NumberOfShards:                       numberOfShards,
		RPS:                                  dc.GetIntProperty(dynamicconfig.HistoryRPS, 3000),
		MaxIDLengthLimit:                     dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		PersistenceMaxQPS:                    dc.GetIntProperty(dynamicconfig.HistoryPersistenceMaxQPS, 9000),
		PersistenceGlobalMaxQPS:              dc.GetIntProperty(dynamicconfig.HistoryPersistenceGlobalMaxQPS, 0),
		ShutdownDrainDuration:                dc.GetDurationProperty(dynamicconfig.HistoryShutdownDrainDuration, 0),
		EnableVisibilitySampling:             dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		VisibilityOpenMaxQPS:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryVisibilityOpenMaxQPS, 300),
		VisibilityClosedMaxQPS:               dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryVisibilityClosedMaxQPS, 300),
		MaxAutoResetPoints:                   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryMaxAutoResetPoints, defaultHistoryMaxAutoResetPoints),
		DefaultWorkflowTaskTimeout:           dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		MaxWorkflowTaskTimeout:               dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.MaxWorkflowTaskTimeout, time.Second*60),
		AdvancedVisibilityWritingMode:        dc.GetStringProperty(dynamicconfig.AdvancedVisibilityWritingMode, common.GetDefaultAdvancedVisibilityWritingMode(isAdvancedVisConfigExist)),
		EmitShardDiffLog:                     dc.GetBoolProperty(dynamicconfig.EmitShardDiffLog, false),
		HistoryCacheInitialSize:              dc.GetIntProperty(dynamicconfig.HistoryCacheInitialSize, 128),
		HistoryCacheMaxSize:                  dc.GetIntProperty(dynamicconfig.HistoryCacheMaxSize, 512),
		HistoryCacheTTL:                      dc.GetDurationProperty(dynamicconfig.HistoryCacheTTL, time.Hour),
		EventsCacheInitialSize:               dc.GetIntProperty(dynamicconfig.EventsCacheInitialSize, 128),
		EventsCacheMaxSize:                   dc.GetIntProperty(dynamicconfig.EventsCacheMaxSize, 512),
		EventsCacheTTL:                       dc.GetDurationProperty(dynamicconfig.EventsCacheTTL, time.Hour),
		RangeSizeBits:                        20, // 20 bits for sequencer, 2^20 sequence number for any range
		AcquireShardInterval:                 dc.GetDurationProperty(dynamicconfig.AcquireShardInterval, time.Minute),
		AcquireShardConcurrency:              dc.GetIntProperty(dynamicconfig.AcquireShardConcurrency, 1),
		StandbyClusterDelay:                  dc.GetDurationProperty(dynamicconfig.StandbyClusterDelay, 5*time.Minute),
		StandbyTaskMissingEventsResendDelay:  dc.GetDurationProperty(dynamicconfig.StandbyTaskMissingEventsResendDelay, 10*time.Minute),
		StandbyTaskMissingEventsDiscardDelay: dc.GetDurationProperty(dynamicconfig.StandbyTaskMissingEventsDiscardDelay, 15*time.Minute),

		TaskProcessRPS: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.TaskProcessRPS, 1000),

		EnablePriorityTaskProcessor:    dc.GetBoolProperty(dynamicconfig.EnablePriorityTaskProcessor, false),
		TaskSchedulerType:              dc.GetIntProperty(dynamicconfig.TaskSchedulerType, int(task.SchedulerTypeWRR)),
		TaskSchedulerWorkerCount:       dc.GetIntProperty(dynamicconfig.TaskSchedulerWorkerCount, 20),
		TaskSchedulerQueueSize:         dc.GetIntProperty(dynamicconfig.TaskSchedulerQueueSize, 2000),
		TaskSchedulerRoundRobinWeights: dc.GetMapProperty(dynamicconfig.TaskSchedulerRoundRobinWeights, convertWeightsToDynamicConfigValue(defaultTaskPriorityWeight)),

		TimerTaskBatchSize:                                dc.GetIntProperty(dynamicconfig.TimerTaskBatchSize, 100),
		TimerTaskWorkerCount:                              dc.GetIntProperty(dynamicconfig.TimerTaskWorkerCount, 10),
		TimerTaskMaxRetryCount:                            dc.GetIntProperty(dynamicconfig.TimerTaskMaxRetryCount, 100),
		TimerProcessorGetFailureRetryCount:                dc.GetIntProperty(dynamicconfig.TimerProcessorGetFailureRetryCount, 5),
		TimerProcessorCompleteTimerFailureRetryCount:      dc.GetIntProperty(dynamicconfig.TimerProcessorCompleteTimerFailureRetryCount, 10),
		TimerProcessorUpdateAckInterval:                   dc.GetDurationProperty(dynamicconfig.TimerProcessorUpdateAckInterval, 30*time.Second),
		TimerProcessorUpdateAckIntervalJitterCoefficient:  dc.GetFloat64Property(dynamicconfig.TimerProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TimerProcessorCompleteTimerInterval:               dc.GetDurationProperty(dynamicconfig.TimerProcessorCompleteTimerInterval, 60*time.Second),
		TimerProcessorFailoverMaxPollRPS:                  dc.GetIntProperty(dynamicconfig.TimerProcessorFailoverMaxPollRPS, 1),
		TimerProcessorMaxPollRPS:                          dc.GetIntProperty(dynamicconfig.TimerProcessorMaxPollRPS, 20),
		TimerProcessorMaxPollInterval:                     dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxPollInterval, 5*time.Minute),
		TimerProcessorMaxPollIntervalJitterCoefficient:    dc.GetFloat64Property(dynamicconfig.TimerProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TimerProcessorRedispatchInterval:                  dc.GetDurationProperty(dynamicconfig.TimerProcessorRedispatchInterval, 5*time.Second),
		TimerProcessorRedispatchIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TimerProcessorRedispatchIntervalJitterCoefficient, 0.15),
		TimerProcessorMaxRedispatchQueueSize:              dc.GetIntProperty(dynamicconfig.TimerProcessorMaxRedispatchQueueSize, 10000),
		TimerProcessorEnablePriorityTaskProcessor:         dc.GetBoolProperty(dynamicconfig.TimerProcessorEnablePriorityTaskProcessor, false),
		TimerProcessorMaxTimeShift:                        dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxTimeShift, 1*time.Second),
		TimerProcessorHistoryArchivalSizeLimit:            dc.GetIntProperty(dynamicconfig.TimerProcessorHistoryArchivalSizeLimit, 500*1024),
		TimerProcessorArchivalTimeLimit:                   dc.GetDurationProperty(dynamicconfig.TimerProcessorArchivalTimeLimit, 1*time.Second),

		TransferTaskBatchSize:                                dc.GetIntProperty(dynamicconfig.TransferTaskBatchSize, 100),
		TransferProcessorFailoverMaxPollRPS:                  dc.GetIntProperty(dynamicconfig.TransferProcessorFailoverMaxPollRPS, 1),
		TransferProcessorMaxPollRPS:                          dc.GetIntProperty(dynamicconfig.TransferProcessorMaxPollRPS, 20),
		TransferTaskWorkerCount:                              dc.GetIntProperty(dynamicconfig.TransferTaskWorkerCount, 10),
		TransferTaskMaxRetryCount:                            dc.GetIntProperty(dynamicconfig.TransferTaskMaxRetryCount, 100),
		TransferProcessorCompleteTransferFailureRetryCount:   dc.GetIntProperty(dynamicconfig.TransferProcessorCompleteTransferFailureRetryCount, 10),
		TransferProcessorMaxPollInterval:                     dc.GetDurationProperty(dynamicconfig.TransferProcessorMaxPollInterval, 1*time.Minute),
		TransferProcessorMaxPollIntervalJitterCoefficient:    dc.GetFloat64Property(dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TransferProcessorUpdateAckInterval:                   dc.GetDurationProperty(dynamicconfig.TransferProcessorUpdateAckInterval, 30*time.Second),
		TransferProcessorUpdateAckIntervalJitterCoefficient:  dc.GetFloat64Property(dynamicconfig.TransferProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TransferProcessorCompleteTransferInterval:            dc.GetDurationProperty(dynamicconfig.TransferProcessorCompleteTransferInterval, 60*time.Second),
		TransferProcessorRedispatchInterval:                  dc.GetDurationProperty(dynamicconfig.TransferProcessorRedispatchInterval, 5*time.Second),
		TransferProcessorRedispatchIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.TransferProcessorRedispatchIntervalJitterCoefficient, 0.15),
		TransferProcessorMaxRedispatchQueueSize:              dc.GetIntProperty(dynamicconfig.TransferProcessorMaxRedispatchQueueSize, 10000),
		TransferProcessorEnablePriorityTaskProcessor:         dc.GetBoolProperty(dynamicconfig.TransferProcessorEnablePriorityTaskProcessor, false),
		TransferProcessorVisibilityArchivalTimeLimit:         dc.GetDurationProperty(dynamicconfig.TransferProcessorVisibilityArchivalTimeLimit, 200*time.Millisecond),

		ReplicatorTaskBatchSize:                                dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 100),
		ReplicatorTaskWorkerCount:                              dc.GetIntProperty(dynamicconfig.ReplicatorTaskWorkerCount, 10),
		ReplicatorTaskMaxRetryCount:                            dc.GetIntProperty(dynamicconfig.ReplicatorTaskMaxRetryCount, 100),
		ReplicatorProcessorMaxPollRPS:                          dc.GetIntProperty(dynamicconfig.ReplicatorProcessorMaxPollRPS, 20),
		ReplicatorProcessorMaxPollInterval:                     dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorMaxPollInterval, 1*time.Minute),
		ReplicatorProcessorMaxPollIntervalJitterCoefficient:    dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorMaxPollIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorUpdateAckInterval:                   dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorUpdateAckInterval, 5*time.Second),
		ReplicatorProcessorUpdateAckIntervalJitterCoefficient:  dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorRedispatchInterval:                  dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorRedispatchInterval, 5*time.Second),
		ReplicatorProcessorRedispatchIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorRedispatchIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorMaxRedispatchQueueSize:              dc.GetIntProperty(dynamicconfig.ReplicatorProcessorMaxRedispatchQueueSize, 10000),
		ReplicatorProcessorEnablePriorityTaskProcessor:         dc.GetBoolProperty(dynamicconfig.ReplicatorProcessorEnablePriorityTaskProcessor, false),
		ReplicatorProcessorFetchTasksBatchSize:                 dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 25),
		ReplicationTaskProcessorStartWait:                      dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorStartWait, 5*time.Second),
		ReplicationTaskProcessorStartWaitJitterCoefficient:     dc.GetFloat64PropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorStartWaitJitterCoefficient, 0.9),
		ReplicationTaskProcessorHostQPS:                        dc.GetFloat64Property(dynamicconfig.ReplicationTaskProcessorHostQPS, 1500),
		ReplicationTaskProcessorShardQPS:                       dc.GetFloat64Property(dynamicconfig.ReplicationTaskProcessorShardQPS, 5),

		ExecutionMgrNumConns:            dc.GetIntProperty(dynamicconfig.ExecutionMgrNumConns, 50),
		HistoryMgrNumConns:              dc.GetIntProperty(dynamicconfig.HistoryMgrNumConns, 50),
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

		BlobSizeLimitError:     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitWarn, 512*1024),
		HistorySizeLimitError:  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeLimitError, 50*1024*1024),
		HistorySizeLimitWarn:   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistorySizeLimitWarn, 10*1024*1024),
		HistoryCountLimitError: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountLimitError, 50*1024),
		HistoryCountLimitWarn:  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.HistoryCountLimitWarn, 10*1024),

		ThrottledLogRPS:   dc.GetIntProperty(dynamicconfig.HistoryThrottledLogRPS, 4),
		EnableStickyQuery: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableStickyQuery, true),

		DefaultActivityRetryPolicy:                       dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultActivityRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		DefaultWorkflowRetryPolicy:                       dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultWorkflowRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		ValidSearchAttributes:                            dc.GetMapProperty(dynamicconfig.ValidSearchAttributes, definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit:                dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:                   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		StickyTTL:                                        dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.StickyTTL, time.Hour*24*365),
		WorkflowTaskHeartbeatTimeout:                     dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.WorkflowTaskHeartbeatTimeout, time.Minute*30),
		DefaultWorkflowExecutionTimeout:                  dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowExecutionTimeout, common.DefaultWorkflowExecutionTimeout),
		DefaultWorkflowRunTimeout:                        dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowRunTimeout, common.DefaultWorkflowRunTimeout),
		MaxWorkflowExecutionTimeout:                      dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.MaxWorkflowExecutionTimeout, common.DefaultWorkflowExecutionTimeout),
		MaxWorkflowRunTimeout:                            dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.MaxWorkflowRunTimeout, common.DefaultWorkflowRunTimeout),
		ReplicationTaskFetcherParallelism:                dc.GetIntProperty(dynamicconfig.ReplicationTaskFetcherParallelism, 1),
		ReplicationTaskFetcherAggregationInterval:        dc.GetDurationProperty(dynamicconfig.ReplicationTaskFetcherAggregationInterval, 2*time.Second),
		ReplicationTaskFetcherTimerJitterCoefficient:     dc.GetFloat64Property(dynamicconfig.ReplicationTaskFetcherTimerJitterCoefficient, 0.15),
		ReplicationTaskFetcherErrorRetryWait:             dc.GetDurationProperty(dynamicconfig.ReplicationTaskFetcherErrorRetryWait, time.Second),
		ReplicationTaskProcessorErrorRetryWait:           dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryWait, time.Second),
		ReplicationTaskProcessorErrorRetryMaxAttempts:    dc.GetIntPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts, 20),
		ReplicationTaskProcessorNoTaskRetryWait:          dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorNoTaskInitialWait, 2*time.Second),
		ReplicationTaskProcessorCleanupInterval:          dc.GetDurationPropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorCleanupInterval, 1*time.Minute),
		ReplicationTaskProcessorCleanupJitterCoefficient: dc.GetFloat64PropertyFilteredByShardID(dynamicconfig.ReplicationTaskProcessorCleanupJitterCoefficient, 0.15),
		EnableRPCReplication:                             dc.GetBoolProperty(dynamicconfig.HistoryEnableRPCReplication, false),
		EnableKafkaReplication:                           dc.GetBoolProperty(dynamicconfig.HistoryEnableKafkaReplication, true),
		EnableCleanupReplicationTask:                     dc.GetBoolProperty(dynamicconfig.HistoryEnableCleanupReplicationTask, true),

		MaxBufferedQueryCount:                 dc.GetIntProperty(dynamicconfig.MaxBufferedQueryCount, 1),
		MutableStateChecksumGenProbability:    dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MutableStateChecksumGenProbability, 0),
		MutableStateChecksumVerifyProbability: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MutableStateChecksumVerifyProbability, 0),
		MutableStateChecksumInvalidateBefore:  dc.GetFloat64Property(dynamicconfig.MutableStateChecksumInvalidateBefore, 0),

		ReplicationEventsFromCurrentCluster:    dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.ReplicationEventsFromCurrentCluster, false),
		StandbyTaskReReplicationContextTimeout: dc.GetDurationPropertyFilteredByNamespaceID(dynamicconfig.StandbyTaskReReplicationContextTimeout, 3*time.Minute),

		EnableDropStuckTaskByNamespaceID: dc.GetBoolPropertyFnWithNamespaceIDFilter(dynamicconfig.EnableDropStuckTaskByNamespaceID, false),
		SkipReapplicationByNamespaceId:   dc.GetBoolPropertyFnWithNamespaceIDFilter(dynamicconfig.SkipReapplicationByNamespaceId, false),
	}

	return cfg
}

// GetShardID return the corresponding shard ID for a given namespaceID and workflowID pair
func (config *Config) GetShardID(namespaceID, workflowID string) int32 {
	return common.WorkflowIDToHistoryShard(namespaceID, workflowID, config.NumberOfShards)
}

// Service represents the history service
type Service struct {
	resource.Resource

	status  int32
	handler *Handler
	params  *resource.BootstrapParams
	config  *Config

	server *grpc.Server
}

// NewService builds a new history service
func NewService(
	params *resource.BootstrapParams,
) (resource.Resource, error) {
	serviceConfig := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger),
		params.PersistenceConfig.NumHistoryShards,
		params.PersistenceConfig.IsAdvancedVisibilityConfigExist())

	params.PersistenceConfig.HistoryMaxConns = serviceConfig.HistoryMgrNumConns()
	params.PersistenceConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityOpenMaxQPS:   serviceConfig.VisibilityOpenMaxQPS,
		VisibilityClosedMaxQPS: serviceConfig.VisibilityClosedMaxQPS,
		EnableSampling:         serviceConfig.EnableVisibilitySampling,
	}

	visibilityManagerInitializer := func(
		persistenceBean persistenceClient.Bean,
		logger log.Logger,
	) (persistence.VisibilityManager, error) {
		visibilityFromDB := persistenceBean.GetVisibilityManager()

		var visibilityFromES persistence.VisibilityManager
		if params.ESConfig != nil {
			visibilityProducer, err := params.MessagingClient.NewProducer(common.VisibilityAppName)
			if err != nil {
				logger.Fatal("Creating visibility producer failed", tag.Error(err))
			}
			visibilityFromES = espersistence.NewESVisibilityManager("", nil, nil, visibilityProducer,
				params.MetricsClient, logger)
		}
		return persistence.NewVisibilityManagerWrapper(
			visibilityFromDB,
			visibilityFromES,
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // history visibility never read
			serviceConfig.AdvancedVisibilityWritingMode,
		), nil
	}

	serviceResource, err := resource.New(
		params,
		common.HistoryServiceName,
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
		visibilityManagerInitializer,
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource: serviceResource,
		status:   common.DaemonStatusInitialized,
		params:   params,
		config:   serviceConfig,
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.GetLogger()
	logger.Info("elastic search config", tag.ESConfig(s.params.ESConfig))
	logger.Info("history starting")

	s.handler = NewHandler(s.Resource, s.config)

	// must start resource first
	s.Resource.Start()
	s.handler.Start()

	opts, err := s.params.RPCFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating grpc server options failed", tag.Error(err))
	}
	opts = append(opts, grpc.UnaryInterceptor(interceptor))
	s.server = grpc.NewServer(opts...)
	nilCheckHandler := NewNilCheckHandler(s.handler)
	historyservice.RegisterHistoryServiceServer(s.server, nilCheckHandler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.GetGRPCListener()
	logger.Info("Starting to serve on history listener")
	if err := s.server.Serve(listener); err != nil {
		logger.Fatal("Failed to serve on history listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	// initiate graceful shutdown :
	// 1. remove self from the membership ring
	// 2. wait for other members to discover we are going down
	// 3. stop acquiring new shards (periodically or based on other membership changes)
	// 4. wait for shard ownership to transfer (and inflight requests to drain) while still accepting new requests
	// 5. Reject all requests arriving at rpc handler to avoid taking on more work except for RespondXXXCompleted and
	//    RecordXXStarted APIs - for these APIs, most of the work is already one and rejecting at last stage is
	//    probably not that desirable. If the shard is closed, these requests will fail anyways.
	// 6. wait for grace period
	// 7. force stop the whole world and return

	const gossipPropagationDelay = 400 * time.Millisecond
	const shardOwnershipTransferDelay = 5 * time.Second
	const gracePeriod = 2 * time.Second

	remainingTime := s.config.ShutdownDrainDuration()

	s.GetLogger().Info("ShutdownHandler: Evicting self from membership ring")
	s.GetMembershipMonitor().EvictSelf()

	s.GetLogger().Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	remainingTime = s.sleep(gossipPropagationDelay, remainingTime)

	s.GetLogger().Info("ShutdownHandler: Initiating shardController shutdown")
	s.handler.controller.PrepareToStop()
	s.GetLogger().Info("ShutdownHandler: Waiting for traffic to drain")
	remainingTime = s.sleep(shardOwnershipTransferDelay, remainingTime)

	s.GetLogger().Info("ShutdownHandler: No longer taking rpc requests")
	s.handler.PrepareToStop()
	remainingTime = s.sleep(gracePeriod, remainingTime)

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()
	s.Resource.Stop()

	s.GetLogger().Info("history stopped")
}

func interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, serviceerror.ToStatus(err).Err()
}

// sleep sleeps for the minimum of desired and available duration
// returns the remaining available time duration
func (s *Service) sleep(desired time.Duration, available time.Duration) time.Duration {
	d := common.MinDuration(desired, available)
	if d > 0 {
		time.Sleep(d)
	}
	return available - d
}
