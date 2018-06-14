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

	testGetPropertyKey:         "testGetPropertyKey",
	testGetIntPropertyKey:      "testGetIntPropertyKey",
	testGetFloat64PropertyKey:  "testGetFloat64PropertyKey",
	testGetDurationPropertyKey: "testGetDurationPropertyKey",
	testGetBoolPropertyKey:     "testGetBoolPropertyKey",

	// frontend settings
	FrontendVisibilityMaxPageSize: "frontend.visibilityMaxPageSize",
	FrontendHistoryMaxPageSize:    "frontend.historyMaxPageSize",
	FrontendRPS:                   "frontend.rps",
	FrontendHistoryMgrNumConns:    "frontend.historyMgrNumConns",

	// matching settings
	MatchingMinTaskThrottlingBurstSize:      "matching.minTaskThrottlingBurstSize",
	MatchingGetTasksBatchSize:               "matching.getTasksBatchSize",
	MatchingLongPollExpirationInterval:      "matching.longPollExpirationInterval",
	MatchingEnableSyncMatch:                 "matching.enableSyncMatch",
	MatchingUpdateAckInterval:               "matching.updateAckInterval",
	MatchingIdleTasklistCheckInterval:       "matching.idleTasklistCheckInterval",
	MatchingOutstandingTaskAppendsThreshold: "matching.outstandingTaskAppendsThreshold",
	MatchingMaxTaskBatchSize:                "matching.maxTaskBatchSize",

	// history settings
	HistoryLongPollExpirationInterval:                  "history.longPollExpirationInterval",
	HistoryCacheInitialSize:                            "history.cacheInitialSize",
	HistoryCacheMaxSize:                                "history.cacheMaxSize",
	HistoryCacheTTL:                                    "history.cacheTTL",
	AcquireShardInterval:                               "history.acquireShardInterval",
	TimerTaskBatchSize:                                 "history.timerTaskBatchSize",
	TimerTaskWorkerCount:                               "history.timerTaskWorkerCount",
	TimerTaskMaxRetryCount:                             "history.timerTaskMaxRetryCount",
	TimerProcessorGetFailureRetryCount:                 "history.timerProcessorGetFailureRetryCount",
	TimerProcessorCompleteTimerFailureRetryCount:       "history.timerProcessorCompleteTimerFailureRetryCount",
	TimerProcessorUpdateShardTaskCount:                 "history.timerProcessorUpdateShardTaskCount",
	TimerProcessorUpdateAckInterval:                    "history.timerProcessorUpdateAckInterval",
	TimerProcessorCompleteTimerInterval:                "history.timerProcessorCompleteTimerInterval",
	TimerProcessorMaxPollInterval:                      "history.timerProcessorMaxPollInterval",
	TimerProcessorStandbyTaskDelay:                     "history.timerProcessorStandbyTaskDelay",
	TransferTaskBatchSize:                              "history.transferTaskBatchSize",
	TransferProcessorMaxPollRPS:                        "history.transferProcessorMaxPollRPS",
	TransferTaskWorkerCount:                            "history.transferTaskWorkerCount",
	TransferTaskMaxRetryCount:                          "history.transferTaskMaxRetryCount",
	TransferProcessorCompleteTransferFailureRetryCount: "history.transferProcessorCompleteTransferFailureRetryCount",
	TransferProcessorUpdateShardTaskCount:              "history.transferProcessorUpdateShardTaskCount",
	TransferProcessorMaxPollInterval:                   "history.transferProcessorMaxPollInterval",
	TransferProcessorUpdateAckInterval:                 "history.transferProcessorUpdateAckInterval",
	TransferProcessorCompleteTransferInterval:          "history.transferProcessorCompleteTransferInterval",
	TransferProcessorStandbyTaskDelay:                  "history.transferProcessorStandbyTaskDelay",
	ReplicatorTaskBatchSize:                            "history.replicatorTaskBatchSize",
	ReplicatorTaskWorkerCount:                          "history.replicatorTaskWorkerCount",
	ReplicatorTaskMaxRetryCount:                        "history.replicatorTaskMaxRetryCount",
	ReplicatorProcessorMaxPollRPS:                      "history.replicatorProcessorMaxPollRPS",
	ReplicatorProcessorUpdateShardTaskCount:            "history.replicatorProcessorUpdateShardTaskCount",
	ReplicatorProcessorMaxPollInterval:                 "history.replicatorProcessorMaxPollInterval",
	ReplicatorProcessorUpdateAckInterval:               "history.replicatorProcessorUpdateAckInterval",
	ExecutionMgrNumConns:                               "history.executionMgrNumConns",
	HistoryMgrNumConns:                                 "history.historyMgrNumConns",
	MaximumBufferedEventsBatch:                         "history.maximumBufferedEventsBatch",
	ShardUpdateMinInterval:                             "history.shardUpdateMinInterval",
}

const (
	unknownKey Key = iota

	// key for tests
	testGetPropertyKey
	testGetIntPropertyKey
	testGetFloat64PropertyKey
	testGetDurationPropertyKey
	testGetBoolPropertyKey
	testGetIntPropertyFilteredByDomainKey
	testGetDurationPropertyFilteredByDomainKey

	// key for frontend

	// FrontendVisibilityMaxPageSize is default max size for ListWorkflowExecutions in one page
	FrontendVisibilityMaxPageSize
	// FrontendHistoryMaxPageSize is default max size for GetWorkflowExecutionHistory in one page
	FrontendHistoryMaxPageSize
	// FrontendRPS is workflow rate limit per second
	FrontendRPS
	// FrontendHistoryMgrNumConns is for persistence cluster.NumConns
	FrontendHistoryMgrNumConns

	// key for matching

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
	// MatchingOutstandingTaskAppendsThreshold is the threshold for outstanding task appends
	MatchingOutstandingTaskAppendsThreshold
	// MatchingMaxTaskBatchSize is max batch size for task writer
	MatchingMaxTaskBatchSize

	// key for history

	// HistoryLongPollExpirationInterval is the long poll expiration interval in the history service
	HistoryLongPollExpirationInterval
	// HistoryCacheInitialSize is initial size of history cache
	HistoryCacheInitialSize
	// HistoryCacheMaxSize is max size of history cache
	HistoryCacheMaxSize
	// HistoryCacheTTL is TTL of history cache
	HistoryCacheTTL
	// AcquireShardInterval is interval that timer used to acquire shard
	AcquireShardInterval
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
	// TimerProcessorCompleteTimerInterval is complete timer interval for timer processor
	TimerProcessorCompleteTimerInterval
	// TimerProcessorMaxPollInterval is max poll interval for timer processor
	TimerProcessorMaxPollInterval
	// TimerProcessorStandbyTaskDelay is task delay for standby task in timer processor
	TimerProcessorStandbyTaskDelay
	// TransferTaskBatchSize is batch size for transferQueueProcessor
	TransferTaskBatchSize
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
	// TransferProcessorUpdateAckInterval is update interval for transferQueueProcessor
	TransferProcessorUpdateAckInterval
	// TransferProcessorCompleteTransferInterval is complete timer interval for transferQueueProcessor
	TransferProcessorCompleteTransferInterval
	// TransferProcessorStandbyTaskDelay is delay time for standby task in transferQueueProcessor
	TransferProcessorStandbyTaskDelay
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
	// ReplicatorProcessorUpdateAckInterval is update interval for ReplicatorProcessor
	ReplicatorProcessorUpdateAckInterval
	// ExecutionMgrNumConns is persistence connections number for ExecutionManager
	ExecutionMgrNumConns
	// HistoryMgrNumConns is persistence connections number for HistoryManager
	HistoryMgrNumConns
	// MaximumBufferedEventsBatch is max number of buffer event in mutable state
	MaximumBufferedEventsBatch
	// ShardUpdateMinInterval is the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval
)

// Filter represents a filter on the dynamic config key
type Filter int

func (f Filter) String() string {
	if f <= unknownFilter || f > TaskListName {
		return filters[unknownFilter]
	}
	return filters[f]
}

var filters = []string{
	"unknownFilter",
	"domainName",
	"taskListName",
}

const (
	unknownFilter Filter = iota
	// DomainName is the domain name
	DomainName
	// TaskListName is the tasklist name
	TaskListName
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
