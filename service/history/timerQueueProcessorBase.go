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
	"context"
	"sync"
	"sync/atomic"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/worker/archiver"
)

var (
	emptyTime = time.Time{}

	loadDomainEntryForTimerTaskRetryDelay = 100 * time.Millisecond
	loadTimerTaskThrottleRetryDelay       = 5 * time.Second
)

type (
	timerQueueProcessorBase struct {
		scope            int
		shard            ShardContext
		historyService   *historyEngineImpl
		cache            *historyCache
		executionManager persistence.ExecutionManager
		status           int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		config           *Config
		logger           log.Logger
		metricsClient    metrics.Client
		timerFiredCount  uint64
		timerProcessor   timerProcessor
		timerQueueAckMgr timerQueueAckMgr
		timerGate        TimerGate
		timeSource       clock.TimeSource
		rateLimiter      quotas.Limiter
		retryPolicy      backoff.RetryPolicy
		lastPollTime     time.Time
		taskProcessor    *taskProcessor

		// timer notification
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

func newTimerQueueProcessorBase(
	scope int,
	shard ShardContext,
	historyService *historyEngineImpl,
	timerQueueAckMgr timerQueueAckMgr,
	timerGate TimerGate,
	maxPollRPS dynamicconfig.IntPropertyFn,
	logger log.Logger,
) *timerQueueProcessorBase {

	log := logger.WithTags(tag.ComponentTimerQueue)
	options := taskProcessorOptions{
		workerCount: shard.GetConfig().TimerTaskWorkerCount(),
		queueSize:   shard.GetConfig().TimerTaskWorkerCount() * shard.GetConfig().TimerTaskBatchSize(),
	}
	taskProcessor := newTaskProcessor(options, shard, historyService.historyCache, logger)
	base := &timerQueueProcessorBase{
		scope:            scope,
		shard:            shard,
		historyService:   historyService,
		cache:            historyService.historyCache,
		executionManager: shard.GetExecutionManager(),
		status:           common.DaemonStatusInitialized,
		shutdownCh:       make(chan struct{}),
		config:           shard.GetConfig(),
		logger:           log,
		metricsClient:    historyService.metricsClient,
		timerQueueAckMgr: timerQueueAckMgr,
		timerGate:        timerGate,
		timeSource:       shard.GetTimeSource(),
		newTimerCh:       make(chan struct{}, 1),
		lastPollTime:     time.Time{},
		taskProcessor:    taskProcessor,
		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(maxPollRPS())
			},
		),
		retryPolicy: common.CreatePersistanceRetryPolicy(),
	}

	return base
}

func (t *timerQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.taskProcessor.start()
	t.shutdownWG.Add(1)
	// notify a initial scan
	t.notifyNewTimer(time.Time{})
	go t.processorPump()

	t.logger.Info("Timer queue processor started.")
}

func (t *timerQueueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	t.timerGate.Close()
	close(t.shutdownCh)
	t.retryTasks()

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("Timer queue processor timedout on shutdown.")
	}

	t.taskProcessor.stop()
	t.logger.Info("Timer queue processor stopped.")
}

func (t *timerQueueProcessorBase) processorPump() {
	defer t.shutdownWG.Done()

RetryProcessor:
	for {
		select {
		case <-t.shutdownCh:
			break RetryProcessor
		default:
			err := t.internalProcessor()
			if err != nil {
				t.logger.Error("processor pump failed with error", tag.Error(err))
			}
		}
	}

	t.logger.Info("Timer queue processor pump shutting down.")
	t.logger.Info("Timer processor exiting.")
}

// NotifyNewTimers - Notify the processor about the new timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorBase) notifyNewTimers(
	timerTasks []persistence.Task,
) {

	if len(timerTasks) == 0 {
		return
	}

	isActive := t.scope == metrics.TimerActiveQueueProcessorScope

	newTime := timerTasks[0].GetVisibilityTimestamp()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTimestamp()
		if ts.Before(newTime) {
			newTime = ts
		}

		switch task.GetType() {
		case persistence.TaskTypeDecisionTimeout:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskDecisionTimeoutScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeActivityTimeout:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskActivityTimeoutScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeUserTimer:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskUserTimerScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskUserTimerScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeWorkflowTimeout:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowTimeoutScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskWorkflowTimeoutScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeDeleteHistoryEvent:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskDeleteHistoryEventScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskDeleteHistoryEventScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeActivityRetryTimer:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityRetryTimerScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskActivityRetryTimerScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeWorkflowBackoffTimer:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskWorkflowBackoffTimerScope, metrics.NewTimerCounter)
			}
			// TODO add default
		}
	}

	t.notifyNewTimer(newTime)
}

func (t *timerQueueProcessorBase) notifyNewTimer(
	newTime time.Time,
) {

	t.newTimeLock.Lock()
	defer t.newTimeLock.Unlock()
	if t.newTime.IsZero() || newTime.Before(t.newTime) {
		t.newTime = newTime
		select {
		case t.newTimerCh <- struct{}{}:
			// Notified about new time.
		default:
			// Channel "full" -> drop and move on, this will happen only if service is in high load.
		}
	}
}

func (t *timerQueueProcessorBase) internalProcessor() error {
	pollTimer := time.NewTimer(backoff.JitDuration(
		t.config.TimerProcessorMaxPollInterval(),
		t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	updateAckTimer := time.NewTimer(backoff.JitDuration(
		t.config.TimerProcessorUpdateAckInterval(),
		t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

	for {
		// Wait until one of four things occurs:
		// 1. we get notified of a new message
		// 2. the timer gate fires (message scheduled to be delivered)
		// 3. shutdown was triggered.
		// 4. updating ack level
		//
		select {
		case <-t.shutdownCh:
			t.logger.Debug("Timer queue processor pump shutting down.")
			return nil
		case <-t.timerQueueAckMgr.getFinishedChan():
			// timer queue ack manager indicate that all task scanned
			// are finished and no more tasks
			// use a separate goroutine since the caller hold the shutdownWG
			go t.Stop()
			return nil
		case <-t.timerGate.FireChan():
			lookAheadTimer, err := t.readAndFanoutTimerTasks()
			if err != nil {
				return err
			}
			if lookAheadTimer != nil {
				t.timerGate.Update(lookAheadTimer.VisibilityTimestamp)
			}
		case <-pollTimer.C:
			pollTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorMaxPollInterval(),
				t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
			))
			if t.lastPollTime.Add(t.config.TimerProcessorMaxPollInterval()).Before(t.timeSource.Now()) {
				lookAheadTimer, err := t.readAndFanoutTimerTasks()
				if err != nil {
					return err
				}
				if lookAheadTimer != nil {
					t.timerGate.Update(lookAheadTimer.VisibilityTimestamp)
				}
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorUpdateAckInterval(),
				t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
			))
			t.timerQueueAckMgr.updateAckLevel()
		case <-t.newTimerCh:
			t.newTimeLock.Lock()
			newTime := t.newTime
			t.newTime = emptyTime
			t.newTimeLock.Unlock()
			// New Timer has arrived.
			t.metricsClient.IncCounter(t.scope, metrics.NewTimerNotifyCounter)
			t.timerGate.Update(newTime)
		}
	}
}

func (t *timerQueueProcessorBase) readAndFanoutTimerTasks() (*persistence.TimerTaskInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), loadTimerTaskThrottleRetryDelay)
	if err := t.rateLimiter.Wait(ctx); err != nil {
		cancel()
		t.notifyNewTimer(time.Time{}) // re-enqueue the event
		return nil, nil
	}
	cancel()

	t.lastPollTime = t.timeSource.Now()
	timerTasks, lookAheadTask, moreTasks, err := t.timerQueueAckMgr.readTimerTasks()
	if err != nil {
		t.notifyNewTimer(time.Time{}) // re-enqueue the event
		return nil, err
	}

	for _, task := range timerTasks {
		if shutdown := t.taskProcessor.addTask(
			&taskInfo{
				processor: t.timerProcessor,
				task:      task,
			},
		); shutdown {
			return nil, nil
		}
		select {
		case <-t.shutdownCh:
			return nil, nil
		default:
		}
	}

	if !moreTasks {
		return lookAheadTask, nil
	}

	t.notifyNewTimer(time.Time{}) // re-enqueue the event
	return nil, nil
}

func (t *timerQueueProcessorBase) retryTasks() {
	t.taskProcessor.retryTasks()
}

func (t *timerQueueProcessorBase) complete(timerTask *persistence.TimerTaskInfo) {
	t.timerQueueAckMgr.completeTimerTask(timerTask)
	atomic.AddUint64(&t.timerFiredCount, 1)
}

func (t *timerQueueProcessorBase) getTimerFiredCount() uint64 {
	return atomic.LoadUint64(&t.timerFiredCount)
}

func (t *timerQueueProcessorBase) getDomainIDAndWorkflowExecution(
	task *persistence.TimerTaskInfo,
) (string, workflow.WorkflowExecution) {

	return task.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
}

func (t *timerQueueProcessorBase) processDeleteHistoryEvent(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(t.getDomainIDAndWorkflowExecution(task))
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}
	lastWriteVersion, err := msBuilder.GetLastWriteVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, lastWriteVersion, task.Version, task)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	domainCacheEntry, err := t.historyService.shard.GetDomainCache().GetDomainByID(task.DomainID)
	if err != nil {
		return err
	}
	clusterConfiguredForHistoryArchival := t.shard.GetService().GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	domainConfiguredForHistoryArchival := domainCacheEntry.GetConfig().HistoryArchivalStatus == workflow.ArchivalStatusEnabled
	archiveHistory := clusterConfiguredForHistoryArchival && domainConfiguredForHistoryArchival

	clusterConfiguredForVisibilityArchival := t.shard.GetService().GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival()
	domainConfiguredForVisibilityArchival := domainCacheEntry.GetConfig().VisibilityArchivalStatus == workflow.ArchivalStatusEnabled
	archiveVisibility := clusterConfiguredForVisibilityArchival && domainConfiguredForVisibilityArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && domain:enabled should be a nop rather than a delete
	if archiveHistory || archiveVisibility {
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupArchiveCount)
		return t.archiveWorkflow(task, context, msBuilder, domainCacheEntry, archiveHistory, archiveVisibility)
	}

	t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteCount)
	return t.deleteWorkflow(task, context, msBuilder)
}

func (t *timerQueueProcessorBase) deleteWorkflow(
	task *persistence.TimerTaskInfo,
	context workflowExecutionContext,
	msBuilder mutableState,
) error {

	if err := t.deleteCurrentWorkflowExecution(task); err != nil {
		return err
	}

	if err := t.deleteWorkflowExecution(task); err != nil {
		return err
	}

	if err := t.deleteWorkflowHistory(task, msBuilder); err != nil {
		return err
	}

	if err := t.deleteWorkflowVisibility(task); err != nil {
		return err
	}
	// calling clear here to force accesses of mutable state to read database
	// if this is not called then callers will get mutable state even though its been removed from database
	context.clear()
	return nil
}

func (t *timerQueueProcessorBase) archiveWorkflow(
	task *persistence.TimerTaskInfo,
	workflowContext workflowExecutionContext,
	msBuilder mutableState,
	domainCacheEntry *cache.DomainCacheEntry,
	archiveHistory bool,
	archiveVisibility bool,
) error {
	req := &archiver.ClientRequest{
		ArchiveRequest: &archiver.ArchiveRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		},
		CallerService:        common.HistoryServiceName,
		AttemptArchiveInline: true, // archive inline by default
	}
	if archiveHistory {
		executionStats, err := workflowContext.loadExecutionStats()
		if err != nil {
			return err
		}
		req.AttemptArchiveInline = executionStats.HistorySize < int64(t.config.TimerProcessorHistoryArchivalSizeLimit())
		req.ArchiveRequest.ShardID = t.shard.GetShardID()
		req.ArchiveRequest.DomainName = domainCacheEntry.GetInfo().Name
		req.ArchiveRequest.BranchToken, err = msBuilder.GetCurrentBranchToken()
		if err != nil {
			return err
		}
		req.ArchiveRequest.NextEventID = msBuilder.GetNextEventID()
		req.ArchiveRequest.CloseFailoverVersion, err = msBuilder.GetLastWriteVersion()
		if err != nil {
			return err
		}
		req.ArchiveRequest.URI = domainCacheEntry.GetConfig().HistoryArchivalURI
		req.ArchiveRequest.Targets = append(req.ArchiveRequest.Targets, archiver.ArchiveTargetHistory)
	}

	if archiveVisibility {
		executionInfo := msBuilder.GetExecutionInfo()
		startEvent, err := msBuilder.GetStartEvent()
		if err != nil {
			return err
		}
		completionEvent, err := msBuilder.GetCompletionEvent()
		if err != nil {
			return err
		}
		req.ArchiveRequest.WorkflowTypeName = executionInfo.WorkflowTypeName
		req.ArchiveRequest.StartTimestamp = executionInfo.StartTimestamp.UnixNano()
		req.ArchiveRequest.ExecutionTimestamp = getWorkflowExecutionTimestamp(msBuilder, startEvent).UnixNano()
		req.ArchiveRequest.CloseTimestamp = completionEvent.GetTimestamp()
		req.ArchiveRequest.CloseStatus = persistence.ToThriftWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
		req.ArchiveRequest.HistoryLength = msBuilder.GetNextEventID() - 1
		req.ArchiveRequest.VisibilityURI = domainCacheEntry.GetConfig().VisibilityArchivalURI
		req.ArchiveRequest.Memo = getWorkflowMemo(executionInfo.Memo)
		req.ArchiveRequest.SearchAttributes = executionInfo.SearchAttributes
		req.ArchiveRequest.Targets = append(req.ArchiveRequest.Targets, archiver.ArchiveTargetVisibility)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.config.TimerProcessorArchivalTimeLimit())
	defer cancel()
	resp, err := t.historyService.archivalClient.Archive(ctx, req)
	if err != nil {
		return err
	}

	if err := t.deleteCurrentWorkflowExecution(task); err != nil {
		return err
	}
	if err := t.deleteWorkflowExecution(task); err != nil {
		return err
	}
	// delete workflow history if history archival is not needed or history as been archived inline
	if !archiveHistory || resp.HistoryArchivedInline {
		if archiveHistory {
			t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteHistoryInlineCount)
		}
		if err := t.deleteWorkflowHistory(task, msBuilder); err != nil {
			return err
		}
	}
	// delete visibility record here regardless if it's been archived inline or not
	// since the entire record is included as part of the archive request.
	if err := t.deleteWorkflowVisibility(task); err != nil {
		return err
	}
	// calling clear here to force accesses of mutable state to read database
	// if this is not called then callers will get mutable state even though its been removed from database
	workflowContext.clear()
	return nil
}

func (t *timerQueueProcessorBase) deleteWorkflowExecution(
	task *persistence.TimerTaskInfo,
) error {

	op := func() error {
		return t.executionManager.DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueProcessorBase) deleteCurrentWorkflowExecution(
	task *persistence.TimerTaskInfo,
) error {

	op := func() error {
		return t.executionManager.DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueProcessorBase) deleteWorkflowHistory(
	task *persistence.TimerTaskInfo,
	msBuilder mutableState,
) error {

	op := func() error {
		branchToken, err := msBuilder.GetCurrentBranchToken()
		if err != nil {
			return err
		}

		logger := t.logger.WithTags(tag.WorkflowID(task.WorkflowID),
			tag.WorkflowRunID(task.RunID),
			tag.WorkflowDomainID(task.DomainID),
			tag.ShardID(t.shard.GetShardID()),
			tag.TaskID(task.GetTaskID()),
			tag.FailoverVersion(task.GetVersion()),
			tag.TaskType(task.GetTaskType()))
		return persistence.DeleteWorkflowExecutionHistoryV2(t.historyService.historyV2Mgr, branchToken, common.IntPtr(t.shard.GetShardID()), logger)

	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueProcessorBase) deleteWorkflowVisibility(
	task *persistence.TimerTaskInfo,
) error {

	op := func() error {
		return t.historyService.DeleteExecutionFromVisibility(task)
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueProcessorBase) getTimerTaskType(
	taskType int,
) string {

	switch taskType {
	case persistence.TaskTypeUserTimer:
		return "UserTimer"
	case persistence.TaskTypeActivityTimeout:
		return "ActivityTimeout"
	case persistence.TaskTypeDecisionTimeout:
		return "DecisionTimeout"
	case persistence.TaskTypeWorkflowTimeout:
		return "WorkflowTimeout"
	case persistence.TaskTypeDeleteHistoryEvent:
		return "DeleteHistoryEvent"
	case persistence.TaskTypeActivityRetryTimer:
		return "ActivityRetryTimerTask"
	case persistence.TaskTypeWorkflowBackoffTimer:
		return "WorkflowBackoffTimerTask"
	}
	return "UnKnown"
}
