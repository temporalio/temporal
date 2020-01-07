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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination timerQueueProcessor_mock.go

package history

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
)

var (
	errUnknownTimerTask = errors.New("unknown timer task")
)

type (
	timerQueueProcessor interface {
		common.Daemon
		FailoverDomain(domainIDs map[string]struct{})
		NotifyNewTimers(clusterName string, timerTask []persistence.Task)
		LockTaskProcessing()
		UnlockTaskProcessing()
	}

	timeNow                 func() time.Time
	updateTimerAckLevel     func(timerKey) error
	timerQueueShutdown      func() error
	timerQueueProcessorImpl struct {
		isGlobalDomainEnabled  bool
		currentClusterName     string
		shard                  ShardContext
		taskAllocator          taskAllocator
		config                 *Config
		metricsClient          metrics.Client
		historyService         *historyEngineImpl
		ackLevel               timerKey
		logger                 log.Logger
		matchingClient         matching.Client
		isStarted              int32
		isStopped              int32
		shutdownChan           chan struct{}
		activeTimerProcessor   *timerQueueActiveProcessorImpl
		standbyTimerProcessors map[string]*timerQueueStandbyProcessorImpl
	}
)

func newTimerQueueProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	matchingClient matching.Client,
	logger log.Logger,
) timerQueueProcessor {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	logger = logger.WithTags(tag.ComponentTimerQueue)
	taskAllocator := newTaskAllocator(shard)

	standbyTimerProcessors := make(map[string]*timerQueueStandbyProcessorImpl)
	for clusterName, info := range shard.GetService().GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if clusterName != shard.GetService().GetClusterMetadata().GetCurrentClusterName() {
			historyRereplicator := xdc.NewHistoryRereplicator(
				currentClusterName,
				shard.GetDomainCache(),
				shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
				func(ctx context.Context, request *h.ReplicateRawEventsRequest) error {
					return historyService.ReplicateRawEvents(ctx, request)
				},
				shard.GetService().GetPayloadSerializer(),
				historyRereplicationTimeout,
				logger,
			)
			nDCHistoryResender := xdc.NewNDCHistoryResender(
				shard.GetDomainCache(),
				shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
				func(ctx context.Context, request *h.ReplicateEventsV2Request) error {
					return historyService.ReplicateEventsV2(ctx, request)
				},
				shard.GetService().GetPayloadSerializer(),
				logger,
			)
			standbyTimerProcessors[clusterName] = newTimerQueueStandbyProcessor(
				shard,
				historyService,
				clusterName,
				taskAllocator,
				historyRereplicator,
				nDCHistoryResender,
				logger,
			)
		}
	}

	return &timerQueueProcessorImpl{
		isGlobalDomainEnabled:  shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled(),
		currentClusterName:     currentClusterName,
		shard:                  shard,
		taskAllocator:          taskAllocator,
		config:                 shard.GetConfig(),
		metricsClient:          historyService.metricsClient,
		historyService:         historyService,
		ackLevel:               timerKey{VisibilityTimestamp: shard.GetTimerAckLevel()},
		logger:                 logger,
		matchingClient:         matchingClient,
		shutdownChan:           make(chan struct{}),
		activeTimerProcessor:   newTimerQueueActiveProcessor(shard, historyService, matchingClient, taskAllocator, logger),
		standbyTimerProcessors: standbyTimerProcessors,
	}
}

func (t *timerQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	t.activeTimerProcessor.Start()
	if t.isGlobalDomainEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			standbyTimerProcessor.Start()
		}
	}
	go t.completeTimersLoop()
}

func (t *timerQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.activeTimerProcessor.Stop()
	if t.isGlobalDomainEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			standbyTimerProcessor.Stop()
		}
	}
	close(t.shutdownChan)
}

// NotifyNewTimers - Notify the processor about the new active / standby timer arrival.
// This should be called each time new timer arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorImpl) NotifyNewTimers(
	clusterName string,
	timerTasks []persistence.Task,
) {

	if clusterName == t.currentClusterName {
		t.activeTimerProcessor.notifyNewTimers(timerTasks)
		return
	}

	standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processor for %s.", clusterName))
	}
	standbyTimerProcessor.setCurrentTime(t.shard.GetCurrentTime(clusterName))
	standbyTimerProcessor.notifyNewTimers(timerTasks)
	standbyTimerProcessor.retryTasks()
}

func (t *timerQueueProcessorImpl) FailoverDomain(
	domainIDs map[string]struct{},
) {

	minLevel := t.shard.GetTimerClusterAckLevel(t.currentClusterName)
	standbyClusterName := t.currentClusterName
	for clusterName, info := range t.shard.GetService().GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		ackLevel := t.shard.GetTimerClusterAckLevel(clusterName)
		if ackLevel.Before(minLevel) {
			minLevel = ackLevel
			standbyClusterName = clusterName
		}
	}
	// the ack manager is exclusive, so just add a cassandra min precision
	maxLevel := t.activeTimerProcessor.getReadLevel().VisibilityTimestamp.Add(1 * time.Millisecond)
	t.logger.Info("Timer Failover Triggered",
		tag.WorkflowDomainIDs(domainIDs),
		tag.MinLevel(minLevel.UnixNano()),
		tag.MaxLevel(maxLevel.UnixNano()))
	// we should consider make the failover idempotent
	updateShardAckLevel, failoverTimerProcessor := newTimerQueueFailoverProcessor(
		t.shard,
		t.historyService,
		domainIDs,
		standbyClusterName,
		minLevel,
		maxLevel,
		t.matchingClient,
		t.taskAllocator,
		t.logger,
	)

	for _, standbyTimerProcessor := range t.standbyTimerProcessors {
		standbyTimerProcessor.retryTasks()
	}

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerDomainFailoverCallback function
	err := updateShardAckLevel(timerKey{VisibilityTimestamp: minLevel})
	if err != nil {
		t.logger.Error("Error when update shard ack level", tag.Error(err))
	}
	failoverTimerProcessor.Start()
}

func (t *timerQueueProcessorImpl) LockTaskProcessing() {
	t.taskAllocator.lock()
}

func (t *timerQueueProcessorImpl) UnlockTaskProcessing() {
	t.taskAllocator.unlock()
}

func (t *timerQueueProcessorImpl) completeTimersLoop() {
	timer := time.NewTimer(t.config.TimerProcessorCompleteTimerInterval())
	defer timer.Stop()
	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			t.completeTimers() //nolint:errcheck
			return
		case <-timer.C:
		CompleteLoop:
			for attempt := 0; attempt < t.config.TimerProcessorCompleteTimerFailureRetryCount(); attempt++ {
				err := t.completeTimers()
				if err != nil {
					t.logger.Info("Failed to complete timers.", tag.Error(err))
					backoff := time.Duration(attempt * 100)
					time.Sleep(backoff * time.Millisecond)
				} else {
					break CompleteLoop
				}
			}
			timer.Reset(t.config.TimerProcessorCompleteTimerInterval())
		}
	}
}

func (t *timerQueueProcessorImpl) completeTimers() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.activeTimerProcessor.getAckLevel()

	if t.isGlobalDomainEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			ackLevel := standbyTimerProcessor.getAckLevel()
			if !compareTimerIDLess(&upperAckLevel, &ackLevel) {
				upperAckLevel = ackLevel
			}
		}

		for _, failoverInfo := range t.shard.GetAllTimerFailoverLevels() {
			if !upperAckLevel.VisibilityTimestamp.Before(failoverInfo.MinLevel) {
				upperAckLevel = timerKey{VisibilityTimestamp: failoverInfo.MinLevel}
			}
		}
	}

	t.logger.Debug(fmt.Sprintf("Start completing timer task from: %v, to %v.", lowerAckLevel, upperAckLevel))
	if !compareTimerIDLess(&lowerAckLevel, &upperAckLevel) {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if lowerAckLevel.VisibilityTimestamp.Before(upperAckLevel.VisibilityTimestamp) {
		err := t.shard.GetExecutionManager().RangeCompleteTimerTask(&persistence.RangeCompleteTimerTaskRequest{
			InclusiveBeginTimestamp: lowerAckLevel.VisibilityTimestamp,
			ExclusiveEndTimestamp:   upperAckLevel.VisibilityTimestamp,
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateTimerAckLevel(t.ackLevel.VisibilityTimestamp)
}
