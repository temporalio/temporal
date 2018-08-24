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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	timeNow                 func() time.Time
	updateTimerAckLevel     func(TimerSequenceID) error
	timerQueueShutdown      func() error
	timerTaskFilter         func(timer *persistence.TimerTaskInfo) (bool, error)
	timerQueueProcessorImpl struct {
		isGlobalDomainEnabled  bool
		currentClusterName     string
		shard                  ShardContext
		config                 *Config
		metricsClient          metrics.Client
		historyService         *historyEngineImpl
		ackLevel               TimerSequenceID
		logger                 bark.Logger
		matchingClient         matching.Client
		isStarted              int32
		isStopped              int32
		shutdownChan           chan struct{}
		activeTimerProcessor   *timerQueueActiveProcessorImpl
		standbyTimerProcessors map[string]*timerQueueStandbyProcessorImpl
	}
)

func newTimerQueueProcessor(shard ShardContext, historyService *historyEngineImpl, matchingClient matching.Client, logger bark.Logger) timerQueueProcessor {
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueTimerQueueComponent,
	})
	standbyTimerProcessors := make(map[string]*timerQueueStandbyProcessorImpl)
	for clusterName := range shard.GetService().GetClusterMetadata().GetAllClusterFailoverVersions() {
		if clusterName != shard.GetService().GetClusterMetadata().GetCurrentClusterName() {
			standbyTimerProcessors[clusterName] = newTimerQueueStandbyProcessor(shard, historyService, clusterName, logger)
		}
	}

	return &timerQueueProcessorImpl{
		isGlobalDomainEnabled:  shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled(),
		currentClusterName:     currentClusterName,
		shard:                  shard,
		config:                 shard.GetConfig(),
		metricsClient:          historyService.metricsClient,
		historyService:         historyService,
		ackLevel:               TimerSequenceID{VisibilityTimestamp: shard.GetTimerAckLevel()},
		logger:                 logger,
		matchingClient:         matchingClient,
		shutdownChan:           make(chan struct{}),
		activeTimerProcessor:   newTimerQueueActiveProcessor(shard, historyService, matchingClient, logger),
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
func (t *timerQueueProcessorImpl) NotifyNewTimers(clusterName string, currentTime time.Time, timerTasks []persistence.Task) {
	if clusterName == t.currentClusterName {
		t.activeTimerProcessor.notifyNewTimers(timerTasks)
		return
	}

	standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processor for %s.", clusterName))
	}
	standbyTimerProcessor.setCurrentTime(currentTime)
	standbyTimerProcessor.notifyNewTimers(timerTasks)
	standbyTimerProcessor.retryTasks()
}

func (t *timerQueueProcessorImpl) FailoverDomain(domainID string) {
	minLevel := t.shard.GetTimerClusterAckLevel(t.currentClusterName)
	standbyClusterName := t.currentClusterName
	for cluster := range t.shard.GetService().GetClusterMetadata().GetAllClusterFailoverVersions() {
		ackLevel := t.shard.GetTimerClusterAckLevel(cluster)
		if ackLevel.Before(minLevel) {
			minLevel = ackLevel
			standbyClusterName = cluster
		}
	}
	// the ack manager is exclusive, so just add a cassandra min precision
	maxLevel := t.activeTimerProcessor.timerQueueAckMgr.getReadLevel().VisibilityTimestamp.Add(1 * time.Millisecond)
	t.logger.Infof("Timer Failover Triggered: %v, min level: %v, max level: %v.\n", domainID, minLevel, maxLevel)
	// we should consider make the failover idempotent
	failoverTimerProcessor := newTimerQueueFailoverProcessor(t.shard, t.historyService, domainID,
		standbyClusterName, minLevel, maxLevel, t.matchingClient, t.logger)

	for _, standbyTimerProcessor := range t.standbyTimerProcessors {
		standbyTimerProcessor.retryTasks()
	}

	failoverTimerProcessor.Start()

	// err is ignored
	t.shard.UpdateTimerFailoverLevel(
		domainID,
		persistence.TimerFailoverLevel{
			MinLevel:     minLevel,
			CurrentLevel: minLevel,
			MaxLevel:     maxLevel,
			DomainIDs:    []string{domainID},
		},
	)
}

func (t *timerQueueProcessorImpl) getTimerFiredCount(clusterName string) uint64 {
	if clusterName == t.currentClusterName {
		return t.activeTimerProcessor.getTimerFiredCount()
	}

	standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processor for %s.", clusterName))
	}
	return standbyTimerProcessor.getTimerFiredCount()
}

func (t *timerQueueProcessorImpl) completeTimersLoop() {
	timer := time.NewTimer(t.config.TimerProcessorCompleteTimerInterval())
	defer timer.Stop()
	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			t.completeTimers()
			return
		case <-timer.C:
		CompleteLoop:
			for attempt := 0; attempt < t.config.TimerProcessorCompleteTimerFailureRetryCount(); attempt++ {
				err := t.completeTimers()
				if err != nil {
					t.logger.Infof("Failed to complete timers: %v.", err)
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
	upperAckLevel := t.activeTimerProcessor.timerQueueAckMgr.getAckLevel()

	if t.isGlobalDomainEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			ackLevel := standbyTimerProcessor.timerQueueAckMgr.getAckLevel()
			if !compareTimerIDLess(&upperAckLevel, &ackLevel) {
				upperAckLevel = ackLevel
			}
		}

		for _, failoverInfo := range t.shard.GetAllTimerFailoverLevels() {
			if !upperAckLevel.VisibilityTimestamp.Before(failoverInfo.MinLevel) {
				upperAckLevel = TimerSequenceID{VisibilityTimestamp: failoverInfo.MinLevel}
			}
		}
	}

	t.logger.Debugf("Start completing timer task from: %v, to %v.", lowerAckLevel, upperAckLevel)
	if !compareTimerIDLess(&lowerAckLevel, &upperAckLevel) {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.HistoryTaskBatchCompleteCounter)

	executionMgr := t.shard.GetExecutionManager()
	minTimestamp := lowerAckLevel.VisibilityTimestamp
	// relax the upper limit for scan since the query is [minTimestamp, maxTimestamp)
	maxTimestamp := upperAckLevel.VisibilityTimestamp.Add(1 * time.Second)
	batchSize := t.config.TimerTaskBatchSize()
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize,
	}

LoadCompleteLoop:
	for {
		response, err := executionMgr.GetTimerIndexTasks(request)
		if err != nil {
			return err
		}
		request.NextPageToken = response.NextPageToken

		for _, timer := range response.Timers {
			timerSequenceID := TimerSequenceID{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
			if compareTimerIDLess(&upperAckLevel, &timerSequenceID) {
				break LoadCompleteLoop
			}
			err := executionMgr.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{
				VisibilityTimestamp: timer.VisibilityTimestamp,
				TaskID:              timer.TaskID})
			if err != nil {
				t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.CompleteTaskFailedCounter)
				t.logger.Warnf("Timer queue ack manager unable to complete timer task: %v; %v", timer, err)
				return err
			}
			t.ackLevel = timerSequenceID
		}

		if len(response.NextPageToken) == 0 {
			break LoadCompleteLoop
		}
	}
	t.ackLevel = upperAckLevel

	t.shard.UpdateTimerAckLevel(t.ackLevel.VisibilityTimestamp)
	return nil
}
