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
	"math"
	"sort"
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

var (
	timerQueueAckMgrMaxTimestamp = time.Unix(0, math.MaxInt64)
)

type (
	timerSequenceIDs []TimerSequenceID

	timerQueueAckMgrImpl struct {
		clusterName   string
		shard         ShardContext
		executionMgr  persistence.ExecutionManager
		logger        bark.Logger
		metricsClient metrics.Client
		lastUpdated   time.Time
		config        *Config

		sync.Mutex
		// outstanding timer task -> finished (true)
		outstandingTasks map[TimerSequenceID]bool
		// outstanding timer tasks, which cannot be processed right now
		retryTasks []*persistence.TimerTaskInfo
		// timer task read level
		readLevel TimerSequenceID
		// timer task ack level
		ackLevel time.Time
	}
	// for each cluster, the ack level is the point in time when
	// all timers before the ack level are processed.
	// for each cluster, the read level is the point in time when
	// all timers from ack level to read level are loaded in memory.

	// TODO this processing logic potentially has bug, refer to #605, #608
)

// Len implements sort.Interace
func (t timerSequenceIDs) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t timerSequenceIDs) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timerSequenceIDs) Less(i, j int) bool {
	return compareTimerIDLess(&t[i], &t[j])
}

func newTimerQueueAckMgr(shard ShardContext, metricsClient metrics.Client, executionMgr persistence.ExecutionManager, clusterName string, logger bark.Logger) *timerQueueAckMgrImpl {
	config := shard.GetConfig()
	ackLevel := shard.GetTimerAckLevel(clusterName)
	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		clusterName:      clusterName,
		shard:            shard,
		executionMgr:     executionMgr,
		metricsClient:    metricsClient,
		logger:           logger,
		lastUpdated:      time.Now(), // this has nothing to do with remote cluster, so use the local time
		config:           config,
		outstandingTasks: make(map[TimerSequenceID]bool),
		retryTasks:       []*persistence.TimerTaskInfo{},
		readLevel:        TimerSequenceID{VisibilityTimestamp: ackLevel},
		ackLevel:         ackLevel,
	}

	return timerQueueAckMgrImpl
}

func (t *timerQueueAckMgrImpl) readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error) {
	t.Lock()
	readLevel := t.readLevel
	timerTaskRetrySize := len(t.retryTasks)
	t.Unlock()

	var tasks []*persistence.TimerTaskInfo
	morePage := timerTaskRetrySize > 0
	var err error
	if timerTaskRetrySize < t.config.TimerTaskBatchSize {
		var token []byte
		tasks, token, err = t.getTimerTasks(readLevel.VisibilityTimestamp, timerQueueAckMgrMaxTimestamp, t.config.TimerTaskBatchSize)
		if err != nil {
			return nil, nil, false, err
		}
		t.logger.Debugf("readTimerTasks: ReadLevel: (%s) count: %v, next token: %v", readLevel, len(tasks), token)
		morePage = len(token) > 0
	}

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var lookAheadTask *persistence.TimerTaskInfo
	t.Lock()
	defer t.Unlock()
	// fillin the retry task
	filteredTasks := t.retryTasks
	t.retryTasks = []*persistence.TimerTaskInfo{}

	// since we have already checked that the clusterName is a valid key of clusterReadLevel
	// there shall be no validation
	readLevel = t.readLevel
	outstandingTasks := t.outstandingTasks
TaskFilterLoop:
	for _, task := range tasks {
		timerSequenceID := TimerSequenceID{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
		_, isLoaded := outstandingTasks[timerSequenceID]
		if isLoaded {
			// timer already loaded
			t.logger.Infof("Skipping task: %v. WorkflowID: %v, RunID: %v, Type: %v",
				timerSequenceID.String(), task.WorkflowID, task.RunID, task.TaskType)
			continue TaskFilterLoop
		}

		domainEntry, err := t.shard.GetDomainCache().GetDomainByID(task.DomainID)
		if err != nil {
			return nil, nil, false, err
		}
		if !domainEntry.GetIsGlobalDomain() {
			if t.clusterName != t.shard.GetService().GetClusterMetadata().GetCurrentClusterName() {
				// timer task does not belong to cluster name
				continue TaskFilterLoop
			}
		} else {
			// global domain we need to check the cluster name
			clusterName := domainEntry.GetReplicationConfig().ActiveClusterName
			if clusterName != t.clusterName {
				// timer task does not belong here
				continue TaskFilterLoop
			}
		}

		// TODO potential bug here
		// there can be severe case when this readTimerTasks is called multiple times
		// and one of the call is really slow, causing the read level updated by other threads,
		// leading the if below to be true
		if task.VisibilityTimestamp.Before(readLevel.VisibilityTimestamp) {
			t.logger.Fatalf(
				"Next timer task time stamp is less than current timer task read level. timer task: (%s), ReadLevel: (%s)",
				timerSequenceID, readLevel)
		}

		if !t.isProcessNow(task.VisibilityTimestamp) {
			lookAheadTask = task
			break TaskFilterLoop
		}

		t.logger.Debugf("Moving timer read level: (%s)", timerSequenceID)
		readLevel = timerSequenceID
		outstandingTasks[timerSequenceID] = false
		filteredTasks = append(filteredTasks, task)
	}
	t.readLevel = readLevel

	// We may have large number of timers which need to be fired immediately.  Return true in such case so the pump
	// can call back immediately to retrieve more tasks
	moreTasks := lookAheadTask == nil && morePage

	return filteredTasks, lookAheadTask, moreTasks, nil
}

func (t *timerQueueAckMgrImpl) retryTimerTask(timerTask *persistence.TimerTaskInfo) {
	t.Lock()
	defer t.Unlock()

	t.retryTasks = append(t.retryTasks, timerTask)
}

func (t *timerQueueAckMgrImpl) readRetryTimerTasks() []*persistence.TimerTaskInfo {
	t.Lock()
	defer t.Unlock()

	retryTasks := t.retryTasks
	t.retryTasks = []*persistence.TimerTaskInfo{}
	return retryTasks
}

func (t *timerQueueAckMgrImpl) completeTimerTask(timerTask *persistence.TimerTaskInfo) {
	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timerTask.VisibilityTimestamp, TaskID: timerTask.TaskID}
	t.Lock()
	defer t.Unlock()

	t.outstandingTasks[timerSequenceID] = true
	if err := t.executionMgr.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{
		VisibilityTimestamp: timerTask.VisibilityTimestamp,
		TaskID:              timerTask.TaskID}); err != nil {
		t.logger.Warnf("Timer queue ack manager unable to complete timer task: %v; %v", timerSequenceID, err)
	}
}

func (t *timerQueueAckMgrImpl) updateAckLevel() {
	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateCounter)

	t.Lock()
	initialAckLevel := t.ackLevel
	updatedAckLevel := t.ackLevel
	outstandingTasks := t.outstandingTasks

	// Timer Sequence IDs can have holes in the middle. So we sort the map to get the order to
	// check. TODO: we can maintain a sorted slice as well.
	var sequenceIDs timerSequenceIDs
	for k := range outstandingTasks {
		sequenceIDs = append(sequenceIDs, k)
	}
	sort.Sort(sequenceIDs)

MoveAckLevelLoop:
	for _, current := range sequenceIDs {
		acked := outstandingTasks[current]
		if acked {
			updatedAckLevel = current.VisibilityTimestamp
			delete(outstandingTasks, current)
		} else {
			break MoveAckLevelLoop
		}
	}
	t.ackLevel = updatedAckLevel
	t.Unlock()

	// Do not update Acklevel if nothing changed upto force update interval
	if initialAckLevel == updatedAckLevel && time.Since(t.lastUpdated) < t.config.TimerProcessorForceUpdateInterval {
		return
	}

	t.logger.Debugf("Updating timer ack level: %v", updatedAckLevel)

	// Always update ackLevel to detect if the shared is stolen
	if err := t.shard.UpdateTimerAckLevel(t.clusterName, updatedAckLevel); err != nil {
		t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateFailedCounter)
		t.logger.Errorf("Error updating timer ack level for shard: %v", err)
	} else {
		t.lastUpdated = time.Now() // this has nothing to do with remote cluster, so use the local time
	}
}

// this function does not take cluster name as parameter, due to we only have one timer queue on Cassandra
// all timer tasks are in this queue and filter will be applied.
func (t *timerQueueAckMgrImpl) getTimerTasks(minTimestamp time.Time, maxTimestamp time.Time, batchSize int) ([]*persistence.TimerTaskInfo, []byte, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize,
	}

	retryCount := t.config.TimerProcessorGetFailureRetryCount
	for attempt := 0; attempt < retryCount; attempt++ {
		response, err := t.executionMgr.GetTimerIndexTasks(request)
		if err == nil {
			return response.Timers, response.NextPageToken, nil
		}
		backoff := time.Duration(attempt * 100)
		time.Sleep(backoff * time.Millisecond)
	}
	return nil, nil, ErrMaxAttemptsExceeded
}

func (t *timerQueueAckMgrImpl) isProcessNow(expiryTime time.Time) bool {
	return !expiryTime.IsZero() && expiryTime.UnixNano() <= t.shard.GetCurrentTime(t.clusterName).UnixNano()
}
