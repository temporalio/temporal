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
		isFailover    bool
		clusterName   string
		shard         ShardContext
		executionMgr  persistence.ExecutionManager
		logger        bark.Logger
		metricsClient metrics.Client
		config        *Config
		// immutable max possible timer level
		maxAckLevel time.Time
		// isReadFinished indicate timer queue ack manager
		// have no more task to send out
		isReadFinished bool
		// finishedChan will send out signal when timer
		// queue ack manager have no more task to send out and all
		// tasks sent are finished
		finishedChan chan struct{}

		sync.Mutex
		// outstanding timer task -> finished (true)
		outstandingTasks map[TimerSequenceID]bool
		// timer task read level
		readLevel TimerSequenceID
		// timer task ack level
		ackLevel TimerSequenceID
		// number of finished and acked tasks, used to reduce # of calls to update shard
		finishedTaskCounter int
	}
	// for each cluster, the ack level is the point in time when
	// all timers before the ack level are processed.
	// for each cluster, the read level is the point in time when
	// all timers from ack level to read level are loaded in memory.

	// TODO this processing logic potentially has bug, refer to #605, #608
)

var _ timerQueueAckMgr = (*timerQueueAckMgrImpl)(nil)

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

func newTimerQueueAckMgr(shard ShardContext, metricsClient metrics.Client, clusterName string, logger bark.Logger) *timerQueueAckMgrImpl {
	ackLevel := TimerSequenceID{VisibilityTimestamp: shard.GetTimerClusterAckLevel(clusterName)}
	maxAckLevel := timerQueueAckMgrMaxTimestamp

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		isFailover:       false,
		clusterName:      clusterName,
		shard:            shard,
		executionMgr:     shard.GetExecutionManager(),
		metricsClient:    metricsClient,
		logger:           logger,
		config:           shard.GetConfig(),
		outstandingTasks: make(map[TimerSequenceID]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		maxAckLevel:      maxAckLevel,
		isReadFinished:   false,
		finishedChan:     nil,
	}

	return timerQueueAckMgrImpl
}

func newTimerQueueFailoverAckMgr(shard ShardContext, metricsClient metrics.Client, standbyClusterName string,
	minLevel time.Time, maxLevel time.Time, logger bark.Logger) *timerQueueAckMgrImpl {
	// failover ack manager will start from the standby cluster's ack level to active cluster's ack level
	ackLevel := TimerSequenceID{VisibilityTimestamp: minLevel}

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		isFailover:       true,
		clusterName:      standbyClusterName,
		shard:            shard,
		executionMgr:     shard.GetExecutionManager(),
		metricsClient:    metricsClient,
		logger:           logger,
		config:           shard.GetConfig(),
		outstandingTasks: make(map[TimerSequenceID]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		maxAckLevel:      maxLevel,
		isReadFinished:   false,
		finishedChan:     make(chan struct{}, 1),
	}

	return timerQueueAckMgrImpl
}

func (t *timerQueueAckMgrImpl) getFinishedChan() <-chan struct{} {
	return t.finishedChan
}

func (t *timerQueueAckMgrImpl) readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error) {
	t.Lock()
	readLevel := t.readLevel
	t.Unlock()

	var tasks []*persistence.TimerTaskInfo
	morePage := false
	var err error
	if readLevel.VisibilityTimestamp.Before(t.maxAckLevel) {
		tasks, morePage, err = t.getTimerTasks(readLevel.VisibilityTimestamp, t.maxAckLevel, t.config.TimerTaskBatchSize)
		if err != nil {
			return nil, nil, false, err
		}
	}

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var lookAheadTask *persistence.TimerTaskInfo
	t.Lock()
	defer t.Unlock()
	if t.isFailover && !morePage {
		t.isReadFinished = true
	}

	filteredTasks := []*persistence.TimerTaskInfo{}

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
			t.logger.Debugf("Skipping timer task: %v. WorkflowID: %v, RunID: %v, Type: %v",
				timerSequenceID.String(), task.WorkflowID, task.RunID, task.TaskType)
			continue TaskFilterLoop
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

func (t *timerQueueAckMgrImpl) completeTimerTask(timerTask *persistence.TimerTaskInfo) {
	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timerTask.VisibilityTimestamp, TaskID: timerTask.TaskID}
	t.Lock()
	defer t.Unlock()

	t.outstandingTasks[timerSequenceID] = true
}

func (t *timerQueueAckMgrImpl) getAckLevel() TimerSequenceID {
	t.Lock()
	defer t.Unlock()
	return t.ackLevel
}

func (t *timerQueueAckMgrImpl) getReadLevel() TimerSequenceID {
	t.Lock()
	defer t.Unlock()
	return t.readLevel
}

func (t *timerQueueAckMgrImpl) updateAckLevel() {
	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateCounter)

	t.Lock()
	ackLevel := t.ackLevel
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
			ackLevel = current
			t.finishedTaskCounter++
			delete(outstandingTasks, current)
		} else {
			break MoveAckLevelLoop
		}
	}
	t.ackLevel = ackLevel

	if t.isFailover && t.isReadFinished && len(outstandingTasks) == 0 {
		// this means in failover mode, all possible failover timer tasks
		// are processed and we are free to shundown
		t.finishedChan <- struct{}{}
	}
	if t.finishedTaskCounter < t.config.TimerProcessorUpdateShardTaskCount {
		t.Unlock()
	} else {
		t.finishedTaskCounter = 0
		t.Unlock()
		t.updateTimerAckLevel(ackLevel)
	}
}

// this function does not take cluster name as parameter, due to we only have one timer queue on Cassandra
// all timer tasks are in this queue and filter will be applied.
func (t *timerQueueAckMgrImpl) getTimerTasks(minTimestamp time.Time, maxTimestamp time.Time, batchSize int) ([]*persistence.TimerTaskInfo, bool, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize,
	}

	retryCount := t.config.TimerProcessorGetFailureRetryCount
	for attempt := 0; attempt < retryCount; attempt++ {
		response, err := t.executionMgr.GetTimerIndexTasks(request)
		if err == nil {
			return response.Timers, len(response.Timers) >= batchSize, nil
		}
		backoff := time.Duration(attempt * 100)
		time.Sleep(backoff * time.Millisecond)
	}
	return nil, false, ErrMaxAttemptsExceeded
}

func (t *timerQueueAckMgrImpl) updateTimerAckLevel(ackLevel TimerSequenceID) {
	t.logger.Debugf("Updating timer ack level: %v", ackLevel)

	// not failover ack level updating
	if !t.isFailover {
		// Always update ackLevel to detect if the shared is stolen
		if err := t.shard.UpdateTimerClusterAckLevel(t.clusterName, ackLevel.VisibilityTimestamp); err != nil {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateFailedCounter)
			t.logger.Errorf("Error updating timer ack level for shard: %v", err)
		}
	} else {
		// TODO failover ack manager should persist failover ack level to Cassandra: issue #646
	}
}

func (t *timerQueueAckMgrImpl) isProcessNow(expiryTime time.Time) bool {
	var now time.Time
	if !t.isFailover {
		// not failover, use the cluster's local time
		now = t.shard.GetCurrentTime(t.clusterName)
	} else {
		// if ack manager is a failover manager, we need to use the current local time
		now = t.shard.GetCurrentTime(t.shard.GetService().GetClusterMetadata().GetCurrentClusterName())
	}

	return !expiryTime.IsZero() && expiryTime.UnixNano() <= now.UnixNano()
}
