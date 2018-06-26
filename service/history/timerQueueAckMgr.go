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
	timerQueueAckMgrMaxQueryLevel = time.Unix(0, math.MaxInt64)
)

type (
	timerSequenceIDs []TimerSequenceID

	timerQueueAckMgrImpl struct {
		scope         int
		isFailover    bool
		clusterName   string
		shard         ShardContext
		executionMgr  persistence.ExecutionManager
		logger        bark.Logger
		metricsClient metrics.Client
		config        *Config
		// immutable max possible timer level
		maxQueryLevel time.Time
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
		// timer task ack level
		ackLevel TimerSequenceID
		// timer task read level
		readLevel TimerSequenceID
		// mutable min timer level
		minQueryLevel time.Time
		pageToken     []byte
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

func newTimerQueueAckMgr(scope int, shard ShardContext, metricsClient metrics.Client, clusterName string, logger bark.Logger) *timerQueueAckMgrImpl {
	ackLevel := TimerSequenceID{VisibilityTimestamp: shard.GetTimerClusterAckLevel(clusterName)}
	maxQueryLevel := timerQueueAckMgrMaxQueryLevel

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		scope:            scope,
		isFailover:       false,
		clusterName:      clusterName,
		shard:            shard,
		executionMgr:     shard.GetExecutionManager(),
		metricsClient:    metricsClient,
		logger:           logger,
		config:           shard.GetConfig(),
		outstandingTasks: make(map[TimerSequenceID]bool),
		ackLevel:         ackLevel,
		readLevel:        ackLevel,
		minQueryLevel:    ackLevel.VisibilityTimestamp,
		pageToken:        nil,
		maxQueryLevel:    maxQueryLevel,
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
		scope:            metrics.TimerActiveQueueProcessorScope,
		isFailover:       true,
		clusterName:      standbyClusterName,
		shard:            shard,
		executionMgr:     shard.GetExecutionManager(),
		metricsClient:    metricsClient,
		logger:           logger,
		config:           shard.GetConfig(),
		outstandingTasks: make(map[TimerSequenceID]bool),
		ackLevel:         ackLevel,
		readLevel:        ackLevel,
		minQueryLevel:    ackLevel.VisibilityTimestamp,
		pageToken:        nil,
		maxQueryLevel:    maxLevel,
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
	minQueryLevel := t.minQueryLevel
	maxQueryLevel := t.maxQueryLevel
	pageToken := t.pageToken
	t.Unlock()

	var tasks []*persistence.TimerTaskInfo
	morePage := false
	var err error
	if minQueryLevel.Before(maxQueryLevel) {
		tasks, pageToken, err = t.getTimerTasks(minQueryLevel, maxQueryLevel, t.config.TimerTaskBatchSize(), pageToken)
		if err != nil {
			return nil, nil, false, err
		}
		morePage = len(pageToken) != 0
		t.logger.Debugf("readTimerTasks: ReadLevel: (%s) count: %v, more timer: %v", minQueryLevel, len(tasks), morePage)
	}

	t.Lock()
	defer t.Unlock()
	t.pageToken = pageToken
	if t.isFailover && !morePage {
		t.isReadFinished = true
	}

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var lookAheadTask *persistence.TimerTaskInfo
	filteredTasks := []*persistence.TimerTaskInfo{}

TaskFilterLoop:
	for _, task := range tasks {
		timerSequenceID := TimerSequenceID{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
		_, isLoaded := t.outstandingTasks[timerSequenceID]
		if isLoaded {
			// timer already loaded
			t.logger.Debugf("Skipping timer task: %v. WorkflowID: %v, RunID: %v, Type: %v",
				timerSequenceID.String(), task.WorkflowID, task.RunID, task.TaskType)
			continue TaskFilterLoop
		}

		if !t.isProcessNow(task.VisibilityTimestamp) {
			lookAheadTask = task
			break TaskFilterLoop
		}

		t.logger.Debugf("Moving timer read level: (%s)", timerSequenceID)
		t.readLevel = timerSequenceID
		t.outstandingTasks[timerSequenceID] = false
		filteredTasks = append(filteredTasks, task)
	}

	if lookAheadTask != nil || t.pageToken == nil {
		if !t.isReadFinished {
			t.minQueryLevel = t.readLevel.VisibilityTimestamp
		} else {
			t.minQueryLevel = t.maxQueryLevel
		}
		t.pageToken = nil
	}

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
	t.metricsClient.IncCounter(t.scope, metrics.AckLevelUpdateCounter)

	t.Lock()
	ackLevel := t.ackLevel
	outstandingTasks := t.outstandingTasks

	t.logger.Debugf("Moving timer ack level from %v, with %v.", ackLevel, outstandingTasks)

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
			delete(outstandingTasks, current)
			t.logger.Debugf("Moving timer ack level to %v.", ackLevel)
		} else {
			break MoveAckLevelLoop
		}
	}
	updateShard := t.ackLevel != ackLevel
	t.ackLevel = ackLevel

	if t.isFailover && t.isReadFinished && len(outstandingTasks) == 0 {
		// this means in failover mode, all possible failover timer tasks
		// are processed and we are free to shundown
		t.logger.Debugf("Timer ack manager shutdoen.")
		t.finishedChan <- struct{}{}
	}
	t.Unlock()

	if updateShard {
		t.updateTimerAckLevel(ackLevel)
	}
}

// this function does not take cluster name as parameter, due to we only have one timer queue on Cassandra
// all timer tasks are in this queue and filter will be applied.
func (t *timerQueueAckMgrImpl) getTimerTasks(minTimestamp time.Time, maxTimestamp time.Time, batchSize int, pageToken []byte) ([]*persistence.TimerTaskInfo, []byte, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: pageToken,
	}

	retryCount := t.config.TimerProcessorGetFailureRetryCount()
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

func (t *timerQueueAckMgrImpl) updateTimerAckLevel(ackLevel TimerSequenceID) {
	t.logger.Debugf("Updating timer ack level for shard: %v", ackLevel)

	// not failover ack level updating
	if !t.isFailover {
		// Always update ackLevel to detect if the shared is stolen
		if err := t.shard.UpdateTimerClusterAckLevel(t.clusterName, ackLevel.VisibilityTimestamp); err != nil {
			t.metricsClient.IncCounter(t.scope, metrics.AckLevelUpdateFailedCounter)
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
