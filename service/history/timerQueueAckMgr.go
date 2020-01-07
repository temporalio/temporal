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
	"math"
	"sort"
	"sync"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

var (
	maximumTime = time.Unix(0, math.MaxInt64)
)

type (
	// timerKeys is used for sorting timers
	timerKeys []timerKey

	// timerKey - Visibility timer stamp + Sequence Number.
	timerKey struct {
		VisibilityTimestamp time.Time
		TaskID              int64
	}

	timerQueueAckMgrImpl struct {
		scope               int
		isFailover          bool
		shard               ShardContext
		executionMgr        persistence.ExecutionManager
		logger              log.Logger
		metricsClient       metrics.Client
		config              *Config
		timeNow             timeNow
		updateTimerAckLevel updateTimerAckLevel
		timerQueueShutdown  timerQueueShutdown
		// isReadFinished indicate timer queue ack manager
		// have no more task to send out
		isReadFinished bool
		// finishedChan will send out signal when timer
		// queue ack manager have no more task to send out and all
		// tasks sent are finished
		finishedChan chan struct{}

		sync.Mutex
		// outstanding timer task -> finished (true)
		outstandingTasks map[timerKey]bool
		// timer task ack level
		ackLevel timerKey
		// timer task read level, used by failover
		readLevel timerKey
		// mutable timer level
		minQueryLevel time.Time
		maxQueryLevel time.Time
		pageToken     []byte

		clusterName string
	}
	// for each cluster, the ack level is the point in time when
	// all timers before the ack level are processed.
	// for each cluster, the read level is the point in time when
	// all timers from ack level to read level are loaded in memory.

	// TODO this processing logic potentially has bug, refer to #605, #608
)

var _ timerQueueAckMgr = (*timerQueueAckMgrImpl)(nil)

// Len implements sort.Interace
func (t timerKeys) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t timerKeys) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timerKeys) Less(i, j int) bool {
	return compareTimerIDLess(&t[i], &t[j])
}

func compareTimerIDLess(first *timerKey, second *timerKey) bool {
	if first.VisibilityTimestamp.Before(second.VisibilityTimestamp) {
		return true
	}
	if first.VisibilityTimestamp.Equal(second.VisibilityTimestamp) {
		return first.TaskID < second.TaskID
	}
	return false
}

func newTimerQueueAckMgr(
	scope int,
	shard ShardContext,
	metricsClient metrics.Client,
	minLevel time.Time,
	timeNow timeNow,
	updateTimerAckLevel updateTimerAckLevel,
	logger log.Logger,
	clusterName string,
) *timerQueueAckMgrImpl {
	ackLevel := timerKey{VisibilityTimestamp: minLevel}

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		scope:               scope,
		isFailover:          false,
		shard:               shard,
		executionMgr:        shard.GetExecutionManager(),
		metricsClient:       metricsClient,
		logger:              logger,
		config:              shard.GetConfig(),
		timeNow:             timeNow,
		updateTimerAckLevel: updateTimerAckLevel,
		timerQueueShutdown:  func() error { return nil },
		outstandingTasks:    make(map[timerKey]bool),
		ackLevel:            ackLevel,
		readLevel:           ackLevel,
		minQueryLevel:       ackLevel.VisibilityTimestamp,
		pageToken:           nil,
		maxQueryLevel:       ackLevel.VisibilityTimestamp,
		isReadFinished:      false,
		finishedChan:        nil,
		clusterName:         clusterName,
	}

	return timerQueueAckMgrImpl
}

func newTimerQueueFailoverAckMgr(
	shard ShardContext,
	metricsClient metrics.Client,
	minLevel time.Time,
	maxLevel time.Time,
	timeNow timeNow,
	updateTimerAckLevel updateTimerAckLevel,
	timerQueueShutdown timerQueueShutdown,
	logger log.Logger,
) *timerQueueAckMgrImpl {
	// failover ack manager will start from the standby cluster's ack level to active cluster's ack level
	ackLevel := timerKey{VisibilityTimestamp: minLevel}

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		scope:               metrics.TimerActiveQueueProcessorScope,
		isFailover:          true,
		shard:               shard,
		executionMgr:        shard.GetExecutionManager(),
		metricsClient:       metricsClient,
		logger:              logger,
		config:              shard.GetConfig(),
		timeNow:             timeNow,
		updateTimerAckLevel: updateTimerAckLevel,
		timerQueueShutdown:  timerQueueShutdown,
		outstandingTasks:    make(map[timerKey]bool),
		ackLevel:            ackLevel,
		readLevel:           ackLevel,
		minQueryLevel:       ackLevel.VisibilityTimestamp,
		pageToken:           nil,
		maxQueryLevel:       maxLevel,
		isReadFinished:      false,
		finishedChan:        make(chan struct{}, 1),
	}

	return timerQueueAckMgrImpl
}

func (t *timerQueueAckMgrImpl) getFinishedChan() <-chan struct{} {
	return t.finishedChan
}

func (t *timerQueueAckMgrImpl) readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error) {
	if t.maxQueryLevel == t.minQueryLevel {
		t.maxQueryLevel = t.shard.UpdateTimerMaxReadLevel(t.clusterName)
	}
	minQueryLevel := t.minQueryLevel
	maxQueryLevel := t.maxQueryLevel
	pageToken := t.pageToken

	var tasks []*persistence.TimerTaskInfo
	morePage := false
	var err error
	if minQueryLevel.Before(maxQueryLevel) {
		tasks, pageToken, err = t.getTimerTasks(minQueryLevel, maxQueryLevel, t.config.TimerTaskBatchSize(), pageToken)
		if err != nil {
			return nil, nil, false, err
		}
		morePage = len(pageToken) != 0
		t.logger.Debug(fmt.Sprintf("readTimerTasks: minQueryLevel: (%s)), maxQueryLevel: (%s), count: %v, more timer: %v",
			minQueryLevel, maxQueryLevel, len(tasks), morePage))
	}

	t.Lock()
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
		timerKey := timerKey{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
		_, isLoaded := t.outstandingTasks[timerKey]
		if isLoaded {
			// timer already loaded
			t.logger.Debug(fmt.Sprintf("Skipping timer task: %v. WorkflowID: %v, RunID: %v, Type: %v",
				timerKey, task.WorkflowID, task.RunID, task.TaskType))
			continue TaskFilterLoop
		}

		if !t.isProcessNow(task.VisibilityTimestamp) {
			lookAheadTask = task                       // this means there is task in the time range (now, now + offset)
			t.maxQueryLevel = task.VisibilityTimestamp // adjust maxQueryLevel so that this task will be read next time
			break TaskFilterLoop
		}

		t.logger.Debug(fmt.Sprintf("Moving timer read level: %v", timerKey))
		t.readLevel = timerKey

		t.outstandingTasks[timerKey] = false
		filteredTasks = append(filteredTasks, task)
	}

	if lookAheadTask != nil || !morePage {
		if t.isReadFinished {
			t.minQueryLevel = maximumTime // set it to the maximum time to avoid any mistakenly read
		} else {
			t.minQueryLevel = t.maxQueryLevel
		}
		t.logger.Debug(fmt.Sprintf("Moved timer minQueryLevel: (%s)", t.minQueryLevel))
		t.pageToken = nil
	}
	t.Unlock()

	// only do lookahead when not in failover mode
	if len(t.pageToken) == 0 && lookAheadTask == nil && !t.isFailover {
		lookAheadTask, err = t.readLookAheadTask()
		if err != nil {
			// NOTE do not return nil filtered task
			// or otherwise the tasks are loaded and will never be dispatched
			// return true so timer quque process base will do another call
			return filteredTasks, nil, true, nil
		}
	}

	// We may have large number of timers which need to be fired immediately.  Return true in such case so the pump
	// can call back immediately to retrieve more tasks
	moreTasks := lookAheadTask == nil && morePage

	return filteredTasks, lookAheadTask, moreTasks, nil
}

// read lookAheadTask from s.GetTimerMaxReadLevel to poll interval from there.
func (t *timerQueueAckMgrImpl) readLookAheadTask() (*persistence.TimerTaskInfo, error) {
	minQueryLevel := t.maxQueryLevel
	maxQueryLevel := maximumTime

	var tasks []*persistence.TimerTaskInfo
	var err error
	tasks, _, err = t.getTimerTasks(minQueryLevel, maxQueryLevel, 1, nil)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 1 {
		return tasks[0], nil
	}
	return nil, nil
}

func (t *timerQueueAckMgrImpl) completeTimerTask(timerTask *persistence.TimerTaskInfo) {
	timerKey := timerKey{VisibilityTimestamp: timerTask.VisibilityTimestamp, TaskID: timerTask.TaskID}
	t.Lock()
	defer t.Unlock()

	t.outstandingTasks[timerKey] = true
}

func (t *timerQueueAckMgrImpl) getReadLevel() timerKey {
	t.Lock()
	defer t.Unlock()
	return t.readLevel
}

func (t *timerQueueAckMgrImpl) getAckLevel() timerKey {
	t.Lock()
	defer t.Unlock()
	return t.ackLevel
}

func (t *timerQueueAckMgrImpl) updateAckLevel() {
	t.metricsClient.IncCounter(t.scope, metrics.AckLevelUpdateCounter)

	t.Lock()
	ackLevel := t.ackLevel
	outstandingTasks := t.outstandingTasks

	t.logger.Debug(fmt.Sprintf("Moving timer ack level from %v, with %v.", ackLevel, outstandingTasks))

	// Timer Sequence IDs can have holes in the middle. So we sort the map to get the order to
	// check. TODO: we can maintain a sorted slice as well.
	var sequenceIDs timerKeys
	for k := range outstandingTasks {
		sequenceIDs = append(sequenceIDs, k)
	}
	sort.Sort(sequenceIDs)

	pendingTasks := len(sequenceIDs)
	if pendingTasks > warnPendingTasks {
		t.logger.Warn("Too many pending tasks.")
	}
	switch t.scope {
	case metrics.TimerActiveQueueProcessorScope:
		t.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerActivePendingTasksTimer, time.Duration(pendingTasks))
	case metrics.TimerStandbyQueueProcessorScope:
		t.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerStandbyPendingTasksTimer, time.Duration(pendingTasks))
	}

MoveAckLevelLoop:
	for _, current := range sequenceIDs {
		acked := outstandingTasks[current]
		if acked {
			ackLevel = current
			delete(outstandingTasks, current)
			t.logger.Debug(fmt.Sprintf("Moving timer ack level to %v.", ackLevel))
		} else {
			break MoveAckLevelLoop
		}
	}
	t.ackLevel = ackLevel

	if t.isFailover && t.isReadFinished && len(outstandingTasks) == 0 {
		t.Unlock()
		// this means in failover mode, all possible failover timer tasks
		// are processed and we are free to shutdown
		t.logger.Debug(fmt.Sprintf("Timer ack manager shutdown."))
		t.finishedChan <- struct{}{}
		err := t.timerQueueShutdown()
		if err != nil {
			t.logger.Error("Error shutting down timer queue", tag.Error(err))
		}
		return
	}

	t.Unlock()
	if err := t.updateTimerAckLevel(ackLevel); err != nil {
		t.metricsClient.IncCounter(t.scope, metrics.AckLevelUpdateFailedCounter)
		t.logger.Error("Error updating timer ack level for shard", tag.Error(err))
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

func (t *timerQueueAckMgrImpl) isProcessNow(expiryTime time.Time) bool {
	if expiryTime.IsZero() { // return true, but somewhere probably have bug creating empty timerTask.
		t.logger.Warn("Timer task has timestamp zero")
	}
	return expiryTime.UnixNano() <= t.timeNow().UnixNano()
}
