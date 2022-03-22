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
	"math"
	"sort"
	"sync"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

var (
	maximumTime = time.Unix(0, math.MaxInt64).UTC()
)

const (
	timerOpInitialInterval    = 100 * time.Millisecond
	timerOpMaxInterval        = 1000 * time.Millisecond
	timerOpExpirationInterval = 10 * time.Second
	timerOpBackoffCoefficient = 2
	timerOpMaximumAttempts    = 20
)

type (
	timerQueueAckMgrImpl struct {
		scope               int
		isFailover          bool
		shard               shard.Context
		executionMgr        persistence.ExecutionManager
		logger              log.Logger
		metricsClient       metrics.Client
		config              *configs.Config
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
		outstandingTasks map[tasks.Key]bool
		// timer task ack level
		ackLevel tasks.Key
		// timer task read level, used by failover
		readLevel tasks.Key
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

func newTimerQueueAckMgr(
	scope int,
	shard shard.Context,
	minLevel time.Time,
	timeNow timeNow,
	updateTimerAckLevel updateTimerAckLevel,
	logger log.Logger,
	clusterName string,
) *timerQueueAckMgrImpl {
	ackLevel := tasks.Key{FireTime: minLevel}

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		scope:               scope,
		isFailover:          false,
		shard:               shard,
		executionMgr:        shard.GetExecutionManager(),
		metricsClient:       shard.GetMetricsClient(),
		logger:              logger,
		config:              shard.GetConfig(),
		timeNow:             timeNow,
		updateTimerAckLevel: updateTimerAckLevel,
		timerQueueShutdown:  func() error { return nil },
		outstandingTasks:    make(map[tasks.Key]bool),
		ackLevel:            ackLevel,
		readLevel:           ackLevel,
		minQueryLevel:       ackLevel.FireTime,
		pageToken:           nil,
		maxQueryLevel:       ackLevel.FireTime,
		isReadFinished:      false,
		finishedChan:        nil,
		clusterName:         clusterName,
	}

	return timerQueueAckMgrImpl
}

func newTimerQueueFailoverAckMgr(
	shard shard.Context,
	minLevel time.Time,
	maxLevel time.Time,
	timeNow timeNow,
	updateTimerAckLevel updateTimerAckLevel,
	timerQueueShutdown timerQueueShutdown,
	logger log.Logger,
) *timerQueueAckMgrImpl {
	// failover ack manager will start from the standby cluster's ack level to active cluster's ack level
	ackLevel := tasks.Key{FireTime: minLevel}

	timerQueueAckMgrImpl := &timerQueueAckMgrImpl{
		scope:               metrics.TimerActiveQueueProcessorScope,
		isFailover:          true,
		shard:               shard,
		executionMgr:        shard.GetExecutionManager(),
		metricsClient:       shard.GetMetricsClient(),
		logger:              logger,
		config:              shard.GetConfig(),
		timeNow:             timeNow,
		updateTimerAckLevel: updateTimerAckLevel,
		timerQueueShutdown:  timerQueueShutdown,
		outstandingTasks:    make(map[tasks.Key]bool),
		ackLevel:            ackLevel,
		readLevel:           ackLevel,
		minQueryLevel:       ackLevel.FireTime,
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

func (t *timerQueueAckMgrImpl) readTimerTasks() ([]tasks.Task, *time.Time, bool, error) {
	if t.maxQueryLevel == t.minQueryLevel {
		t.maxQueryLevel = t.shard.GetQueueMaxReadLevel(tasks.CategoryTimer, t.clusterName).FireTime
	}
	minQueryLevel := t.minQueryLevel
	maxQueryLevel := t.maxQueryLevel
	pageToken := t.pageToken

	var timerTasks []tasks.Task
	morePage := false
	var err error
	if minQueryLevel.Before(maxQueryLevel) {
		timerTasks, pageToken, err = t.getTimerTasks(minQueryLevel, maxQueryLevel, t.config.TimerTaskBatchSize(), pageToken)
		if err != nil {
			return nil, nil, false, err
		}
		morePage = len(pageToken) != 0
		t.logger.Debug("readTimerTasks",
			tag.MinQueryLevel(minQueryLevel), tag.MaxQueryLevel(maxQueryLevel), tag.Counter(len(timerTasks)), tag.Bool(morePage))
	}

	t.Lock()
	t.pageToken = pageToken
	if t.isFailover && !morePage {
		t.isReadFinished = true
	}

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var nextFireTime *time.Time
	filteredTasks := []tasks.Task{}

TaskFilterLoop:
	for _, task := range timerTasks {
		timerKey := &tasks.Key{FireTime: task.GetVisibilityTime(), TaskID: task.GetTaskID()}
		_, isLoaded := t.outstandingTasks[*timerKey]
		if isLoaded {
			// timer already loaded
			continue TaskFilterLoop
		}

		if !t.isProcessNow(timerKey.FireTime) {
			nextFireTime = timestamp.TimePtr(task.GetVisibilityTime()) // this means there is task in the time range (now, now + offset)
			t.maxQueryLevel = timerKey.FireTime                        // adjust maxQueryLevel so that this task will be read next time
			break TaskFilterLoop
		}

		t.logger.Debug("Moving timer read level", tag.Task(timerKey))
		t.readLevel = *timerKey

		t.outstandingTasks[*timerKey] = false
		filteredTasks = append(filteredTasks, task)
	}

	if nextFireTime != nil || !morePage {
		if t.isReadFinished {
			t.minQueryLevel = maximumTime // set it to the maximum time to avoid any mistakenly read
		} else {
			t.minQueryLevel = t.maxQueryLevel
		}
		t.logger.Debug("Moved timer minQueryLevel", tag.MinQueryLevel(t.minQueryLevel))
		t.pageToken = nil
	}
	t.Unlock()

	// only do lookahead when not in failover mode
	if len(t.pageToken) == 0 && nextFireTime == nil && !t.isFailover {
		nextFireTime, err = t.readLookAheadTask()
		if err != nil {
			// NOTE do not return nil filtered task
			// or otherwise the tasks are loaded and will never be dispatched
			// return true so timer quque process base will do another call
			return filteredTasks, nil, true, nil
		}
	}

	// We may have large number of timers which need to be fired immediately.  Return true in such case so the pump
	// can call back immediately to retrieve more tasks
	moreTasks := nextFireTime == nil && morePage

	return filteredTasks, nextFireTime, moreTasks, nil
}

// read lookAheadTask from s.GetTimerMaxReadLevel to poll interval from there.
func (t *timerQueueAckMgrImpl) readLookAheadTask() (*time.Time, error) {
	minQueryLevel := t.maxQueryLevel
	maxQueryLevel := minQueryLevel.Add(t.config.TimerProcessorMaxPollInterval())

	var tasks []tasks.Task
	var err error
	tasks, _, err = t.getTimerTasks(minQueryLevel, maxQueryLevel, 1, nil)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 1 {
		return timestamp.TimePtr(tasks[0].GetVisibilityTime()), nil
	}
	return timestamp.TimePtr(maxQueryLevel), nil
}

func (t *timerQueueAckMgrImpl) completeTimerTask(
	taskTimestamp time.Time,
	taskID int64,
) {
	timerKey := &tasks.Key{FireTime: taskTimestamp, TaskID: taskID}
	t.Lock()
	defer t.Unlock()

	t.outstandingTasks[*timerKey] = true
}

func (t *timerQueueAckMgrImpl) getReadLevel() tasks.Key {
	t.Lock()
	defer t.Unlock()
	return t.readLevel
}

func (t *timerQueueAckMgrImpl) getAckLevel() tasks.Key {
	t.Lock()
	defer t.Unlock()
	return t.ackLevel
}

func (t *timerQueueAckMgrImpl) updateAckLevel() error {
	t.metricsClient.IncCounter(t.scope, metrics.AckLevelUpdateCounter)

	t.Lock()
	ackLevel := t.ackLevel
	outstandingTasks := t.outstandingTasks

	// Timer Sequence IDs can have holes in the middle. So we sort the map to get the order to
	// check. TODO: we can maintain a sorted slice as well.
	var sequenceIDs tasks.Keys
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
		t.metricsClient.RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoTimerActivePendingTasksTimer, pendingTasks)
	case metrics.TimerStandbyQueueProcessorScope:
		t.metricsClient.RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoTimerStandbyPendingTasksTimer, pendingTasks)
	}

MoveAckLevelLoop:
	for _, current := range sequenceIDs {
		acked := outstandingTasks[current]
		if acked {
			ackLevel = current
			delete(outstandingTasks, current)
			t.logger.Debug("Moving timer ack level", tag.AckLevel(ackLevel))
		} else {
			break MoveAckLevelLoop
		}
	}
	t.ackLevel = ackLevel

	if t.isFailover && t.isReadFinished && len(outstandingTasks) == 0 {
		t.Unlock()
		// this means in failover mode, all possible failover timer tasks
		// are processed and we are free to shutdown
		t.logger.Debug("Timer ack manager shutdown")
		t.finishedChan <- struct{}{}
		err := t.timerQueueShutdown()
		if err != nil {
			t.logger.Error("Error shutting down timer queue", tag.Error(err))
		}
		return err
	}

	t.Unlock()
	if err := t.updateTimerAckLevel(ackLevel); err != nil {
		t.metricsClient.IncCounter(t.scope, metrics.AckLevelUpdateFailedCounter)
		t.logger.Error("Error updating timer ack level for shard", tag.Error(err))
		return err
	}
	return nil
}

// this function does not take cluster name as parameter, due to we only have one timer queue on Cassandra
// all timer tasks are in this queue and filter will be applied.
func (t *timerQueueAckMgrImpl) getTimerTasks(minTimestamp time.Time, maxTimestamp time.Time, batchSize int, pageToken []byte) ([]tasks.Task, []byte, error) {
	request := &persistence.GetHistoryTasksRequest{
		ShardID:      t.shard.GetShardID(),
		TaskCategory: tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.Key{
			FireTime: minTimestamp,
		},
		ExclusiveMaxTaskKey: tasks.Key{
			FireTime: maxTimestamp,
		},
		BatchSize:     batchSize,
		NextPageToken: pageToken,
	}
	response, err := t.executionMgr.GetHistoryTasks(context.TODO(), request)
	if err != nil {
		return nil, nil, err
	}
	return response.Tasks, response.NextPageToken, nil
}

func (t *timerQueueAckMgrImpl) isProcessNow(expiryTime time.Time) bool {
	if expiryTime.IsZero() { // return true, but somewhere probably have bug creating empty timerTask.
		t.logger.Warn("Timer task has timestamp zero")
	}
	return expiryTime.UnixNano() <= t.timeNow().UnixNano()
}

func createTimerRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(timerOpInitialInterval)
	policy.SetMaximumInterval(timerOpMaxInterval)
	policy.SetExpirationInterval(timerOpExpirationInterval)
	policy.SetBackoffCoefficient(timerOpBackoffCoefficient)
	policy.SetMaximumAttempts(timerOpMaximumAttempts)

	return policy
}
