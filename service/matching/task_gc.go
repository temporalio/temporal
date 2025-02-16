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

package matching

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/persistence"
)

type taskGC struct {
	backlogMgr *backlogManagerImpl
	db         *taskQueueDB
	config     *taskQueueConfig

	lock           int64
	ackLevel       int64
	lastDeleteTime time.Time
}

var maxTimeBetweenTaskDeletes = time.Second

// newTaskGC returns an instance of a task garbage collector object
// taskGC internally maintains a delete cursor and attempts to delete
// a batch of tasks everytime Run() method is called.
//
// In order for the taskGC to actually delete tasks when Run() is called, one of
// two conditions must be met
//   - Size Threshold: More than MaxDeleteBatchSize tasks are waiting to be deleted (rough estimation)
//   - Time Threshold: Time since previous delete was attempted exceeds maxTimeBetweenTaskDeletes
//
// Finally, the Run() method is safe to be called from multiple threads. The underlying
// implementation will make sure only one caller executes Run() and others simply bail out
func newTaskGC(backlogMgr *backlogManagerImpl) *taskGC {
	return &taskGC{backlogMgr: backlogMgr, db: backlogMgr.db, config: backlogMgr.config}
}

// Run deletes a batch of completed tasks, if it's possible to do so
// Only attempts deletion if size or time thresholds are met
func (tgc *taskGC) Run(ackLevel int64) {
	tgc.tryDeleteNextBatch(ackLevel, false)
}

// RunNow deletes a batch of completed tasks if it's possible to do so
// This method attempts deletions without waiting for size/time threshold to be met
func (tgc *taskGC) RunNow(ackLevel int64) {
	tgc.tryDeleteNextBatch(ackLevel, true)
}

func (tgc *taskGC) tryDeleteNextBatch(ackLevel int64, ignoreTimeCond bool) {
	if !tgc.tryLock() {
		return
	}
	defer tgc.unlock()

	batchSize := tgc.config.MaxTaskDeleteBatchSize()
	if !tgc.checkPrecond(ackLevel, batchSize, ignoreTimeCond) {
		return
	}
	tgc.lastDeleteTime = time.Now().UTC()

	ctx, cancel := context.WithTimeout(tgc.backlogMgr.tqCtx, ioTimeout)
	defer cancel()

	n, err := tgc.db.CompleteTasksLessThan(ctx, ackLevel+1, batchSize)
	if err != nil {
		return
	}
	// implementation behavior for CompleteTasksLessThan:
	// - unit test, cassandra: always return UnknownNumRowsAffected (in this case means "all")
	// - sql: return number of rows affected (should be <= batchSize)
	// if we get UnknownNumRowsAffected or a smaller number than our limit, we know we got
	// everything <= ackLevel, so we can reset ours. if not, we may have to try again.
	if n == persistence.UnknownNumRowsAffected || n < batchSize {
		tgc.ackLevel = ackLevel
	}
}

func (tgc *taskGC) checkPrecond(ackLevel int64, batchSize int, ignoreTimeCond bool) bool {
	backlog := ackLevel - tgc.ackLevel
	if backlog >= int64(batchSize) {
		return true
	}
	return backlog > 0 && (ignoreTimeCond || time.Now().UTC().Sub(tgc.lastDeleteTime) > maxTimeBetweenTaskDeletes)
}

func (tgc *taskGC) tryLock() bool {
	return atomic.CompareAndSwapInt64(&tgc.lock, 0, 1)
}

func (tgc *taskGC) unlock() {
	atomic.StoreInt64(&tgc.lock, 0)
}
