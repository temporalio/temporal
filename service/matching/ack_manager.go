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
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// Used to convert out of order acks into ackLevel movement.
type ackManager struct {
	sync.RWMutex
	backlogMgr       *backlogManagerImpl // accessing approximateBacklogCounter
	outstandingTasks *treemap.Map        // TaskID->acked
	readLevel        int64               // Maximum TaskID inserted into outstandingTasks
	ackLevel         int64               // Maximum TaskID below which all tasks are acked
	backlogCountHint atomic.Int64
	logger           log.Logger
}

func newAckManager(backlogMgr *backlogManagerImpl) ackManager {
	return ackManager{
		backlogMgr:       backlogMgr,
		logger:           backlogMgr.logger,
		outstandingTasks: treemap.NewWith(godsutils.Int64Comparator),
		readLevel:        -1,
		ackLevel:         -1}
}

// Registers task as in-flight and moves read level to it. Tasks can be added in increasing order of taskID only.
func (m *ackManager) addTask(taskID int64) {
	m.Lock()
	defer m.Unlock()
	if m.readLevel >= taskID {
		m.logger.Fatal("Next task ID is less than current read level.",
			tag.TaskID(taskID),
			tag.ReadLevel(m.readLevel))
	}
	m.readLevel = taskID
	if _, found := m.outstandingTasks.Get(taskID); found {
		m.logger.Fatal("Already present in outstanding tasks", tag.TaskID(taskID))
	}
	m.outstandingTasks.Put(taskID, false)
	m.backlogCountHint.Add(1)
}

func (m *ackManager) getReadLevel() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.readLevel
}

func (m *ackManager) setReadLevel(readLevel int64) {
	m.Lock()
	defer m.Unlock()
	m.readLevel = readLevel
}

func (m *ackManager) setReadLevelAfterGap(newReadLevel int64) {
	m.Lock()
	defer m.Unlock()
	if m.ackLevel == m.readLevel {
		// This is called after we read a range and find no tasks. The range we read was m.readLevel to newReadLevel.
		// (We know this because nothing should change m.readLevel except the getTasksPump loop itself, after initialization.
		// And getTasksPump doesn't start until it gets a signal from taskWriter that it's initialized the levels.)
		// If we've acked all tasks up to m.readLevel, and there are no tasks between that and newReadLevel, then we've
		// acked all tasks up to newReadLevel too. This lets us advance the ack level on a task queue with no activity
		// but where the rangeid has moved higher, to prevent excessive reads on the next load.
		m.ackLevel = newReadLevel
	}
	m.readLevel = newReadLevel
}

func (m *ackManager) getAckLevel() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.ackLevel
}

// Moves ack level to the new level if it is higher than the current one.
// Also updates the read level if it is lower than the ackLevel.
func (m *ackManager) setAckLevel(ackLevel int64) {
	m.Lock()
	defer m.Unlock()
	if ackLevel > m.ackLevel {
		m.ackLevel = ackLevel
	}
	if ackLevel > m.readLevel {
		m.readLevel = ackLevel
	}
}

func (m *ackManager) completeTask(taskID int64) int64 {
	m.Lock()
	defer m.Unlock()

	macked, found := m.outstandingTasks.Get(taskID)
	if !found {
		return m.ackLevel
	}

	acked := macked.(bool)
	if acked {
		// don't adjust ack level if nothing has changed
		return m.ackLevel
	}

	// TODO the ack level management should be done by a dedicated coroutine
	//  this is only a temporarily solution
	m.outstandingTasks.Put(taskID, true)
	m.backlogCountHint.Add(-1)

	// Adjust the ack level as far as we can
	var numberOfAckedTasks int64
	for {
		min, acked := m.outstandingTasks.Min()
		if min == nil || !acked.(bool) {
			break
		}
		m.ackLevel = min.(int64)
		m.outstandingTasks.Remove(min)
		numberOfAckedTasks += 1
	}
	if numberOfAckedTasks > 0 {
		m.backlogMgr.db.updateApproximateBacklogCount(-numberOfAckedTasks)
	}
	return m.ackLevel
}

func (m *ackManager) getBacklogCountHint() int64 {
	return m.backlogCountHint.Load()
}
