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

	"go.uber.org/atomic"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/util"
)

// Used to convert out of order acks into ackLevel movement.
type ackManager struct {
	sync.RWMutex
	outstandingTasks map[int64]bool // key->TaskID, value->(true for acked, false->for non acked)
	readLevel        int64          // Maximum TaskID inserted into outstandingTasks
	ackLevel         int64          // Maximum TaskID below which all tasks are acked
	backlogCounter   atomic.Int64
	logger           log.Logger
}

func newAckManager(logger log.Logger) ackManager {
	return ackManager{logger: logger, outstandingTasks: make(map[int64]bool), readLevel: -1, ackLevel: -1}
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
	if _, ok := m.outstandingTasks[taskID]; ok {
		m.logger.Fatal("Already present in outstanding tasks", tag.TaskID(taskID))
	}
	m.outstandingTasks[taskID] = false // true is for acked
	m.backlogCounter.Inc()
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

func (m *ackManager) completeTask(taskID int64) (ackLevel int64) {
	m.Lock()
	defer m.Unlock()
	if completed, ok := m.outstandingTasks[taskID]; ok && !completed {
		m.outstandingTasks[taskID] = true
		m.backlogCounter.Dec()
	}

	// TODO the ack level management shuld be done by a dedicated coroutine
	//  this is only a temporarily solution

	taskIDs := maps.Keys(m.outstandingTasks)
	util.SortSlice(taskIDs)

	// Update ackLevel
	for _, taskID := range taskIDs {
		if acked := m.outstandingTasks[taskID]; acked {
			m.ackLevel = taskID
			delete(m.outstandingTasks, taskID)
		} else {
			return m.ackLevel
		}
	}
	return m.ackLevel
}

func (m *ackManager) getBacklogCountHint() int64 {
	return m.backlogCounter.Load()
}
