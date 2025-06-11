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
	db               *taskQueueDB // to update approximateBacklogCount
	outstandingTasks *treemap.Map // TaskID->acked
	readLevel        int64        // Maximum TaskID inserted into outstandingTasks
	ackLevel         int64        // Maximum TaskID below which all tasks are acked
	backlogCountHint atomic.Int64 // TODO(pri): old matcher cleanup, task reader tracks this now
	logger           log.Logger
}

func newAckManager(db *taskQueueDB, logger log.Logger) *ackManager {
	return &ackManager{
		db:               db,
		logger:           logger,
		outstandingTasks: treemap.NewWith(godsutils.Int64Comparator),
		readLevel:        -1,
		ackLevel:         -1,
	}
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

func (m *ackManager) completeTask(taskID int64) (newAckLevel, numberOfAckedTasks int64) {
	m.Lock()
	defer m.Unlock()

	macked, found := m.outstandingTasks.Get(taskID)
	if !found {
		return m.ackLevel, 0
	}

	acked := macked.(bool)
	if acked {
		// don't adjust ack level if nothing has changed
		return m.ackLevel, 0
	}

	// TODO the ack level management should be done by a dedicated coroutine
	//  this is only a temporarily solution
	m.outstandingTasks.Put(taskID, true)
	m.backlogCountHint.Add(-1)

	// Adjust the ack level as far as we can
	for {
		min, acked := m.outstandingTasks.Min()
		if min == nil || !acked.(bool) {
			break
		}
		m.ackLevel = min.(int64)
		m.outstandingTasks.Remove(min)
		numberOfAckedTasks += 1
	}
	return m.ackLevel, numberOfAckedTasks
}

func (m *ackManager) getBacklogCountHint() int64 {
	return m.backlogCountHint.Load()
}
