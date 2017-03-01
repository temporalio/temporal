package matching

import "github.com/uber-common/bark"

// Used to convert out of order acks into ackLevel movement.
type ackManager struct {
	logger bark.Logger

	outstandingTasks map[int64]bool // key->TaskID, value->(true for acked, false->for non acked)
	readLevel        int64          // Maximum TaskID inserted into outstandingTasks
	ackLevel         int64          // Maximum TaskID below which all tasks are acked
}

// Registers task as in-flight and moves read level to it. Tasks can be added in increasing order of taskID only.
func (m *ackManager) addTask(taskID int64) {
	if m.readLevel >= taskID {
		m.logger.Fatalf("Next task ID is less than current read level.  TaskID: %v, ReadLevel: %v", taskID,
			m.readLevel)
	}
	m.readLevel = taskID
	if _, ok := m.outstandingTasks[taskID]; ok {
		m.logger.Fatalf("Already present in outstanding tasks: taskID=%v", taskID)
	}
	m.outstandingTasks[taskID] = false // true is for acked
}

func newAckManager(logger bark.Logger) ackManager {
	return ackManager{logger: logger, outstandingTasks: make(map[int64]bool), readLevel: -1, ackLevel: -1}
}

func (m *ackManager) getReadLevel() int64 {
	return m.readLevel
}

func (m *ackManager) getAckLevel() int64 {
	return m.ackLevel
}

// Moves ack level to the new level if it is higher than the current one.
// Also updates the read level it is lower than the ackLevel.
func (m *ackManager) setAckLevel(ackLevel int64) {
	if ackLevel > m.ackLevel {
		m.ackLevel = ackLevel
	}
	if ackLevel > m.readLevel {
		m.readLevel = ackLevel
	}
}

func (m *ackManager) completeTask(taskID int64) (ackLevel int64) {
	if _, ok := m.outstandingTasks[taskID]; ok {
		m.outstandingTasks[taskID] = true
	}
	// Update ackLevel
	for current := m.ackLevel + 1; current <= m.readLevel; current++ {
		if acked, ok := m.outstandingTasks[current]; ok {
			if acked {
				m.ackLevel = current
				delete(m.outstandingTasks, current)
			} else {
				return m.ackLevel
			}
		}
	}
	return m.ackLevel
}
