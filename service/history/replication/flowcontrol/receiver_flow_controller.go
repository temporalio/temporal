package flowcontrol

import (
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/replication"
)

const MaxOutstandingTasks = 100

type (
	ReceiverFlowController interface {
		GetFlowControlInfo(priority enums.TaskPriority) enums.ReplicationFlowControlCommand
	}
	streamReceiverFlowControllerImpl struct {
		taskTrackers map[enums.TaskPriority]replication.ExecutableTaskTracker
	}
)

func NewReceiverFlowControl(taskTrackers map[enums.TaskPriority]replication.ExecutableTaskTracker) *streamReceiverFlowControllerImpl {
	return &streamReceiverFlowControllerImpl{
		taskTrackers: taskTrackers,
	}
}

func (s *streamReceiverFlowControllerImpl) GetFlowControlInfo(priority enums.TaskPriority) enums.ReplicationFlowControlCommand {
	if taskTracker, ok := s.taskTrackers[priority]; ok {
		if taskTracker.Size() > 100 {
			return enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
		}
	}
	return enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
}
