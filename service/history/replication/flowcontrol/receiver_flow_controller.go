package flowcontrol

import "go.temporal.io/server/api/enums/v1"

type (
	Info struct {
		command enums.ReplicationFlowControlCommand
	}
	ReceiverFlowController interface {
		GetFlowControlInfo(priority enums.TaskPriority) Info
	}
	streamReceiverFlowControllerImpl struct {
	}
)

func NewReceiverFlowControl() ReceiverFlowController {
	return &streamReceiverFlowControllerImpl{}
}

func (s streamReceiverFlowControllerImpl) GetFlowControlInfo(priority enums.TaskPriority) Info {
	//TODO implement me
	panic("implement me")
}
