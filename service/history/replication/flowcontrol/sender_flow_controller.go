package flowcontrol

import (
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
)

type (
	SenderFlowController interface {
		Acquire(priority enums.TaskPriority) error
		UpdateReceiverFlowControlInfo(syncState adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState)
	}
	senderFlowControllerImpl struct {
	}
)

func NewSenderFlowController() SenderFlowController {
	return &senderFlowControllerImpl{}
}

func (s senderFlowControllerImpl) UpdateReceiverFlowControlInfo(syncState adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState) {
	//TODO implement me
	panic("implement me")
}

func (s senderFlowControllerImpl) Acquire(priority enums.TaskPriority) error {
	//TODO implement me
	panic("implement me")
}
