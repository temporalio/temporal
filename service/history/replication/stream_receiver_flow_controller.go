//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_receiver_flow_controller_mock.go
package replication

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/configs"
)

type (
	FlowControlSignalProvider func() *FlowControlSignal

	// FlowControlSignal holds signals to make flow control decision, more signalsProvider can be added here i.e. total persistence rps, cpu usage etc.
	FlowControlSignal struct {
		taskTrackingCount int
	}
	ReceiverFlowController interface {
		GetFlowControlInfo(priority enumsspb.TaskPriority) enumsspb.ReplicationFlowControlCommand
	}
	streamReceiverFlowControllerImpl struct {
		signalsProvider map[enumsspb.TaskPriority]FlowControlSignalProvider
		config          *configs.Config
	}
)

func NewReceiverFlowControl(signals map[enumsspb.TaskPriority]FlowControlSignalProvider, config *configs.Config) *streamReceiverFlowControllerImpl {
	return &streamReceiverFlowControllerImpl{
		signalsProvider: signals,
		config:          config,
	}
}

func (s *streamReceiverFlowControllerImpl) GetFlowControlInfo(priority enumsspb.TaskPriority) enumsspb.ReplicationFlowControlCommand {
	if signal, ok := s.signalsProvider[priority]; ok {
		if signal().taskTrackingCount > s.config.ReplicationReceiverMaxOutstandingTaskCount() {
			return enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
		}
	}
	return enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
}
