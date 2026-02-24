//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_receiver_flow_controller_mock.go
package replication

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/configs"
)

type (
	FlowControlSignalProvider func() *FlowControlSignal

	// FlowControlSignal holds signals to make flow control decision, more signalsProvider can be added here i.e. total persistence rps, cpu usage etc.
	FlowControlSignal struct {
		taskTrackingCount  int
		lastSlowSubmission time.Time
	}

	// FlowControlInfo holds the flow control command and an optional cause string for logging when command is PAUSE.
	FlowControlInfo struct {
		Command enumsspb.ReplicationFlowControlCommand
		Cause   string
	}

	ReceiverFlowController interface {
		GetFlowControlInfo(priority enumsspb.TaskPriority) FlowControlInfo
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

func (s *streamReceiverFlowControllerImpl) GetFlowControlInfo(priority enumsspb.TaskPriority) FlowControlInfo {
	if signal, ok := s.signalsProvider[priority]; ok {
		signalData := signal()
		limit := s.config.ReplicationReceiverMaxOutstandingTaskCount()

		if signalData.taskTrackingCount > limit {
			return FlowControlInfo{
				Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
				Cause:   fmt.Sprintf("task count %d > limit %d", signalData.taskTrackingCount, limit),
			}
		}

		if s.config.EnableReplicationReceiverSlowSubmissionFlowControl() {
			now := time.Now()
			slowSubmissionWindow := now.Add(-s.config.ReplicationReceiverSlowSubmissionWindow())
			if signalData.lastSlowSubmission.After(slowSubmissionWindow) {
				return FlowControlInfo{
					Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
					Cause:   fmt.Sprintf("slow submission within window (last at %v)", signalData.lastSlowSubmission),
				}
			}
		}
	}
	return FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME}
}
