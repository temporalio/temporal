package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/definition"
)

func TestWorkerCommandsTask_GetDestination_ReturnsEmpty(t *testing.T) {
	task := &WorkerCommandsTask{
		WorkflowKey:  definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
		ControlQueue: "control-queue-xyz",
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("token")},
			}},
		},
	}
	// GetDestination returns "" for grouping; ControlQueue is used for routing.
	require.Empty(t, task.GetDestination())
	require.Equal(t, "control-queue-xyz", task.ControlQueue)
}

func TestWorkerCommandsTask_OutboundTaskGroup(t *testing.T) {
	task := &WorkerCommandsTask{}
	require.Equal(t, WorkerCommandsTaskGroup, task.OutboundTaskGroup())
	require.Equal(t, "worker_commands", task.OutboundTaskGroup())
}
