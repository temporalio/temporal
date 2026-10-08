package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/definition"
)

func TestWorkerCommandsTask_GetDestination_ReturnsFixedValue(t *testing.T) {
	task := &WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
		Destination: "control-queue-xyz",
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("token")},
			}},
		},
	}
	// GetDestination returns fixed value for grouping, not the control queue.
	require.Equal(t, WorkerCommandsTaskDestination, task.GetDestination())
	require.NotEqual(t, task.Destination, task.GetDestination())
}

func TestWorkerCommandsTask_OutboundTaskGroup(t *testing.T) {
	task := &WorkerCommandsTask{}
	require.Equal(t, WorkerCommandsTaskGroup, task.OutboundTaskGroup())
	require.Equal(t, "worker_commands", task.OutboundTaskGroup())
}
