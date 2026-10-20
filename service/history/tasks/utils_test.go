package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
)

func TestTags_NexusStage(t *testing.T) {
	t.Parallel()

	workflowKey := definition.NewWorkflowKey("ns-id", "wf-id", "run-id")
	hsmOutbound := func(taskType string) Task {
		return &StateMachineOutboundTask{
			StateMachineTask: StateMachineTask{
				WorkflowKey: workflowKey,
				Info:        &persistencespb.StateMachineTaskInfo{Type: taskType},
			},
			Destination: "endpoint",
		}
	}
	chasmTask := func(category Category, group string) Task {
		task := &ChasmTask{
			WorkflowKey: workflowKey,
			Category:    category,
			Info:        &persistencespb.ChasmTaskInfo{},
		}
		task.SetOutboundTaskGroup(group)
		return task
	}

	for _, tc := range []struct {
		name      string
		task      Task
		wantStage string
	}{
		{name: "HSM Nexus invocation", task: hsmOutbound("nexusoperations.Invocation"), wantStage: "caller-outbound"},
		{name: "HSM Nexus cancelation", task: hsmOutbound("nexusoperations.Cancelation"), wantStage: "caller-outbound"},
		{name: "CHASM Nexus operation", task: chasmTask(CategoryOutbound, "nexus"), wantStage: "caller-outbound"},
		{name: "HSM callback", task: hsmOutbound("callbacks.Invocation")},
		{name: "CHASM task with an unrelated group", task: chasmTask(CategoryOutbound, "callback.invocation")},
		{name: "CHASM Nexus group outside the outbound queue", task: chasmTask(CategoryTimer, "nexus")},
		{name: "worker commands", task: &WorkerCommandsTask{WorkflowKey: workflowKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stages []string
			for _, logTag := range Tags(tc.task) {
				if logTag.Key() == tag.NexusStageCallerOutbound.Key() {
					stages = append(stages, logTag.Value().(string))
				}
			}
			if tc.wantStage == "" {
				require.Empty(t, stages)
			} else {
				require.Equal(t, []string{tc.wantStage}, stages)
			}
		})
	}
}
