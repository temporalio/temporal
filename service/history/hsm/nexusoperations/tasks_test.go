package nexusoperations_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"go.temporal.io/server/service/history/tasks"
)

// tasks.Tags matches this library's task types by prefix, since it cannot import them.
func TestOutboundTasksTaggedAsCallerOutbound(t *testing.T) {
	for _, taskType := range []string{nexusoperations.TaskTypeInvocation, nexusoperations.TaskTypeCancelation} {
		task := &tasks.StateMachineOutboundTask{
			StateMachineTask: tasks.StateMachineTask{Info: &persistencespb.StateMachineTaskInfo{Type: taskType}},
		}
		require.Contains(t, tasks.Tags(task), tag.NexusStageCallerOutbound, taskType)
	}
}
