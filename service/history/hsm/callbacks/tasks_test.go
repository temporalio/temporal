package callbacks_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/hsm/callbacks"
	"go.temporal.io/server/service/history/tasks"
)

// tasks.Tags matches this library's task types by prefix, since it cannot import them.
func TestInvocationTaskTaggedAsHandlerOutbound(t *testing.T) {
	task := &tasks.StateMachineOutboundTask{
		StateMachineTask: tasks.StateMachineTask{Info: &persistencespb.StateMachineTaskInfo{Type: callbacks.TaskTypeInvocation}},
	}
	require.Contains(t, tasks.Tags(task), tag.NexusStageHandlerOutbound)
}
