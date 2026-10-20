package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/tasks"
)

// tasks.Tags matches this library's task group by name, since it cannot import it.
func TestTaskGroupTaggedAsCallerOutbound(t *testing.T) {
	task := &tasks.ChasmTask{Category: tasks.CategoryOutbound, Info: &persistencespb.ChasmTaskInfo{}}
	task.SetOutboundTaskGroup(TaskGroupName)
	require.Contains(t, tasks.Tags(task), tag.NexusStageCallerOutbound)
}
