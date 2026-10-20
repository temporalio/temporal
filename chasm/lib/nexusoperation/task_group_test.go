package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/tasks"
)

// tasks.Tags can't import this package, so it repeats TaskGroupName as a string literal.
// This test fails if the group is renamed without updating tasks.Tags.
func TestTaskGroupTaggedAsCallerOutbound(t *testing.T) {
	task := &tasks.ChasmTask{Category: tasks.CategoryOutbound, Info: &persistencespb.ChasmTaskInfo{}}
	task.SetOutboundTaskGroup(TaskGroupName)
	require.Contains(t, tasks.Tags(task), tag.NexusStageCallerOutbound)
}
