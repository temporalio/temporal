package interceptor

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	// Blank imports register chasm/lib service file descriptors into protoregistry.GlobalFiles.
	_ "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	_ "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

// resetExcludedAPIs resets the package-level sync.Once and map so each test starts fresh.
func resetExcludedAPIs(t *testing.T) {
	t.Helper()
	excludedAPIsOnce = sync.Once{}
	excludedAPIs = nil
}

func TestIsExcludedAPI_HistoryService(t *testing.T) {
	resetExcludedAPIs(t)

	// PollMutableState is API_CATEGORY_LONG_POLL in historyservice
	assert.True(t, isExcludedAPI("/temporal.server.api.historyservice.v1.HistoryService/PollMutableState"))
	// RecordActivityTaskHeartbeat is API_CATEGORY_STANDARD
	assert.False(t, isExcludedAPI("/temporal.server.api.historyservice.v1.HistoryService/RecordActivityTaskHeartbeat"))
}

func TestIsExcludedAPI_ChasmActivityService(t *testing.T) {
	resetExcludedAPIs(t)

	// PollActivityExecution is API_CATEGORY_LONG_POLL — must be excluded
	assert.True(t, isExcludedAPI("/temporal.server.chasm.lib.activity.proto.v1.ActivityService/PollActivityExecution"))
	// StartActivityExecution is API_CATEGORY_STANDARD — must not be excluded
	assert.False(t, isExcludedAPI("/temporal.server.chasm.lib.activity.proto.v1.ActivityService/StartActivityExecution"))
}

func TestIsExcludedAPI_ChasmSchedulerService(t *testing.T) {
	resetExcludedAPIs(t)

	// All SchedulerService methods are API_CATEGORY_STANDARD — none excluded
	assert.False(t, isExcludedAPI("/temporal.server.chasm.lib.scheduler.proto.v1.SchedulerService/CreateSchedule"))
	assert.False(t, isExcludedAPI("/temporal.server.chasm.lib.scheduler.proto.v1.SchedulerService/DeleteSchedule"))
}

func TestIsExcludedAPI_UnknownMethod(t *testing.T) {
	resetExcludedAPIs(t)

	assert.False(t, isExcludedAPI("/some.unknown.Service/DoSomething"))
}
