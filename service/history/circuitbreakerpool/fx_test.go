package circuitbreakerpool

import (
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
)

func TestOnStateChangeTagsOnlyNexusTaskGroups(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		taskGroup     string
		wantComponent bool
	}{
		{name: "HSM invocation", taskGroup: "nexusoperations.Invocation", wantComponent: true},
		{name: "HSM cancellation", taskGroup: "nexusoperations.Cancelation", wantComponent: true},
		{name: "CHASM operation", taskGroup: "nexus", wantComponent: true},
		{name: "callback", taskGroup: "callbacks.Invocation"},
		{name: "unknown", taskGroup: "unknown"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			capture := logger.StartCapture()
			onStateChange(tasks.TaskGroupNamespaceIDAndDestination{
				TaskGroup:   tc.taskGroup,
				NamespaceID: "namespace-id",
				Destination: "destination",
			}, "namespace", logger)("ignored", gobreaker.StateClosed, gobreaker.StateOpen)

			records := capture.Snapshot()
			require.Len(t, records, 1)
			if tc.wantComponent {
				require.Contains(t, records[0].Tags, tag.NewStringTag("component", "nexus-outbound"))
			} else {
				require.NotContains(t, records[0].Tags, tag.NewStringTag("component", "nexus-outbound"))
			}
		})
	}
}
