package circuitbreakerpool

import (
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/tasks"
)

func TestOnStateChangeNexusStage(t *testing.T) {
	testCases := []struct {
		name      string
		taskGroup string
		stage     string
	}{
		{
			name:      "HSM caller invocation",
			taskGroup: nexusoperations.TaskTypeInvocation,
			stage:     "caller-outbound",
		},
		{
			name:      "HSM caller cancellation",
			taskGroup: nexusoperations.TaskTypeCancelation,
			stage:     "caller-outbound",
		},
		{
			name:      "CHASM caller outbound",
			taskGroup: chasmnexus.TaskGroupName,
			stage:     "caller-outbound",
		},
		{
			name:      "CHASM handler outbound",
			taskGroup: "callback.invoke",
			stage:     "handler-outbound",
		},
		{
			name:      "shared HSM callback",
			taskGroup: callbacks.TaskTypeInvocation,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			capture := logger.StartCapture()
			onStateChange(
				tasks.TaskGroupNamespaceIDAndDestination{TaskGroup: testCase.taskGroup},
				"namespace",
				log.With(logger, tag.ComponentOutboundQueue),
			)("", gobreaker.StateClosed, gobreaker.StateOpen)

			records := capture.Snapshot()
			require.Len(t, records, 1)
			actualTags := make(map[string]any, len(records[0].Tags))
			for _, actual := range records[0].Tags {
				actualTags[actual.Key()] = actual.Value()
			}
			require.Equal(t, "outbound-queue-processor", actualTags["component"])
			if testCase.stage == "" {
				require.NotContains(t, actualTags, "nexus-stage")
			} else {
				require.Equal(t, testCase.stage, actualTags["nexus-stage"])
			}
		})
	}
}
