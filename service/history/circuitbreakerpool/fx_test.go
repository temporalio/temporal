package circuitbreakerpool

import (
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/testing/testlogger"
	hsmcallbacks "go.temporal.io/server/service/history/hsm/callbacks"
	hsmnexus "go.temporal.io/server/service/history/hsm/nexusoperations"
	"go.temporal.io/server/service/history/tasks"
)

func TestOnStateChangeNexusStage(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		taskGroup string
		stage     string
	}{
		{
			name:      "HSM caller invocation",
			taskGroup: hsmnexus.TaskTypeInvocation,
			stage:     "caller-outbound",
		},
		{
			name:      "HSM caller cancellation",
			taskGroup: hsmnexus.TaskTypeCancelation,
			stage:     "caller-outbound",
		},
		{
			name:      "CHASM caller outbound",
			taskGroup: chasmnexus.TaskGroupName,
			stage:     "caller-outbound",
		},
		{
			name:      "CHASM handler outbound",
			taskGroup: chasmcallback.InvocationTaskGroup,
			stage:     "handler-outbound",
		},
		{
			name:      "HSM handler outbound",
			taskGroup: hsmcallbacks.TaskTypeInvocation,
			stage:     "handler-outbound",
		},
		{
			name:      "non-Nexus task group",
			taskGroup: "other.Task",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			capture := logger.StartCapture()
			onStateChange(
				tasks.TaskGroupNamespaceIDAndDestination{TaskGroup: testCase.taskGroup},
				"namespace",
				logger,
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
