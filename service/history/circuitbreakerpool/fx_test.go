package circuitbreakerpool

import (
	"fmt"
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/circuitbreaker"
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
				circuitbreaker.NewRecentFailures(trippingFailuresKept, trippingFailureMessageLength),
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

func TestOnStateChangeReportsTrippingFailures(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := logger.StartCapture()
	failures := circuitbreaker.NewRecentFailures(trippingFailuresKept, trippingFailureMessageLength)
	transition := onStateChange(tasks.TaskGroupNamespaceIDAndDestination{TaskGroup: chasmnexus.TaskGroupName}, "namespace", failures, logger)
	for i := range trippingFailuresKept + 1 {
		failures.RecordFailure(fmt.Sprintf("failure %d", i))
	}

	transition("", gobreaker.StateClosed, gobreaker.StateOpen)
	transition("", gobreaker.StateOpen, gobreaker.StateHalfOpen)

	records := capture.Snapshot()
	require.Len(t, records, 2)
	tagsOf := func(index int) map[string]any {
		tags := make(map[string]any, len(records[index].Tags))
		for _, actual := range records[index].Tags {
			tags[actual.Key()] = actual.Value()
		}
		return tags
	}
	opened := tagsOf(0)
	require.Equal(t, []any{"failure 1", "failure 2", "failure 3", "failure 4", "failure 5"}, opened["recent-failures"])
	require.EqualValues(t, trippingFailuresKept+1, opened["consecutive-failures"])
	halfOpen := tagsOf(1)
	require.NotContains(t, halfOpen, "recent-failures")
	require.NotContains(t, halfOpen, "consecutive-failures")
}
