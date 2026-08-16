package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/action"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

func names(seq []umpire.Action) []string {
	out := make([]string, len(seq))
	for i, a := range seq {
		out[i] = a.Name
	}
	return out
}

// TestPlanEdge shows the actions-model planner assembling action sequences from the model and
// the registry — the hand-written plans are what it computes, and hosting/from-state pick the
// right action where an event has several producers.
func TestPlanEdge(t *testing.T) {
	cases := []struct {
		name    string
		from    string
		event   string
		hosting umpire.Hosting
		want    []string
	}{
		{
			name: "embedded async completion (succeed from started)",
			from: model.NexusStarted, event: model.NexusSucceed, hosting: umpire.Embedded,
			want: []string{"cmd:ScheduleNexusOperation", "handler:AsyncAck", "callback:Complete"},
		},
		{
			name: "standalone async completion (succeed from started)",
			from: model.NexusStarted, event: model.NexusSucceed, hosting: umpire.Standalone,
			want: []string{"StartNexusOperationExecution", "handler:AsyncAck", "callback:Complete"},
		},
		{
			name: "sync success (succeed from scheduled) uses the handler, not a callback",
			from: model.NexusScheduled, event: model.NexusSucceed, hosting: umpire.Embedded,
			want: []string{"cmd:ScheduleNexusOperation", "handler:SyncOk"},
		},
		{
			name: "timeout from backing_off routes through a retryable failure",
			from: model.NexusBackingOff, event: model.NexusTimeout, hosting: umpire.Embedded,
			want: []string{"cmd:ScheduleNexusOperation", "handler:RetryableError", "timer:ForceTimeout(backing_off)"},
		},
		{
			name: "terminate from started is standalone-only",
			from: model.NexusStarted, event: model.NexusTerminate, hosting: umpire.Standalone,
			want: []string{"StartNexusOperationExecution", "handler:AsyncAck", "TerminateNexusOperationExecution"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seq, err := action.PlanEdge(tc.from, tc.event, tc.hosting)
			require.NoError(t, err)
			require.Equal(t, tc.want, names(seq))
		})
	}
}

// TestPlanEdge_TerminateNeedsStandalone shows the hosting constraint flows through: terminate
// is unreachable under Embedded (the entity planner drops the standalone-only edge).
func TestPlanEdge_TerminateNeedsStandalone(t *testing.T) {
	_, err := action.PlanEdge(model.NexusStarted, model.NexusTerminate, umpire.Embedded)
	require.Error(t, err)
}
