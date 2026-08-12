package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpirev1/action"
)

// TestFaultTargetsExcludesEntryAndAmbient pins the reduction the learned-footprint fault targeting
// relies on: a plan's own Entry RPC (whose drop just fails the drive) and ambient traffic
// (long-polls, cluster metadata) are excluded; the internal calls remain the resilience targets.
func TestFaultTargetsExcludesEntryAndAmbient(t *testing.T) {
	plan := []umpire.Action{action.StartStandalone} // Entry: StartNexusOperationExecution
	const (
		entry     = "/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperationExecution"
		ambient   = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
		internalA = "/temporal.server.api.historyservice.v1.HistoryService/RespondWorkflowTaskCompleted"
		internalB = "/temporal.server.api.matchingservice.v1.MatchingService/UpdateWorkflowExecution"
	)
	targets := action.FaultTargets(plan, []string{entry, ambient, internalA, internalB})
	require.ElementsMatch(t, []string{internalA, internalB}, targets)
}

// TestFaultVariantsFromLearnedFootprint checks the variants derive from the reduced footprint: one
// plan copy per internal target with a Drop prepended, entry/ambient calls contributing none.
func TestFaultVariantsFromLearnedFootprint(t *testing.T) {
	plan := []umpire.Action{action.StartStandalone}
	learned := []string{
		"/x/StartNexusOperationExecution", // entry → excluded
		"/x/RespondWorkflowTaskCompleted", // internal → one variant
		"/x/GetSystemInfo",                // ambient → excluded
	}
	variants := action.FaultVariants(plan, learned)
	require.Len(t, variants, 1, "one variant per learned target")
	require.Equal(t, umpire.Fault, variants[0][0].Kind, "the target's Drop is prepended")
	require.Equal(t, names(plan), names(variants[0][1:]), "the rest is the original plan")
}
