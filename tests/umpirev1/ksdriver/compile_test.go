package ksdriver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A Workflow route ending in "complete" compiles to a program that returns a result —
// the workflow reaches started then completed.
func TestCompile_Workflow_StartComplete(t *testing.T) {
	ti, err := Compile("Workflow", []string{"start", "complete"})
	require.NoError(t, err)
	require.Len(t, ti.GetWorkflowInput().GetInitialActions(), 1)
	acts := ti.GetWorkflowInput().GetInitialActions()[0].GetActions()
	require.Len(t, acts, 1)
	require.NotNil(t, acts[0].GetReturnResult(), "start→complete must return a result")
	require.Nil(t, ti.GetClientSequence(), "no client actions needed for a plain complete")
}

// A Workflow route stopping at "start" compiles to a program that blocks, so the workflow
// stays in "started" for the Monitor to observe.
func TestCompile_Workflow_StartOnly_Holds(t *testing.T) {
	ti, err := Compile("Workflow", []string{"start"})
	require.NoError(t, err)
	acts := ti.GetWorkflowInput().GetInitialActions()[0].GetActions()
	require.NotNil(t, acts[0].GetAwaitWorkflowState(), "start-only must block to stay started")
}

func TestCompile_Workflow_MustStartWithStart(t *testing.T) {
	_, err := Compile("Workflow", []string{"complete"})
	require.Error(t, err)
}

func TestCompile_UnknownEntity(t *testing.T) {
	_, err := Compile("Nope", []string{"x"})
	require.Error(t, err)
}

func TestCompile_UnimplementedEntity(t *testing.T) {
	_, err := Compile("WorkflowUpdate", []string{"admit", "complete"})
	require.Error(t, err)
}

// A NexusOperation route compiles to a workflow that schedules an ExecuteNexusOperation
// against the given endpoint. A terminal route (schedule→succeed) awaits completion.
func TestCompile_Nexus_ScheduleSucceed(t *testing.T) {
	ti, err := Compile("NexusOperation", []string{"schedule", "succeed"},
		WithNexus("my-endpoint", "my-op"))
	require.NoError(t, err)
	acts := ti.GetWorkflowInput().GetInitialActions()[0].GetActions()
	require.Len(t, acts, 1)
	op := acts[0].GetNexusOperation()
	require.NotNil(t, op, "must schedule a nexus operation")
	require.Equal(t, "my-endpoint", op.GetEndpoint())
	require.Equal(t, "my-op", op.GetOperation())
	require.NotNil(t, op.GetAwaitableChoice().GetWaitFinish(), "terminal route awaits completion")
}

// A route stopping at "start" waits for STARTED only, not completion.
func TestCompile_Nexus_ScheduleStart_WaitsStarted(t *testing.T) {
	ti, err := Compile("NexusOperation", []string{"schedule", "start"}, WithNexus("ep", ""))
	require.NoError(t, err)
	op := ti.GetWorkflowInput().GetInitialActions()[0].GetActions()[0].GetNexusOperation()
	require.NotNil(t, op.GetAwaitableChoice().GetWaitStarted(), "start-only route waits for STARTED")
	require.Equal(t, "operation", op.GetOperation(), "empty operation defaults")
}

func TestCompile_Nexus_RequiresEndpoint(t *testing.T) {
	_, err := Compile("NexusOperation", []string{"schedule", "succeed"})
	require.Error(t, err)
}
