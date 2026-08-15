package umpire2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileKitchenSinkWorkflowRoutes(t *testing.T) {
	completed, err := compileKitchenSinkRoute("Workflow", []string{"start", "complete"}, "", "")
	require.NoError(t, err)
	completedActions := completed.GetWorkflowInput().GetInitialActions()[0].GetActions()
	require.Len(t, completedActions, 1)
	require.NotNil(t, completedActions[0].GetReturnResult())
	require.Nil(t, completed.GetClientSequence())

	started, err := compileKitchenSinkRoute("Workflow", []string{"start"}, "", "")
	require.NoError(t, err)
	startedActions := started.GetWorkflowInput().GetInitialActions()[0].GetActions()
	require.NotNil(t, startedActions[0].GetAwaitWorkflowState())
}

func TestCompileKitchenSinkNexusRoutes(t *testing.T) {
	completed, err := compileKitchenSinkRoute("NexusOperation", []string{"schedule", "succeed"}, "endpoint", "operation-name")
	require.NoError(t, err)
	operation := completed.GetWorkflowInput().GetInitialActions()[0].GetActions()[0].GetNexusOperation()
	require.Equal(t, "endpoint", operation.GetEndpoint())
	require.Equal(t, "operation-name", operation.GetOperation())
	require.NotNil(t, operation.GetAwaitableChoice().GetWaitFinish())

	started, err := compileKitchenSinkRoute("NexusOperation", []string{"schedule", "start"}, "endpoint", "")
	require.NoError(t, err)
	operation = started.GetWorkflowInput().GetInitialActions()[0].GetActions()[0].GetNexusOperation()
	require.Equal(t, "operation", operation.GetOperation())
	require.NotNil(t, operation.GetAwaitableChoice().GetWaitStarted())
}

func TestCompileKitchenSinkRejectsInvalidRoutes(t *testing.T) {
	for name, test := range map[string]struct {
		entity   string
		route    []string
		endpoint string
	}{
		"empty":                {entity: "Workflow"},
		"workflow start":       {entity: "Workflow", route: []string{"complete"}},
		"unknown entity":       {entity: "Nope", route: []string{"x"}},
		"unimplemented entity": {entity: "WorkflowUpdate", route: []string{"admit", "complete"}},
		"nexus endpoint":       {entity: "NexusOperation", route: []string{"schedule", "succeed"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := compileKitchenSinkRoute(test.entity, test.route, test.endpoint, "")
			require.Error(t, err)
		})
	}
}
