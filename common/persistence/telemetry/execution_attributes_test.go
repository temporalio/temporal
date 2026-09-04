package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence"
)

type testExecutionIdentityProvider struct{}

func (testExecutionIdentityProvider) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return 4, chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  "business-id",
		RunID:       "run-id",
	}
}

func TestExecutionSpanAttributesUsesIdentityProvider(t *testing.T) {
	attrs := executionSpanAttributes(testExecutionIdentityProvider{})
	attrsByKey := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		attrsByKey[string(attr.Key)] = attr.Value.AsInterface()
	}

	require.Equal(t, map[string]any{
		"temporalBusinessID":       "business-id",
		"temporalChasmArchetypeID": int64(4),
		"temporalNamespaceID":      "namespace-id",
		"temporalRunID":            "run-id",
	}, attrsByKey)
}

func TestExecutionSpanAttributes(t *testing.T) {
	archetypeID := chasm.ArchetypeID(4)
	snapshot := persistence.InternalWorkflowSnapshot{
		NamespaceID: "namespace-id",
		WorkflowID:  "business-id",
		RunID:       "run-id",
	}
	mutation := persistence.InternalWorkflowMutation{
		NamespaceID: "namespace-id",
		WorkflowID:  "business-id",
		RunID:       "run-id",
	}
	for _, tc := range []struct {
		name       string
		request    any
		includeRun bool
	}{
		{
			name: "create",
			request: &persistence.InternalCreateWorkflowExecutionRequest{
				ArchetypeID:         archetypeID,
				NewWorkflowSnapshot: snapshot,
			},
			includeRun: true,
		},
		{
			name: "update",
			request: &persistence.InternalUpdateWorkflowExecutionRequest{
				ArchetypeID:            archetypeID,
				UpdateWorkflowMutation: mutation,
			},
			includeRun: true,
		},
		{
			name: "conflict resolve",
			request: &persistence.InternalConflictResolveWorkflowExecutionRequest{
				ArchetypeID:           archetypeID,
				ResetWorkflowSnapshot: snapshot,
			},
			includeRun: true,
		},
		{
			name: "set",
			request: &persistence.InternalSetWorkflowExecutionRequest{
				ArchetypeID:         archetypeID,
				SetWorkflowSnapshot: snapshot,
			},
			includeRun: true,
		},
		{
			name: "get current",
			request: &persistence.GetCurrentExecutionRequest{
				ArchetypeID: archetypeID,
				NamespaceID: "namespace-id",
				WorkflowID:  "business-id",
			},
		},
		{
			name: "get",
			request: &persistence.GetWorkflowExecutionRequest{
				ArchetypeID: archetypeID,
				NamespaceID: "namespace-id",
				WorkflowID:  "business-id",
				RunID:       "run-id",
			},
			includeRun: true,
		},
		{
			name: "delete current",
			request: &persistence.DeleteCurrentWorkflowExecutionRequest{
				ArchetypeID: archetypeID,
				NamespaceID: "namespace-id",
				WorkflowID:  "business-id",
				RunID:       "run-id",
			},
			includeRun: true,
		},
		{
			name: "delete",
			request: &persistence.DeleteWorkflowExecutionRequest{
				ArchetypeID: archetypeID,
				NamespaceID: "namespace-id",
				WorkflowID:  "business-id",
				RunID:       "run-id",
			},
			includeRun: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attrs := executionSpanAttributes(tc.request)
			attrsByKey := make(map[string]any, len(attrs))
			for _, attr := range attrs {
				attrsByKey[string(attr.Key)] = attr.Value.AsInterface()
			}
			expected := map[string]any{
				"temporalBusinessID":       "business-id",
				"temporalChasmArchetypeID": int64(4),
				"temporalNamespaceID":      "namespace-id",
			}
			if tc.includeRun {
				expected["temporalRunID"] = "run-id"
			}
			require.Equal(t, expected, attrsByKey)
		})
	}

	t.Run("workflow execution", func(t *testing.T) {
		attrs := executionSpanAttributes(&persistence.GetWorkflowExecutionRequest{
			NamespaceID: "namespace-id",
			WorkflowID:  "workflow-id",
			RunID:       "run-id",
		})

		require.Empty(t, attrs)
	})
}
