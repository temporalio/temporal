package startworkflow

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	testDeriveNamespaceID = "my-namespace-1234-5678-9abc-def012345678"
	testDeriveWorkflowID  = "my-workflow-id"
	testDeriveRequestID   = "my-request-id"
	testDeriveArchetypeID = 1
	testDeriveClusterID   = 1
)

func TestDeriveRunID_Deterministic(t *testing.T) {
	got := DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID)
	require.Equal(t, "291b333c-b896-5d8c-a196-d791dfe6ca28", got)
	second := DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID)
	require.Equal(t, got, second)

	parsed, err := uuid.Parse(got)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(5), parsed.Version())
}

func TestDeriveRunID_EveryFieldMatters(t *testing.T) {
	base := DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID)

	for name, got := range map[string]string{
		"different namespaceID": DeriveRunID("11111111-2222-3333-4444-555555555555", testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID),
		"different workflowID":  DeriveRunID(testDeriveNamespaceID, "other-workflow-id", testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID),
		"different archetypeID": DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID+1, testDeriveRequestID, testDeriveClusterID),
		"different requestID":   DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, "other-request-id", testDeriveClusterID),
		"empty requestID":       DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, "", testDeriveClusterID),
		"different clusterID":   DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID+1),
	} {
		require.NotEqual(t, base, got, "%s must derive a different run ID", name)
	}
}

// TestDeriveRunID_FieldsAreUnambiguouslyFramed guards the length-prefixing.
func TestDeriveRunID_FieldsAreUnambiguouslyFramed(t *testing.T) {
	left := DeriveRunID(testDeriveNamespaceID, "a", testDeriveArchetypeID, "bc", testDeriveClusterID)
	right := DeriveRunID(testDeriveNamespaceID, "ab", testDeriveArchetypeID, "c", testDeriveClusterID)
	require.NotEqual(t, left, right)
}
