package nexusworkflowref

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/components/nexusoperations"
)

const (
	testNamespaceID      = "namespace-id"
	testWorkflowID       = "workflow-id"
	testRunID            = "run-id"
	testScheduledEventID = int64(42)
	testRequestID        = "request-id"
)

func hsmToken() *tokenspb.NexusOperationCompletion {
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: testNamespaceID,
		WorkflowId:  testWorkflowID,
		RunId:       testRunID,
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{
				Type: nexusoperations.OperationMachineType,
				Id:   strconv.FormatInt(testScheduledEventID, 10),
			}},
		},
		RequestId: testRequestID,
	}
}

func chasmToken(t *testing.T, archetypeID uint32, path []string) *tokenspb.NexusOperationCompletion {
	t.Helper()
	ref, err := (&persistencespb.ChasmComponentRef{
		NamespaceId:   testNamespaceID,
		BusinessId:    testWorkflowID,
		RunId:         testRunID,
		ArchetypeId:   archetypeID,
		ComponentPath: path,
	}).Marshal()
	require.NoError(t, err)
	return &tokenspb.NexusOperationCompletion{ComponentRef: ref, RequestId: testRequestID}
}

func TestHSMRefToCHASMRef(t *testing.T) {
	t.Parallel()

	chasmCompletion, err := HSMRefToCHASMRef(hsmToken())
	require.NoError(t, err)
	require.NotEmpty(t, chasmCompletion.GetComponentRef())
	require.Nil(t, chasmCompletion.GetRef())
	require.Equal(t, testRequestID, chasmCompletion.GetRequestId())

	var ref persistencespb.ChasmComponentRef
	require.NoError(t, ref.Unmarshal(chasmCompletion.GetComponentRef()))
	require.Equal(t, testNamespaceID, ref.GetNamespaceId())
	require.Equal(t, testWorkflowID, ref.GetBusinessId())
	require.Equal(t, testRunID, ref.GetRunId())
	require.Equal(t, chasm.WorkflowArchetypeID, ref.GetArchetypeId())
	require.Equal(t, []string{operationsFieldName, strconv.FormatInt(testScheduledEventID, 10)}, ref.GetComponentPath())
}

func TestHSMRefToCHASMRef_NoOperationRef(t *testing.T) {
	t.Parallel()

	_, err := HSMRefToCHASMRef(&tokenspb.NexusOperationCompletion{RequestId: testRequestID})
	require.Error(t, err)
}

func TestCHASMRefToHSMRef(t *testing.T) {
	t.Parallel()

	token := chasmToken(t, chasm.WorkflowArchetypeID, []string{operationsFieldName, strconv.FormatInt(testScheduledEventID, 10)})
	hsmCompletion, err := CHASMRefToHSMRef(token)
	require.NoError(t, err)
	require.Empty(t, hsmCompletion.GetComponentRef())
	require.Equal(t, testNamespaceID, hsmCompletion.GetNamespaceId())
	require.Equal(t, testWorkflowID, hsmCompletion.GetWorkflowId())
	require.Equal(t, testRunID, hsmCompletion.GetRunId())
	require.Equal(t, testRequestID, hsmCompletion.GetRequestId())

	path := hsmCompletion.GetRef().GetPath()
	require.Len(t, path, 1)
	require.Equal(t, nexusoperations.OperationMachineType, path[0].GetType())
	require.Equal(t, strconv.FormatInt(testScheduledEventID, 10), path[0].GetId())
	// Non-nil versioned transitions so the HSM completion handler's reset fallback does not nil-panic.
	require.NotNil(t, hsmCompletion.GetRef().GetMachineInitialVersionedTransition())
	require.NotNil(t, hsmCompletion.GetRef().GetMachineLastUpdateVersionedTransition())
}

func TestCHASMRefToHSMRef_RejectsNonWorkflowArchetype(t *testing.T) {
	t.Parallel()

	token := chasmToken(t, chasm.WorkflowArchetypeID+1, []string{operationsFieldName, strconv.FormatInt(testScheduledEventID, 10)})
	_, err := CHASMRefToHSMRef(token)
	require.ErrorContains(t, err, "not workflow-backed")
}

func TestCHASMRefToHSMRef_RejectsUnexpectedPath(t *testing.T) {
	t.Parallel()

	token := chasmToken(t, chasm.WorkflowArchetypeID, []string{"NotOperations", "42"})
	_, err := CHASMRefToHSMRef(token)
	require.ErrorContains(t, err, "unexpected nexus operation component path")
}

func TestRoundTripHSMToCHASMToHSM(t *testing.T) {
	t.Parallel()

	chasmCompletion, err := HSMRefToCHASMRef(hsmToken())
	require.NoError(t, err)
	hsmCompletion, err := CHASMRefToHSMRef(chasmCompletion)
	require.NoError(t, err)

	require.Equal(t, testNamespaceID, hsmCompletion.GetNamespaceId())
	require.Equal(t, testWorkflowID, hsmCompletion.GetWorkflowId())
	require.Equal(t, testRunID, hsmCompletion.GetRunId())
	require.Equal(t, testRequestID, hsmCompletion.GetRequestId())
	require.Equal(t, strconv.FormatInt(testScheduledEventID, 10), hsmCompletion.GetRef().GetPath()[0].GetId())
}
