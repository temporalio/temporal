package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/emptypb"
)

// The CHASM Workflow root component's data message changed from google.protobuf.Empty to
// WorkflowState. Node identity comes from the registered component type ID rather than the
// proto type, so the swap is invisible to the tree -- but the persisted bytes still have to
// survive it in both directions during a rolling upgrade. These tests pin that, going through
// the same codec chasm/tree.go's encodeChasmBlob uses.

// Forward: a workflow persisted by an older server decodes as an all-zero WorkflowState rather
// than failing, so it simply looks like a workflow with no callbacks yet.
func TestWorkflowStateDecodesLegacyEmptyBlob(t *testing.T) {
	legacy, err := serialization.Encode(&emptypb.Empty{}, serialization.WithDeterministicProto3)
	require.NoError(t, err)
	require.Empty(t, legacy.GetData(), "emptypb.Empty must serialize to zero bytes")

	var state chasmworkflowpb.WorkflowState
	require.NoError(t, serialization.Decode(legacy, &state))
	require.Zero(t, state.GetTotalCallbacksCount())
	require.Zero(t, state.GetTotalCallbacksSize())
}

// An absent blob is also legal: unmarshalProto substitutes an empty proto3 blob when a node has
// no data, which is the shape a Workflow node has today because NewWorkflow leaves the embedded
// message nil.
func TestWorkflowStateDecodesEmptyProto3Blob(t *testing.T) {
	var state chasmworkflowpb.WorkflowState
	require.NoError(t, serialization.Decode(&commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte{},
	}, &state))
	require.Zero(t, state.GetTotalCallbacksCount())
	require.Zero(t, state.GetTotalCallbacksSize())
}

// Backward, and the reason this file exists: during a rolling upgrade an old server can load a
// workflow a new server wrote, decode it into emptypb.Empty, and write it back. If that dropped
// the new fields the counters would silently reset to zero, which reads as "this workflow
// predates the counters" and triggers a spurious recompute. protobuf-go retains unknown fields
// across a round trip, so the totals survive -- assert it rather than trust it.
func TestWorkflowStateSurvivesRoundTripThroughLegacyEmpty(t *testing.T) {
	original := &chasmworkflowpb.WorkflowState{
		TotalCallbacksCount: 7,
		TotalCallbacksSize:  123456,
	}
	encoded, err := serialization.Encode(original, serialization.WithDeterministicProto3)
	require.NoError(t, err)

	// An old server decodes into the message it knows about, then re-persists it.
	var asLegacy emptypb.Empty
	require.NoError(t, serialization.Decode(encoded, &asLegacy))
	reEncoded, err := serialization.Encode(&asLegacy, serialization.WithDeterministicProto3)
	require.NoError(t, err)

	var recovered chasmworkflowpb.WorkflowState
	require.NoError(t, serialization.Decode(reEncoded, &recovered))
	require.Equal(t, int32(7), recovered.GetTotalCallbacksCount())
	require.Equal(t, int64(123456), recovered.GetTotalCallbacksSize())
}
