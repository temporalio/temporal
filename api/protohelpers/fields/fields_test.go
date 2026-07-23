package fields_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/protohelpers/fields"
	"go.temporal.io/server/api/protohelpers/protofield"
)

// TestHandlesAreExhaustive fails if a proto field is added or removed without
// regenerating the handles (run `make proto`). This is the guard that keeps the
// generated field model in sync with the proto definitions.
func TestHandlesAreExhaustive(t *testing.T) {
	require.NoError(t, protofield.Verify(
		&workflowservice.StartWorkflowExecutionRequest{},
		fields.StartWorkflowExecutionRequest.All(),
	))
	require.NoError(t, protofield.Verify(
		&commonpb.Memo{},
		fields.Memo.All(),
	))
	// Command covers a message with a oneof group; Verify must reconcile the
	// single "attributes" handle against the descriptor's oneof, not its members.
	require.NoError(t, protofield.Verify(
		&commandpb.Command{},
		fields.Command.All(),
	))
}

// TestOneofHandle exercises the read-only oneof handle: presence detection and
// reading the set member's wrapper.
func TestOneofHandle(t *testing.T) {
	f := fields.Command.Attributes
	require.Equal(t, protofield.KindOneof, f.Kind())

	cmd := &commandpb.Command{}
	require.False(t, f.IsSet(cmd))
	require.Nil(t, f.Get(cmd))

	cmd.Attributes = &commandpb.Command_StartTimerCommandAttributes{
		StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{TimerId: "t"},
	}
	require.True(t, f.IsSet(cmd))
	require.NotNil(t, f.Get(cmd))
}

// TestHandleBehavior exercises the get/set/is-set closures across every kind
// (scalar, enum, message, repeated, map) to prove the generated handles operate
// on the underlying message.
func TestHandleBehavior(t *testing.T) {
	f := fields.StartWorkflowExecutionRequest
	req := &workflowservice.StartWorkflowExecutionRequest{}

	// Scalar.
	require.False(t, f.Namespace.IsSet(req))
	f.Namespace.Set(req, "ns")
	require.True(t, f.Namespace.IsSet(req))
	require.Equal(t, "ns", f.Namespace.Get(req))

	// Enum (a scalar kind).
	require.Equal(t, protofield.KindEnum, f.WorkflowIdReusePolicy.Kind())
	require.False(t, f.WorkflowIdReusePolicy.IsSet(req))
	f.WorkflowIdReusePolicy.Set(req, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	require.True(t, f.WorkflowIdReusePolicy.IsSet(req))

	// Message.
	require.False(t, f.WorkflowType.IsSet(req))
	f.WorkflowType.Set(req, &commonpb.WorkflowType{Name: "wf"})
	require.True(t, f.WorkflowType.IsSet(req))
	require.Equal(t, "wf", f.WorkflowType.Get(req).GetName())

	// Repeated.
	require.False(t, f.Links.IsSet(req))
	f.Links.Set(req, []*commonpb.Link{{}})
	require.True(t, f.Links.IsSet(req))
	require.Len(t, f.Links.Get(req), 1)

	// Map (on Memo).
	memo := &commonpb.Memo{}
	require.False(t, fields.Memo.Fields.IsSet(memo))
	fields.Memo.Fields.Set(memo, map[string]*commonpb.Payload{"k": {}})
	require.True(t, fields.Memo.Fields.IsSet(memo))
	require.Len(t, fields.Memo.Fields.Get(memo), 1)
}
