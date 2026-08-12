package fact

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
)

func chasmTransitionAttrs(componentType, path, requestID, destination string) attribute.Set {
	return attribute.NewSet(
		telemetry.AttrChasmComponentType.String(componentType),
		telemetry.AttrChasmComponentPath.String(path),
		telemetry.AttrNexusRequestID.String(requestID),
		telemetry.AttrChasmTransitionDestination.String(destination),
		telemetry.AttrWorkflowID.String("wf1"),
		telemetry.AttrNamespaceID.String("ns1"),
		telemetry.AttrRunID.String("run1"),
	)
}

func TestChasmTransition_RoutesNexusOperation(t *testing.T) {
	f := &ChasmTransition{}
	require.True(t, f.ImportSpanEvent(
		chasmTransitionAttrs("*nexusoperation.Operation", "Operations/5", "req-abc", "OPERATION_STATUS_STARTED")))
	require.True(t, f.IsNexusOperation())
	require.Equal(t, "OPERATION_STATUS_STARTED", f.Destination)
	require.Equal(t, telemetry.EventChasmTransition, f.Name())

	ident := f.TargetEntity()
	require.NotNil(t, ident)
	require.Equal(t, NexusOperationType, ident.EntityID.Type)
	require.Equal(t, "wf1:req-abc", ident.EntityID.ID) // "<workflowID>:<requestID>"
	require.Equal(t, WorkflowType, ident.Parent().EntityID.Type)
	require.Equal(t, "wf1", ident.Parent().EntityID.ID)
}

// A transition for a component the umpire doesn't model (e.g. the CHASM activity)
// is discarded, not misrouted.
func TestChasmTransition_IgnoresNonNexusComponent(t *testing.T) {
	f := &ChasmTransition{}
	require.False(t, f.ImportSpanEvent(
		chasmTransitionAttrs("*activity.Activity", "x", "req", "ACTIVITY_EXECUTION_STATUS_STARTED")))
	require.Nil(t, f.TargetEntity())
}

// The scheduling transition fires before the operation is attached to the tree (empty
// component path), but it still carries the stable request ID, so it routes to the
// operation's entity — the entity is created at "scheduled", not a step late.
func TestChasmTransition_SchedulingRoutesByRequestID(t *testing.T) {
	f := &ChasmTransition{}
	require.True(t, f.ImportSpanEvent(
		chasmTransitionAttrs("*nexusoperation.Operation", "", "req-xyz", "OPERATION_STATUS_SCHEDULED")))
	ident := f.TargetEntity()
	require.NotNil(t, ident)
	require.Equal(t, "wf1:req-xyz", ident.EntityID.ID)
}

// Without a request ID there is no per-operation identity to route on.
func TestChasmTransition_IgnoresMissingRequestID(t *testing.T) {
	f := &ChasmTransition{}
	require.False(t, f.ImportSpanEvent(
		chasmTransitionAttrs("*nexusoperation.Operation", "Operations/5", "", "OPERATION_STATUS_STARTED")))
	require.Nil(t, f.TargetEntity())
}
