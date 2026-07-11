package fact

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
)

func updateEventAttrs(extra ...attribute.KeyValue) attribute.Set {
	base := []attribute.KeyValue{
		telemetry.AttrUpdateID.String("upd1"),
		telemetry.AttrWorkflowID.String("wf1"),
		telemetry.AttrRunID.String("run1"),
	}
	return attribute.NewSet(append(base, extra...)...)
}

func TestUpdateSpanFacts_ImportSpanEvent(t *testing.T) {
	attrs := updateEventAttrs()

	admitted := &WorkflowUpdateAdmitted{}
	require.True(t, admitted.ImportSpanEvent(attrs))
	require.Equal(t, "upd1", admitted.UpdateID)
	require.Equal(t, "wf1", admitted.WorkflowID)
	require.Equal(t, telemetry.EventWorkflowUpdateAdmitted, admitted.Name())

	// EntityPath routes the update under its parent workflow.
	ident := admitted.TargetEntity()
	require.NotNil(t, ident)
	require.Equal(t, WorkflowUpdateType, ident.EntityID.Type)
	require.Equal(t, "upd1", ident.EntityID.ID)
	require.NotNil(t, ident.ParentID)
	require.Equal(t, WorkflowType, ident.ParentID.Type)
	require.Equal(t, "wf1", ident.ParentID.ID)

	accepted := &WorkflowUpdateAccepted{}
	require.True(t, accepted.ImportSpanEvent(attrs))
	require.Equal(t, "upd1", accepted.UpdateID)

	rejected := &WorkflowUpdateRejected{}
	require.True(t, rejected.ImportSpanEvent(attrs))
	require.Equal(t, "upd1", rejected.UpdateID)
}

func TestWorkflowUpdateCompleted_Outcome(t *testing.T) {
	success := &WorkflowUpdateCompleted{}
	require.True(t, success.ImportSpanEvent(updateEventAttrs(
		telemetry.AttrUpdateOutcome.String(telemetry.UpdateOutcomeSuccess))))
	require.True(t, success.IsSuccess())

	failure := &WorkflowUpdateCompleted{}
	require.True(t, failure.ImportSpanEvent(updateEventAttrs(
		telemetry.AttrUpdateOutcome.String(telemetry.UpdateOutcomeFailure))))
	require.False(t, failure.IsSuccess())
}

func TestUpdateSpanFacts_DiscardedWithoutUpdateID(t *testing.T) {
	attrs := attribute.NewSet(telemetry.AttrWorkflowID.String("wf1"))
	f := &WorkflowUpdateAccepted{}
	require.False(t, f.ImportSpanEvent(attrs))
	require.Nil(t, f.TargetEntity())
}
