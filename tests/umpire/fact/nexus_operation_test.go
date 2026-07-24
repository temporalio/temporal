package fact

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
)

func nexusEventAttrs(extra ...attribute.KeyValue) attribute.Set {
	base := []attribute.KeyValue{
		telemetry.AttrNexusScheduledEventID.String("5"),
		telemetry.AttrWorkflowID.String("wf1"),
		telemetry.AttrNamespaceID.String("ns1"),
	}
	return attribute.NewSet(append(base, extra...)...)
}

func TestNexusSpanFacts_ImportSpanEvent(t *testing.T) {
	attrs := nexusEventAttrs()

	started := &NexusOperationStarted{}
	require.True(t, started.ImportSpanEvent(attrs))
	require.Equal(t, "5", started.ScheduledEventID)
	require.Equal(t, "wf1", started.WorkflowID)
	require.Equal(t, telemetry.EventNexusOperationStarted, started.Name())

	// EntityPath routes the operation under its caller workflow, keyed "<wf>:<schedID>".
	ident := started.TargetEntity()
	require.NotNil(t, ident)
	require.Equal(t, NexusOperationType, ident.EntityID.Type)
	require.Equal(t, "wf1:5", ident.EntityID.ID)
	parent := ident.Parent()
	require.NotNil(t, parent)
	require.Equal(t, WorkflowType, parent.EntityID.Type)
	require.Equal(t, "wf1", parent.EntityID.ID)
}

func TestNexusTerminalFact_Outcome(t *testing.T) {
	f := &NexusOperationFailed{}
	require.True(t, f.ImportSpanEvent(nexusEventAttrs(
		telemetry.AttrNexusOutcome.String("handler_error"))))
	require.Equal(t, "handler_error", f.Outcome)
	require.Equal(t, telemetry.EventNexusOperationFailed, f.Name())
}

func TestNexusSpanFacts_DiscardedWithoutScheduledEventID(t *testing.T) {
	attrs := attribute.NewSet(telemetry.AttrWorkflowID.String("wf1"))
	f := &NexusOperationScheduled{}
	require.False(t, f.ImportSpanEvent(attrs))
	require.Nil(t, f.TargetEntity())
}
