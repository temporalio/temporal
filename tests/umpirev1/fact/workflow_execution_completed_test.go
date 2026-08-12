package fact

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
)

func TestWorkflowExecutionCompleted_ImportSpanEvent(t *testing.T) {
	attrs := attribute.NewSet(
		telemetry.AttrWorkflowID.String("wf1"),
		telemetry.AttrRunID.String("run1"),
	)
	f := &WorkflowExecutionCompleted{}
	require.True(t, f.ImportSpanEvent(attrs))
	require.Equal(t, "wf1", f.WorkflowID)
	require.Equal(t, "run1", f.RunID)
	require.Equal(t, telemetry.EventWorkflowExecutionCompleted, f.Name())

	ident := f.TargetEntity()
	require.NotNil(t, ident)
	require.Equal(t, WorkflowType, ident.EntityID.Type)
	require.Equal(t, "wf1", ident.EntityID.ID)
	require.Nil(t, ident.Parent())
}

func TestWorkflowExecutionCompleted_DiscardedWithoutWorkflowID(t *testing.T) {
	f := &WorkflowExecutionCompleted{}
	require.False(t, f.ImportSpanEvent(attribute.NewSet(telemetry.AttrRunID.String("run1"))))
	require.Nil(t, f.TargetEntity())
}
