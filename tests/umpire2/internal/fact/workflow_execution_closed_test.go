package fact

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

func TestWorkflowExecutionClosedImportSpanEvent(t *testing.T) {
	for _, test := range []struct {
		name      string
		outcome   string
		successor string
	}{
		{name: "completed", outcome: "completed"},
		{name: "failed retry", outcome: "failed", successor: "retry-run"},
		{name: "canceled", outcome: "canceled"},
		{name: "terminated", outcome: "terminated"},
		{name: "timed out retry", outcome: "timed_out", successor: "retry-run"},
		{name: "continued as new", outcome: "continued_as_new", successor: "next-run"},
	} {
		t.Run(test.name, func(t *testing.T) {
			attrs := attribute.NewSet(
				telemetry.AttrNamespaceID.String("namespace-id"),
				telemetry.AttrWorkflowID.String("workflow-id"),
				telemetry.AttrRunID.String("run-id"),
				telemetry.AttrWorkflowCloseOutcome.String(test.outcome),
				telemetry.AttrWorkflowSuccessorRunID.String(test.successor),
			)

			closed := &WorkflowExecutionClosed{}
			require.True(t, closed.ImportSpanEvent(attrs))
			require.Equal(t, test.outcome, closed.Outcome)
			require.Equal(t, test.successor, closed.SuccessorRunID)
			require.Equal(t, nsPath(
				"namespace-id",
				umpire.NewEntityID(WorkflowType, "workflow-id"),
			), closed.TargetEntity())
		})
	}
}

func TestWorkflowExecutionClosedRejectsIncompleteIdentityAndOutcome(t *testing.T) {
	for _, attrs := range []attribute.Set{
		attribute.NewSet(telemetry.AttrWorkflowID.String("workflow-id"), telemetry.AttrRunID.String("run-id"), telemetry.AttrWorkflowCloseOutcome.String("completed")),
		attribute.NewSet(telemetry.AttrNamespaceID.String("namespace-id"), telemetry.AttrRunID.String("run-id"), telemetry.AttrWorkflowCloseOutcome.String("completed")),
		attribute.NewSet(telemetry.AttrNamespaceID.String("namespace-id"), telemetry.AttrWorkflowID.String("workflow-id"), telemetry.AttrWorkflowCloseOutcome.String("completed")),
		attribute.NewSet(telemetry.AttrNamespaceID.String("namespace-id"), telemetry.AttrWorkflowID.String("workflow-id"), telemetry.AttrRunID.String("run-id")),
		attribute.NewSet(telemetry.AttrNamespaceID.String("namespace-id"), telemetry.AttrWorkflowID.String("workflow-id"), telemetry.AttrRunID.String("run-id"), telemetry.AttrWorkflowCloseOutcome.String("unknown")),
	} {
		require.False(t, (&WorkflowExecutionClosed{}).ImportSpanEvent(attrs))
	}
}
