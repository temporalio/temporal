package umpire2

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
	"go.temporal.io/server/tests/umpire2/internal/model"
	"go.temporal.io/server/tests/umpire2/internal/protocol"
)

func newTestExecutionTrace(t *testing.T) *executionTrace {
	t.Helper()
	compiled, err := protocol.Default()
	require.NoError(t, err)
	runtime, err := umpirefw.NewRuntime(compiled.RuntimeDeclaration(nil))
	require.NoError(t, err)
	return newExecutionTrace(runtime, compiled.CausalFootprints())
}

func TestExecutionTraceRetainsActionWindowWhenRecorderRejectsFinish(t *testing.T) {
	trace := newTestExecutionTrace(t)
	trace.setRecorder(umpirefw.NewTraceRecorder(umpirefw.TraceOptions{MaxEvents: 1}))
	const namespaceID = "namespace"
	require.NoError(t, trace.observeExecution(umpirefw.ExecutionObservation{
		Kind: umpirefw.ExecutionActionStart, Scope: namespaceID, Action: "action", Outcome: umpirefw.ExecutionOutcomeStarted,
	}))

	err := trace.observeExecution(umpirefw.ExecutionObservation{
		Kind: umpirefw.ExecutionActionFinish, Scope: namespaceID, Action: "action", Outcome: umpirefw.ExecutionOutcomeFailed, ErrorClass: "error",
	})
	require.ErrorIs(t, err, umpirefw.ErrTraceLimit)
	require.Len(t, trace.active[namespaceID]["action"], 1)
}

func TestExecutionTraceReconcilesDeclaredCausalFootprintAtActionFinish(t *testing.T) {
	trace := newTestExecutionTrace(t)
	trace.setRecorder(umpirefw.NewTraceRecorder(umpirefw.TraceOptions{MaxEvents: 10, MaxBytes: 8192}))
	const namespaceID = "namespace"
	start := umpirefw.ExecutionObservation{
		Kind: umpirefw.ExecutionActionStart, Scope: namespaceID,
		Action: "nexus.respond_start.scheduled.sync", Outcome: umpirefw.ExecutionOutcomeStarted,
	}
	finish := umpirefw.ExecutionObservation{
		Kind: umpirefw.ExecutionActionFinish, Scope: namespaceID,
		Action: "nexus.respond_start.scheduled.sync", Outcome: umpirefw.ExecutionOutcomeSucceeded,
	}
	require.NoError(t, trace.observeExecution(start))
	require.NoError(t, trace.recordFacts([]umpirefw.Fact{&fact.NexusOperationTerminal{
		NamespaceID: namespaceID,
		WorkflowID:  "workflow",
		EntityPath: &umpirefw.EntityPath{
			EntityID:  umpirefw.NewEntityID(model.NexusOperationType, "operation"),
			Ancestors: []umpirefw.EntityID{umpirefw.NewEntityID(model.NamespaceType, namespaceID)},
		},
	}}))
	require.NoError(t, trace.observeExecution(finish))

	trace.setRecorder(umpirefw.NewTraceRecorder(umpirefw.TraceOptions{MaxEvents: 10, MaxBytes: 8192}))
	require.NoError(t, trace.observeExecution(start))
	err := trace.observeExecution(finish)
	require.ErrorContains(t, err, "causal observation is missing")
}

func TestExecutionTracePurgesActiveScope(t *testing.T) {
	trace := newTestExecutionTrace(t)
	recorder := umpirefw.NewTraceRecorder(umpirefw.TraceOptions{MaxEvents: 10, MaxBytes: 8192})
	trace.setRecorder(recorder)
	require.NoError(t, trace.observeExecution(umpirefw.ExecutionObservation{
		Kind: umpirefw.ExecutionActionStart, Scope: "namespace", Action: "action", Outcome: umpirefw.ExecutionOutcomeStarted,
	}))

	trace.purgeScope("namespace")
	require.NoError(t, trace.recordFacts([]umpirefw.Fact{startedWorkflowIn("namespace", "workflow")}))

	events := recorder.Snapshot().Events
	require.Empty(t, events[len(events)-1].Causes)
}

func TestExecutionTraceRecordsQualifiedFailingVerdict(t *testing.T) {
	trace := newTestExecutionTrace(t)
	recorder := umpirefw.NewTraceRecorder(umpirefw.TraceOptions{MaxEvents: 10, MaxBytes: 8192})
	trace.setRecorder(recorder)
	property := umpirefw.MonitorSafetyProperty("action")

	require.NoError(t, trace.observeExecution(umpirefw.ExecutionObservation{
		Kind: umpirefw.ExecutionVerdict, Scope: "namespace", Checkpoint: "action", Property: property,
		Pass: false, Violations: 1,
	}))

	retained := recorder.Snapshot()
	retained.Complete = true
	require.NoError(t, umpirefw.ValidateTraceEvidence(retained, umpirefw.InProcessProfile()))
	require.Equal(t, umpirefw.TraceEvent{
		Key: "verdict:1", Kind: umpirefw.TraceVerdict, Name: property, Source: umpirefw.InProcessEvidence,
		Fields: map[string]string{"scope": "namespace", "checkpoint": "action", "pass": "false", "violations": "1"},
	}, retained.Events[0])
}
