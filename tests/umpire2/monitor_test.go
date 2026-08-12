package umpire2

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
)

func startedWorkflowIn(namespaceID, workflowID string) umpirefw.Fact {
	return &fact.WorkflowStarted{
		Request: &historyservice.StartWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId: workflowID,
			},
		},
		EntityPath: &umpirefw.EntityPath{
			EntityID:  umpirefw.NewEntityID(model.WorkflowType, workflowID),
			Ancestors: []umpirefw.EntityID{umpirefw.NewEntityID(model.NamespaceType, namespaceID)},
		},
	}
}

func countWorkflows(u *Monitor, namespaceID string) int {
	root := umpirefw.NewEntityID(model.NamespaceType, namespaceID)
	return len(u.ModelState().QueryEntities(model.WorkflowType, 0, &root))
}

func TestMonitor_CheckNamespace_IsScopedAndPurgeable(t *testing.T) {
	ctx := context.Background()
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)

	const nsA, nsB = "ns-a", "ns-b"
	require.NoError(t, u.ModelState().RouteFacts(ctx, []umpirefw.Fact{
		startedWorkflowIn(nsA, "wf-a"),
		startedWorkflowIn(nsB, "wf-b"),
	}))
	require.Equal(t, 1, countWorkflows(u, nsA))
	require.Equal(t, 1, countWorkflows(u, nsB))
	rootA := umpirefw.NewEntityID(model.NamespaceType, nsA)
	entries := u.ModelState().QueryEntities(model.WorkflowType, 0, &rootA)
	require.Len(t, entries, 1)
	workflow, ok := entries[0].Entity.(*model.Workflow)
	require.True(t, ok)
	require.Equal(t, model.WorkflowStarted, workflow.Lifecycle().Current())

	// Checking namespace A must only surface A's stuck workflow, never B's.
	violations := u.CheckNamespace(ctx, nsA)
	require.NotEmpty(t, violations, "expected a violation for the started workflow in namespace A")
	for _, v := range violations {
		tags := fmt.Sprintf("%v", v.Tags)
		require.NotContains(t, tags, "wf-b", "namespace A check leaked into another namespace")
	}

	// Purging A drops only A's data; B is untouched.
	u.PurgeNamespace(nsA)
	require.Equal(t, 0, countWorkflows(u, nsA))
	require.Equal(t, 1, countWorkflows(u, nsB))

	// A re-check of the purged namespace finds nothing.
	require.Empty(t, u.CheckNamespace(ctx, nsA))
}

func TestMonitor_CheckNamespaceSafetyDoesNotPromotePendingLiveness(t *testing.T) {
	ctx := context.Background()
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)

	const namespaceID = "ns"
	require.NoError(t, u.ModelState().RouteFacts(ctx, []umpirefw.Fact{
		startedWorkflowIn(namespaceID, "in-flight"),
	}))

	require.Empty(t, u.CheckNamespaceSafety(ctx, namespaceID))
	require.NotEmpty(t, u.CheckNamespace(ctx, namespaceID))
}

func TestMonitorRoutesAndPurgesProtocolRelations(t *testing.T) {
	ctx := context.Background()
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)
	const namespaceID = "namespace"
	started := &fact.WorkflowRunStarted{
		NamespaceID: namespaceID,
		WorkflowID:  "workflow",
		RunID:       "run",
		FirstRunID:  "run",
		EntityPath: &umpirefw.EntityPath{
			EntityID: umpirefw.NewEntityID(model.WorkflowRunType, "run"),
			Ancestors: []umpirefw.EntityID{
				umpirefw.NewEntityID(model.NamespaceType, namespaceID),
				umpirefw.NewEntityID(model.WorkflowType, "workflow"),
			},
		},
	}

	require.NoError(t, u.routeFacts(ctx, []umpirefw.Fact{started}))
	require.Equal(t, []umpirefw.EntityID{
		umpirefw.NewEntityID(model.WorkflowRunType, namespaceID+"\x00run"),
	}, u.Relations().Targets(
		protocol.WorkflowRunsRelation,
		umpirefw.NewEntityID(model.WorkflowType, namespaceID+"\x00workflow"),
	))

	u.PurgeNamespace(namespaceID)
	require.Empty(t, u.Relations().Snapshot())
}

func TestMonitorNexusActivityConsistencyUsesProtocolRelations(t *testing.T) {
	ctx := context.Background()
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)
	const namespaceID = "namespace"
	activityLink := &commonpb.Link{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{ActivityId: "activity"}}}
	require.NoError(t, u.routeFacts(ctx, []umpirefw.Fact{
		fact.NewNexusOperationExecutionSnapshot(namespaceID, "operation", []*commonpb.Link{activityLink}),
		fact.NewActivityExecutionSnapshot(namespaceID, "activity", enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, nil),
	}))

	root := umpirefw.NewEntityID(model.NamespaceType, namespaceID)
	for _, entry := range u.ModelState().QueryEntities(model.NexusOperationType, 0, &root) {
		entry.Entity.(*model.NexusOperation).Links = nil
	}

	violations := u.CheckNamespaceSafety(ctx, namespaceID)
	require.Len(t, violations, 1)
	require.Equal(t, "NexusActivityLinkConsistencyRule", violations[0].Rule)
}

func TestMonitorRecordsOptionalSemanticCoverage(t *testing.T) {
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)
	coverage, err := umpirefw.NewCoverage(true)
	require.NoError(t, err)
	u.SetCoverage(coverage)
	const namespaceID = "namespace"
	started := &fact.WorkflowRunStarted{
		NamespaceID: namespaceID,
		WorkflowID:  "workflow",
		RunID:       "run",
		FirstRunID:  "run",
		EntityPath: &umpirefw.EntityPath{
			EntityID: umpirefw.NewEntityID(model.WorkflowRunType, "run"),
			Ancestors: []umpirefw.EntityID{
				umpirefw.NewEntityID(model.NamespaceType, namespaceID),
				umpirefw.NewEntityID(model.WorkflowType, "workflow"),
			},
		},
	}

	require.NoError(t, u.routeFacts(context.Background(), []umpirefw.Fact{started}))
	points := coverage.Snapshot()
	require.True(t, slices.Contains(points, umpirefw.CoveragePoint{Kind: umpirefw.CoverageFact, ID: "WorkflowRunStarted"}))
	require.True(t, slices.Contains(points, umpirefw.CoveragePoint{Kind: umpirefw.CoverageRelation, ID: "workflow-runs"}))
	require.True(t, slices.Contains(points, umpirefw.CoveragePoint{Kind: umpirefw.CoverageTransition, ID: "WorkflowRun:created/start/started"}))

	u.CheckNamespaceSafety(context.Background(), namespaceID)
	require.True(t, slices.Contains(coverage.Snapshot(), umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleEvaluated, ID: "NexusActivityLinkConsistencyRule"}))
}

func TestMonitorRecordsOptionalNormalizedTrace(t *testing.T) {
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)
	recorder := umpirefw.NewTraceRecorder(umpirefw.TraceOptions{MaxEvents: 20, MaxBytes: 8192})
	u.SetTraceRecorder(recorder)
	const namespaceID = "namespace"
	started := &fact.WorkflowRunStarted{
		NamespaceID: namespaceID,
		WorkflowID:  "workflow",
		RunID:       "run",
		FirstRunID:  "run",
		EntityPath: &umpirefw.EntityPath{
			EntityID: umpirefw.NewEntityID(model.WorkflowRunType, "run"),
			Ancestors: []umpirefw.EntityID{
				umpirefw.NewEntityID(model.NamespaceType, namespaceID),
				umpirefw.NewEntityID(model.WorkflowType, "workflow"),
			},
		},
	}

	require.NoError(t, u.routeFacts(context.Background(), []umpirefw.Fact{started}))
	trace := recorder.Snapshot()
	require.True(t, traceContains(trace, umpirefw.TraceFact, "WorkflowRunStarted"))
	require.True(t, traceContains(trace, umpirefw.TraceTransition, "WorkflowRun:created/start/started"))
	require.True(t, traceContains(trace, umpirefw.TraceRelation, "workflow-runs"))
}

func traceContains(trace umpirefw.Trace, kind umpirefw.TraceKind, name string) bool {
	return slices.ContainsFunc(trace.Events, func(event umpirefw.TraceEvent) bool {
		return event.Kind == kind && event.Name == name
	})
}
