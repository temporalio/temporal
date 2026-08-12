package regress_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/regress"
)

var (
	nexusOperation = regress.EntityType("NexusOperation")
	workflowRun    = regress.EntityType("WorkflowRun")
	runID          = regress.ValueType("RunID")

	nexusState = regress.OutcomeSchema(
		"nexus.state",
		regress.SymbolParameter("operation", nexusOperation),
		regress.LiteralParameter("state", regress.ValueType("NexusState")),
	)
	nexusCancel = regress.ActionSchema(
		"nexus.cancel",
		regress.SymbolParameter("operation", nexusOperation),
	)
	workflowState = regress.OutcomeSchema(
		"workflow.state",
		regress.SymbolParameter("workflow", workflowRun),
		regress.LiteralParameter("state", regress.ValueType("WorkflowState")),
	)
	workflowRunID = regress.ProjectionSchema(
		"workflow.run_id",
		runID,
		regress.SymbolParameter("workflow", workflowRun),
	)
	dropRPC = regress.PolicySchema(
		"nexus.drop",
		regress.LiteralParameter("rpc", regress.ValueType("RPC")),
	)
)

func TestNormalizeDeclaresAndReusesTypedSymbols(t *testing.T) {
	plan := regress.OnePath(
		regress.Outcome(nexusState, regress.Symbol("op"), regress.Literal("started")),
		regress.Action(nexusCancel, regress.Symbol("op")),
	)

	ir, err := regress.Normalize(plan)
	require.NoError(t, err)
	require.Equal(t, regress.Symbols{
		"op": {
			Name:        "op",
			Type:        nexusOperation,
			FirstSource: 1,
			Uses:        []int{1, 2},
		},
	}, ir.Symbols)
	require.Len(t, ir.Nodes, 2)
	require.Equal(t, []regress.Edge{{From: 0, To: 1}}, ir.Edges)
}

func TestNormalizeReportsBothSourcesForSymbolTypeConflict(t *testing.T) {
	plan := regress.OnePath(
		regress.Outcome(nexusState, regress.Symbol("target"), regress.Literal("started")),
		regress.Outcome(workflowState, regress.Symbol("target"), regress.Literal("started")),
	)

	_, err := regress.Normalize(plan)
	require.Error(t, err)
	require.ErrorIs(t, err, regress.ErrSymbolTypeConflict)
	require.Equal(t, &regress.CompileError{
		Category: regress.ErrorSymbolTypeConflict,
		Source:   2,
		Related:  []int{1},
		Symbol:   "target",
		Expected: workflowRun.String(),
		Actual:   nexusOperation.String(),
	}, err)
}

func TestNormalizeBindsProjectedValues(t *testing.T) {
	plan := regress.OnePath(
		regress.Bind("run", regress.Project(workflowRunID, regress.Symbol("handler"))),
	)

	ir, err := regress.Normalize(plan)
	require.NoError(t, err)
	require.Equal(t, runID, ir.Symbols["run"].Type)
	require.Equal(t, workflowRun, ir.Symbols["handler"].Type)
	require.Equal(t, "run", ir.Nodes[0].Binding)
}

func TestNormalizeAnyOrderPreservesOnlySurroundingOrder(t *testing.T) {
	plan := regress.OnePath(
		state("first"),
		regress.AnyOrder(state("left"), state("right")),
		state("last"),
	)

	ir, err := regress.Normalize(plan)
	require.NoError(t, err)
	require.Equal(t, []regress.Edge{
		{From: 0, To: 1},
		{From: 0, To: 2},
		{From: 1, To: 3},
		{From: 2, To: 3},
	}, ir.Edges)
}

func TestNormalizeDuringScopesPolicyOverBody(t *testing.T) {
	plan := regress.OnePath(
		state("before"),
		regress.During(
			regress.Policy(dropRPC, regress.Literal("CancelNexusOperation")),
			state("cancel"),
			state("canceled"),
		),
		state("after"),
	)

	ir, err := regress.Normalize(plan)
	require.NoError(t, err)
	require.Equal(t, []regress.Edge{
		{From: 0, To: 1},
		{From: 1, To: 2},
		{From: 2, To: 3},
	}, ir.Edges)
	require.Equal(t, []regress.Scope{{
		ID: 0,
		Policy: regress.PolicyIR{
			Source:    2,
			Name:      "nexus.drop",
			Arguments: []regress.Argument{regress.Literal("CancelNexusOperation")},
		},
		Body: []int{1, 2},
	}}, ir.Scopes)
}

func TestNormalizeBeforeAddsLabeledEdges(t *testing.T) {
	plan := regress.OnePath(regress.AnyOrder(
		regress.Step("a", state("a")),
		regress.Step("b", state("b")),
		regress.Step("c", state("c")),
		regress.Before("a", "c"),
		regress.Before("b", "c"),
	))

	ir, err := regress.Normalize(plan)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"a": 0, "b": 1, "c": 2}, ir.Labels)
	require.Equal(t, []regress.Edge{{From: 0, To: 2}, {From: 1, To: 2}}, ir.Edges)
}

func TestNormalizeRejectsOrderingCycle(t *testing.T) {
	plan := regress.OnePath(regress.AnyOrder(
		regress.Step("a", state("a")),
		regress.Step("b", state("b")),
		regress.Before("a", "b"),
		regress.Before("b", "a"),
	))

	_, err := regress.Normalize(plan)
	require.ErrorIs(t, err, regress.ErrContradictoryOrdering)
}

func TestNormalizeExpandsBoundedRepeat(t *testing.T) {
	ir, err := regress.Normalize(regress.OnePath(regress.Repeat(2, state("op"))))
	require.NoError(t, err)
	require.Len(t, ir.Nodes, 2)
	require.Equal(t, []regress.Edge{{From: 0, To: 1}}, ir.Edges)
}

func TestNormalizeRejectsUnboundedRepeat(t *testing.T) {
	_, err := regress.Normalize(regress.AllPaths(regress.Repeat(0, state("op"))))
	require.ErrorIs(t, err, regress.ErrUnboundedCycle)
}

func TestNormalizeRejectsPolicyOutsideDuring(t *testing.T) {
	_, err := regress.Normalize(regress.OnePath(
		regress.Policy(dropRPC, regress.Literal("CancelNexusOperation")),
	))

	require.ErrorIs(t, err, regress.ErrInvalidPolicyLifetime)
}

func TestNormalizeLowersRequirementsOutsideTheMilestoneDAG(t *testing.T) {
	chasm := regress.RequirementSchema("CHASM")

	ir, err := regress.Normalize(regress.OnePath(
		regress.Require(chasm),
		state("op"),
	))
	require.NoError(t, err)
	require.Len(t, ir.Nodes, 1)
	require.Equal(t, []regress.RequirementIR{{Source: 1, Name: "CHASM"}}, ir.Requirements)
	require.Empty(t, ir.Edges)
}

func TestIRArtifactRepresentationRoundTrips(t *testing.T) {
	ir, err := regress.Normalize(regress.AllPaths(
		regress.AnyOrder(state("left"), state("right")),
	))
	require.NoError(t, err)

	encoded, err := regress.MarshalIR(ir)
	require.NoError(t, err)
	decoded, err := regress.UnmarshalIR(encoded)
	require.NoError(t, err)
	require.Equal(t, ir, decoded)
	require.Equal(t, string(encoded), decoded.String())
}

func state(symbol string) regress.Instruction {
	return regress.Outcome(nexusState, regress.Symbol(symbol), regress.Literal("started"))
}
