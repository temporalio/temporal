package umpiretest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

type judgeMonitor struct {
	snapshot   umpire.Snapshot
	violations []umpire.Violation
}

func (m *judgeMonitor) Snapshot(string) umpire.Snapshot {
	return m.snapshot
}

func (m *judgeMonitor) CheckNamespace(context.Context, string) []umpire.Violation {
	return m.violations
}

func (m *judgeMonitor) ObservationSummary(string) string {
	return "observed test snapshot"
}

func TestJudgeMatchesExactlyOneEntity(t *testing.T) {
	monitor := &judgeMonitor{snapshot: umpire.Snapshot{Entities: []umpire.EntitySnapshot{{
		Type: umpire.EntityType("Workflow"), ID: "workflow-id", Current: "completed", Terminal: true,
	}}}}

	result, err := Judge(t.Context(), JudgmentRequest{
		Monitor: monitor, NamespaceID: "namespace-id",
		Expectation: Expectation{Subject: EntitySelector{Entity: umpire.EntityType("Workflow"), ID: "workflow-id"}, State: "completed"},
		Profile:     umpire.InProcessProfile(), Timeout: time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, umpire.ClaimEstablished, result.Claim.Status)
	require.Equal(t, "workflow-id", result.Matched.ID)
	require.Equal(t, MatchExactlyOne, result.Cardinality)
}

func TestJudgeResolvesSymbolicBinding(t *testing.T) {
	monitor := &judgeMonitor{snapshot: umpire.Snapshot{Entities: []umpire.EntitySnapshot{{
		Type: umpire.EntityType("Workflow"), ID: "workflow-id", Current: "completed",
	}}}}

	result, err := Judge(t.Context(), JudgmentRequest{
		Monitor: monitor, NamespaceID: "namespace-id", Bindings: map[string]string{"workflow": "workflow-id"},
		Expectation: Expectation{Subject: EntitySelector{Entity: umpire.EntityType("Workflow"), Ref: "workflow"}, State: "completed"},
		Profile:     umpire.InProcessProfile(), Timeout: time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, umpire.ClaimEstablished, result.Claim.Status)
	require.Equal(t, "workflow-id", result.Matched.ID)
}

func TestJudgeReportsAmbiguousIdentityAsInconclusive(t *testing.T) {
	monitor := &judgeMonitor{snapshot: umpire.Snapshot{Entities: []umpire.EntitySnapshot{
		{Type: umpire.EntityType("Workflow"), ID: "one", Current: "completed"},
		{Type: umpire.EntityType("Workflow"), ID: "two", Current: "completed"},
	}}}

	result, err := Judge(t.Context(), JudgmentRequest{
		Monitor: monitor, NamespaceID: "namespace-id",
		Expectation: Expectation{Subject: EntitySelector{Entity: umpire.EntityType("Workflow")}, State: "completed"},
		Profile:     umpire.InProcessProfile(), Timeout: time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, MatchMultiple, result.Cardinality)
	require.Equal(t, umpire.ClaimInconclusive, result.Claim.Status)
	require.Contains(t, result.Claim.Omissions, "identity:ambiguous")
}

func TestJudgeReportsBoundedMissingTargetAsViolation(t *testing.T) {
	monitor := &judgeMonitor{}

	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	result, err := Judge(ctx, JudgmentRequest{
		Monitor: monitor, NamespaceID: "namespace-id",
		Expectation: Expectation{Subject: EntitySelector{Entity: umpire.EntityType("Workflow")}, State: "completed"},
		Profile:     umpire.InProcessProfile(), Timeout: time.Hour, PollInterval: time.Microsecond,
	})
	require.NoError(t, err)
	require.Equal(t, MatchNone, result.Cardinality)
	require.Equal(t, umpire.ClaimViolated, result.Claim.Status)
	require.True(t, result.TimedOut)
	require.Contains(t, result.Diagnostic, "no matching entity")
}

func TestJudgeRejectsInvalidSelectorBeforePolling(t *testing.T) {
	_, err := Judge(t.Context(), JudgmentRequest{
		Monitor: &judgeMonitor{}, NamespaceID: "namespace-id",
		Expectation: Expectation{Subject: EntitySelector{Entity: umpire.EntityType("Workflow"), Ref: "workflow", ID: "workflow-id"}, State: "completed"},
		Profile:     umpire.InProcessProfile(), Timeout: time.Second,
	})
	require.ErrorContains(t, err, "cannot set both Ref and ID")
}
