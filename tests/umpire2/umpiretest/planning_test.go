package umpiretest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2"
)

func TestPlanUsesCanonicalProtocolAndRecordsDefaults(t *testing.T) {
	protocol, err := umpire2.DefaultProtocol()
	require.NoError(t, err)

	result, err := Plan(protocol, PlanRequest{
		Target: PlanTarget{Entity: umpire2.WorkflowType, State: umpire2.WorkflowCompleted},
	})
	require.NoError(t, err)
	require.Equal(t, umpire.Shortest, result.Mode)
	require.Equal(t, int64(1), result.Seed)
	require.True(t, result.Plan.Reaches(umpire2.WorkflowCompleted))
	require.Len(t, result.Plan.Routes, 1)
}

func TestPlanRejectsInvalidIntent(t *testing.T) {
	protocol, err := umpire2.DefaultProtocol()
	require.NoError(t, err)

	_, err = Plan(nil, PlanRequest{Target: PlanTarget{Entity: umpire2.WorkflowType, State: umpire2.WorkflowCompleted}})
	require.ErrorContains(t, err, "protocol is nil")
	_, err = Plan(protocol, PlanRequest{})
	require.ErrorContains(t, err, "target entity is empty")
	_, err = Plan(protocol, PlanRequest{Target: PlanTarget{Entity: umpire2.WorkflowType}})
	require.ErrorContains(t, err, "target state is empty")
}
