package migration

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSameTimePendingStartsReceiveUniqueIdentities(t *testing.T) {
	when := timestamppb.New(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	starts := []*schedulespb.BufferedStart{
		{NominalTime: when, ActualTime: when},
		{NominalTime: when, ActualTime: when},
	}

	converted := convertBufferedStartsLegacyToCHASM(
		starts,
		"namespace-id",
		"schedule-id",
		1,
		"workflow-id",
	)
	require.Len(t, converted, 2)
	require.NotEqual(t, converted[0].GetRequestId(), converted[1].GetRequestId(),
		"distinct migrated actions must have distinct completion identities")
	require.NotEqual(t, converted[0].GetWorkflowId(), converted[1].GetWorkflowId(),
		"distinct migrated actions must not collide at workflow start")

	// The first start keeps the undecorated workflow ID, so a migrated action
	// still dedups against one the V1 scheduler had already started.
	require.Equal(t,
		schedulerinternal.GenerateWorkflowID("workflow-id", when.AsTime()),
		converted[0].GetWorkflowId())
}

func TestMigratedStartsPreserveExistingIdentities(t *testing.T) {
	when := timestamppb.New(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	starts := []*schedulespb.BufferedStart{
		{NominalTime: when, ActualTime: when, RequestId: "req-0", WorkflowId: "wf-0"},
		{NominalTime: when, ActualTime: when, RequestId: "req-1", WorkflowId: "wf-1"},
	}

	converted := convertBufferedStartsLegacyToCHASM(
		starts,
		"namespace-id",
		"schedule-id",
		1,
		"workflow-id",
	)
	require.Len(t, converted, 2)
	for i, start := range converted {
		require.Equal(t, fmt.Sprintf("req-%d", i), start.GetRequestId(),
			"identities carried over from V1 must not be regenerated")
		require.Equal(t, fmt.Sprintf("wf-%d", i), start.GetWorkflowId(),
			"identities carried over from V1 must not be suffixed")
	}
}
