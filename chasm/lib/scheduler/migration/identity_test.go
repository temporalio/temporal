package migration

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
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
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
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
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	)
	require.Len(t, converted, 2)
	for i, start := range converted {
		require.Equal(t, fmt.Sprintf("req-%d", i), start.GetRequestId(),
			"identities carried over from V1 must not be regenerated")
		require.Equal(t, fmt.Sprintf("wf-%d", i), start.GetWorkflowId(),
			"identities carried over from V1 must not be suffixed")
	}
}

func TestMigratedStartsResolveOverlapPolicy(t *testing.T) {
	when := timestamppb.New(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	tests := []struct {
		name           string
		startPolicy    enumspb.ScheduleOverlapPolicy
		schedulePolicy enumspb.ScheduleOverlapPolicy
		want           enumspb.ScheduleOverlapPolicy
	}{
		{
			name:           "explicit start policy",
			startPolicy:    enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			schedulePolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			want:           enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		},
		{
			name:           "inherited schedule policy",
			schedulePolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			want:           enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			name: "default policy",
			want: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converted := convertBufferedStartsLegacyToCHASM(
				[]*schedulespb.BufferedStart{{
					NominalTime:   when,
					ActualTime:    when,
					OverlapPolicy: tt.startPolicy,
				}},
				"namespace-id",
				"schedule-id",
				1,
				"workflow-id",
				tt.schedulePolicy,
			)

			require.Len(t, converted, 1)
			require.Equal(t, tt.want, converted[0].GetOverlapPolicy())
		})
	}
}
