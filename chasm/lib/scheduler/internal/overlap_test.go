package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestTracksCompletionResult(t *testing.T) {
	require.False(t, TracksCompletionResult(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	require.True(t, TracksCompletionResult(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
}

func TestResolveOverlapPolicy(t *testing.T) {
	tests := []struct {
		name           string
		overlapPolicy  enumspb.ScheduleOverlapPolicy
		schedulePolicy enumspb.ScheduleOverlapPolicy
		want           enumspb.ScheduleOverlapPolicy
	}{
		{
			name:           "action override",
			overlapPolicy:  enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			schedulePolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			want:           enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			name:           "schedule policy",
			schedulePolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			want:           enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		},
		{
			name: "default",
			want: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ResolveOverlapPolicy(tt.overlapPolicy, tt.schedulePolicy))
		})
	}
}
