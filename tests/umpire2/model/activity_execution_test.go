package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

func TestActivityExecutionSnapshotIsIdempotent(t *testing.T) {
	state := umpire.NewModelState()
	RegisterDefaultEntities(state)
	snapshot := fact.NewActivityExecutionSnapshot("namespace-id", "activity-id", enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, nil)

	require.NoError(t, state.RouteFacts(context.Background(), []umpire.Fact{snapshot}))
	require.NoError(t, state.RouteFacts(context.Background(), []umpire.Fact{snapshot}))

	entries := state.QueryEntities(ActivityType, 0, nil)
	require.Len(t, entries, 1)
	activity, ok := entries[0].Entity.(*Activity)
	require.True(t, ok)
	require.Equal(t, ActivityCompleted, activity.FSM.Current())
	require.Empty(t, activity.FSM.Illegal())
}
