package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/testing/testcontext"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetStartTimeFromState(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		state *persistencespb.WorkflowExecutionState
		want  *time.Time
	}{
		{name: "nil state"},
		{name: "missing start time", state: &persistencespb.WorkflowExecutionState{}},
		{
			name:  "exact microsecond",
			state: &persistencespb.WorkflowExecutionState{StartTime: timestamppb.New(time.Unix(123, 456789000))},
			want:  new(time.Unix(123, 456789000).UTC()),
		},
		{
			name:  "below rounding boundary",
			state: &persistencespb.WorkflowExecutionState{StartTime: timestamppb.New(time.Unix(123, 456789123))},
			want:  new(time.Unix(123, 456789000).UTC()),
		},
		{
			name:  "above rounding boundary",
			state: &persistencespb.WorkflowExecutionState{StartTime: timestamppb.New(time.Unix(123, 456789999))},
			want:  new(time.Unix(123, 456789000).UTC()),
		},
		{
			name:  "end of second",
			state: &persistencespb.WorkflowExecutionState{StartTime: timestamppb.New(time.Unix(123, 999999999))},
			want:  new(time.Unix(123, 999999000).UTC()),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nanos := tc.state.GetStartTime().GetNanos()
			require.Equal(t, tc.want, getStartTimeFromState(tc.state))
			require.Equal(t, nanos, tc.state.GetStartTime().GetNanos())
		})
	}
}

func TestCurrentExecutionStartTimeRoundTrip(t *testing.T) {
	t.Parallel()

	state := &persistencespb.WorkflowExecutionState{
		StartTime: timestamppb.New(time.Unix(123, 456789999)),
	}
	row := sqlplugin.CurrentExecutionsRow{StartTime: getStartTimeFromState(state)}
	stored := row
	// PostgreSQL rounds timestamps sent by pq to the nearest microsecond.
	stored.StartTime = new(row.StartTime.Round(time.Microsecond))
	tx := &currentExecutionsTx{current: &stored}

	require.NoError(t, assertRunIDAndUpdateCurrentExecution(
		testcontext.For(t), tx, row, row.RunID, serialization.NewSerializer(),
	))
	require.Zero(t, tx.updates)
}
