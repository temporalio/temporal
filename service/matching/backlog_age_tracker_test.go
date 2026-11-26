package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBacklogAgeTracker(t *testing.T) {
	t.Parallel()

	b := newBacklogAgeTracker()
	oldest := func() time.Time { return b.oldestTime().UTC() }
	require.True(t, b.oldestTime().IsZero())

	now := time.Now()
	young := timestamppb.New(now.Add(-time.Second))
	old := timestamppb.New(now.Add(-time.Minute))

	// two tasks with the exact same create time are counted separately
	b.record(young, 1)
	b.record(young, 1)
	require.Equal(t, young.AsTime(), oldest())

	b.record(old, 1)
	require.Equal(t, old.AsTime(), oldest())

	b.record(old, -1)
	require.Equal(t, young.AsTime(), oldest())

	b.record(young, -1)
	require.Equal(t, young.AsTime(), oldest(), "one task with this time remains")

	b.record(young, -1)
	require.True(t, b.oldestTime().IsZero())

	// removing more than was added doesn't leave a negative count behind
	b.record(young, -1)
	b.record(young, 1)
	require.Equal(t, young.AsTime(), oldest())

	// nil create times are ignored
	b.record(nil, 1)
	require.Equal(t, young.AsTime(), oldest())
}
