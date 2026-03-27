package taskqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestMergeStats(t *testing.T) {
	into := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 10,
		ApproximateBacklogAge:   durationpb.New(100 * time.Second),
		TasksAddRate:            5,
		TasksDispatchRate:       3,
	}
	from := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 20,
		ApproximateBacklogAge:   durationpb.New(50 * time.Second),
		TasksAddRate:            2,
		TasksDispatchRate:       1,
	}

	MergeStats(into, from)

	require.Equal(t, int64(30), into.ApproximateBacklogCount)
	require.Equal(t, 100*time.Second, into.ApproximateBacklogAge.AsDuration())
	require.InDelta(t, 7, into.TasksAddRate, 1e-9)
	require.InDelta(t, 4, into.TasksDispatchRate, 1e-9)
}

func TestMergeStats_NilInto(t *testing.T) {
	from := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 10,
	}
	require.NotPanics(t, func() { MergeStats(nil, from) })
}

func TestMergeStats_BothNil(t *testing.T) {
	require.NotPanics(t, func() { MergeStats(nil, nil) })
}

func TestMergeStats_NilFrom(t *testing.T) {
	into := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 10,
		ApproximateBacklogAge:   durationpb.New(100 * time.Second),
		TasksAddRate:            5,
		TasksDispatchRate:       3,
	}

	MergeStats(into, nil)

	require.Equal(t, int64(10), into.ApproximateBacklogCount)
	require.Equal(t, 100*time.Second, into.ApproximateBacklogAge.AsDuration())
	require.InDelta(t, 5, into.TasksAddRate, 1e-9)
	require.InDelta(t, 3, into.TasksDispatchRate, 1e-9)
}

func TestMergeStats_NilBacklogAges(t *testing.T) {
	into := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 5,
	}
	from := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 3,
		ApproximateBacklogAge:   durationpb.New(10 * time.Second),
	}

	MergeStats(into, from)

	require.Equal(t, int64(8), into.ApproximateBacklogCount)
	require.Equal(t, 10*time.Second, into.ApproximateBacklogAge.AsDuration())
}

func TestMergeStats_RightOlderAge(t *testing.T) {
	into := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogAge: durationpb.New(10 * time.Second),
	}
	from := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogAge: durationpb.New(50 * time.Second),
	}

	MergeStats(into, from)

	require.Equal(t, 50*time.Second, into.ApproximateBacklogAge.AsDuration())
}

func TestDedupPollers(t *testing.T) {
	pollers := []*taskqueuepb.PollerInfo{
		{Identity: "worker-1"},
		{Identity: "worker-2"},
		{Identity: "worker-1"},
		{Identity: "worker-3"},
	}

	result := DedupPollers(pollers)

	require.Len(t, result, 3)
	idents := make(map[string]bool)
	for _, p := range result {
		idents[p.GetIdentity()] = true
	}
	require.True(t, idents["worker-1"])
	require.True(t, idents["worker-2"])
	require.True(t, idents["worker-3"])
}

func TestDedupPollers_Empty(t *testing.T) {
	result := DedupPollers(nil)
	require.Empty(t, result)
}

func TestDedupPollers_NoDuplicates(t *testing.T) {
	pollers := []*taskqueuepb.PollerInfo{
		{Identity: "worker-1"},
		{Identity: "worker-2"},
	}

	result := DedupPollers(pollers)
	require.Len(t, result, 2)
}
