package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestRequestID(t *testing.T) {
	id := requestID("seed")

	require.Equal(t, id, requestID("seed"))
	require.NotEqual(t, id, requestID("other-seed"))
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id)
}

func TestPercentile(t *testing.T) {
	values := []float64{40, 10, 30, 20}

	require.InDelta(t, 0, percentile(nil, 0.50), 0)
	require.InDelta(t, 10, percentile(append([]float64(nil), values...), 0), 0)
	require.InDelta(t, 20, percentile(append([]float64(nil), values...), 0.50), 0)
	require.InDelta(t, 30, percentile(append([]float64(nil), values...), 0.75), 0)
	require.InDelta(t, 40, percentile(append([]float64(nil), values...), 1), 0)
}

func TestPhaseTimingsFormat(t *testing.T) {
	var timings phaseTimings
	timings.add(e2eTimings{
		start:           10 * time.Millisecond,
		poll:            20 * time.Millisecond,
		scheduleToStart: 30 * time.Millisecond,
		complete:        40 * time.Millisecond,
	})

	formatted := timings.format()
	require.Contains(t, formatted, "start_p50_ms=10.00")
	require.Contains(t, formatted, "poll_p50_ms=20.00")
	require.Contains(t, formatted, "wft_s2s_p50_ms=30.00")
	require.Contains(t, formatted, "complete_p50_ms=40.00")
}

func TestChoosePollerGroupID(t *testing.T) {
	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		PollerGroupsInfo: &taskqueuepb.PollerGroupsInfo{
			PollerGroups: []*taskqueuepb.PollerGroupInfo{
				{Id: "group-a"},
				{Id: "group-b"},
			},
		},
	}

	require.Equal(t, "group-b", choosePollerGroupID(resp, "current", 1))
}

func TestChoosePollerGroupIDLegacyFallback(t *testing.T) {
	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		PollerGroupInfos: []*taskqueuepb.PollerGroupInfo{
			{Id: "legacy-a"},
			{Id: "legacy-b"},
		},
	}

	require.Equal(t, "legacy-b", choosePollerGroupID(resp, "current", 3))
}

func TestChoosePollerGroupIDKeepsCurrentWhenMissing(t *testing.T) {
	require.Equal(t, "current", choosePollerGroupID(&workflowservice.PollWorkflowTaskQueueResponse{}, "current", 0))

	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		PollerGroupsInfo: &taskqueuepb.PollerGroupsInfo{
			PollerGroups: []*taskqueuepb.PollerGroupInfo{{}},
		},
	}
	require.Equal(t, "current", choosePollerGroupID(resp, "current", 0))
}

func TestReleaseOutstanding(t *testing.T) {
	outstanding := make(chan struct{}, 1)
	outstanding <- struct{}{}

	releaseOutstanding(outstanding)
	require.Empty(t, outstanding)

	releaseOutstanding(outstanding)
	require.Empty(t, outstanding)
}
