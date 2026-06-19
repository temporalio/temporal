package tests

import (
	"context"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

func scalerEnvOptions(dcPartitions int) []testcore.TestOption {
	return []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, dcPartitions),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, dcPartitions),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPartitionScaleManager, dynamicconfig.PartitionScaleManagerSettings{
			MaxRate:            100,         // don't limit speed of changes
			BatchSize:          1,           // always go directly to scaler
			BackgroundInterval: time.Second, // ping scaler often and drain faster
			DrainBufferTime:    time.Second, // drain faster
		}),
	}
}

func TestPartitionScaling_Up(t *testing.T) {
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	s := testcore.NewEnv(t, scalerEnvOptions(1)...)

	t.Log("set to 2 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   2,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(s, s.Tv(), 10)
	defer stopTasks()

	t.Log("wait until partitions 0,1 have 5 tasks backlog")
	await.RequireTrue(t, scalerBacklogAtLeast(s, s.Tv(), 5, 0, 1), 15*time.Second, time.Second)

	t.Log("check that 2,3 have no tasks (leave 4,5 unloaded)")
	s.True(scalerBacklogEmpty(s, s.Tv(), 5, 2, 3)())

	t.Log("set to 6 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("wait until partitions 2,3,4,5 have 5 tasks backlog")
	await.RequireTrue(t, scalerBacklogAtLeast(s, s.Tv(), 5, 2, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(s, s.Tv(), s.TaskPoller(), 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	await.RequireTrue(t, scalerBacklogEmpty(s, s.Tv(), 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)
}

func TestPartitionScaling_Down(t *testing.T) {
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	s := testcore.NewEnv(t, scalerEnvOptions(1)...)

	t.Log("set to 6 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(s, s.Tv(), 10)
	defer stopTasks()

	t.Log("wait until partitions 0-5 have 5 tasks backlog")
	await.RequireTrue(t, scalerBacklogAtLeast(s, s.Tv(), 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("set to 4 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   4,
	})

	t.Log("wait until 4,5 see no new tasks over a 1s window")
	await.Require(s.Context(), t, scalerBacklogUnchanged(s, s.Tv(), time.Second, 4, 5), 15*time.Second, time.Millisecond)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := s.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(s, s.Tv(), s.TaskPoller(), 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	await.RequireTrue(t, scalerBacklogEmpty(s, s.Tv(), 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	// We want to check that polls went to all 6 partitions directly, even though we decreased
	// the target to 4. Note that tasks will be forwarded, so we'll still drain everything even
	// if we don't poll all 6. So we have to look at metrics.
	pollsByPartition := scalerCountPolls(s.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 6)

	// Note that this test does not test the read count eventually drops!
	// (i.e. polls will continue to go to 4,5 after they are drained)
	// That's tested in TestPartitionScaling_Down_AndStopPolling.
}

func TestPartitionScaling_Up_FromDC(t *testing.T) {
	// default dynamic config to 3
	s := testcore.NewEnv(t, scalerEnvOptions(3)...)

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(s, s.Tv(), 10)
	defer stopTasks()

	t.Log("wait until partitions 0,1,2 have 5 tasks backlog")
	await.RequireTrue(t, scalerBacklogAtLeast(s, s.Tv(), 5, 0, 1, 2), 15*time.Second, time.Second)

	t.Log("set to 6 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("wait until partitions 3,4,5 have 5 tasks backlog")
	await.RequireTrue(t, scalerBacklogAtLeast(s, s.Tv(), 5, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(s, s.Tv(), s.TaskPoller(), 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	await.RequireTrue(t, scalerBacklogEmpty(s, s.Tv(), 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)
}

func TestPartitionScaling_Down_FromDC(t *testing.T) {
	// default dynamic config to 6
	s := testcore.NewEnv(t, scalerEnvOptions(6)...)

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(s, s.Tv(), 10)
	defer stopTasks()

	t.Log("wait until partitions 0-5 have 5 tasks backlog")
	await.RequireTrue(t, scalerBacklogAtLeast(s, s.Tv(), 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("set to 4 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   4,
	})

	t.Log("wait until 4,5 see no new tasks over a 1s window")
	await.Require(s.Context(), t, scalerBacklogUnchanged(s, s.Tv(), time.Second, 4, 5), 15*time.Second, time.Millisecond)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := s.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(s, s.Tv(), s.TaskPoller(), 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	await.RequireTrue(t, scalerBacklogEmpty(s, s.Tv(), 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	// We want to check that polls went to all 6 partitions directly, even though we decreased
	// the target to 4. Note that tasks will be forwarded, so we'll still drain everything even
	// if we don't poll all 6. So we have to look at metrics.
	pollsByPartition := scalerCountPolls(s.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 6)
}

func TestPartitionScaling_Down_AndStopPolling(t *testing.T) {
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	s := testcore.NewEnv(t, scalerEnvOptions(1)...)

	t.Log("set to 6 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(s, s.Tv(), 10)
	defer stopTasks()

	capture := s.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(s, s.Tv(), s.TaskPoller(), 3)
	defer stopPolls()

	var polls map[int]int
	t.Log("wait for 10 successful polls on each partition")
	await.Require(s.Context(), t, func(c *await.T) {
		polls = scalerCountPolls(s.Tv(), capture)
		t.Log("polls(6)", polls)
		require.Len(c, polls, 6)
		for _, v := range polls {
			require.GreaterOrEqual(c, v, 10)
		}
	}, 15*time.Second, time.Second)

	t.Log("set to 3 partitions using scaler")
	s.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   3,
	})

	// initially, polls should continue going to all 6, but new tasks should go to only 3
	// eventually, the three will report that they are drained, and then new polls should only go to 3.
	await.Require(s.Context(), t, func(c *await.T) {
		polls2 := scalerCountPolls(s.Tv(), capture)
		diff := scalerSubtractPollCounts(polls2, polls)
		polls = polls2
		t.Log("polls(3)", diff)
		require.Len(c, diff, 3)
	}, 15*time.Second, time.Second)
}

// TODO: test disabling scaler

func scalerBackgroundTasks(s testcore.Env, tv *testvars.TestVars, rate float32) func() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		t := time.NewTicker(time.Duration(float32(time.Second) / rate))
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
					Namespace:    s.Namespace().String(),
					WorkflowId:   uuid.NewString(),
					WorkflowType: tv.WorkflowType(),
					TaskQueue:    tv.TaskQueue(),
					Identity:     tv.ClientIdentity(),
					RequestId:    uuid.NewString(),
				})
			}
		}
	}()

	return cancel
}

func scalerBackgroundPolls(s testcore.Env, tv *testvars.TestVars, tp *taskpoller.TaskPoller, workers int) func() {
	ctx, cancel := context.WithCancel(context.Background())

	for range workers {
		go func() {
			for ctx.Err() == nil {
				_, _ = tp.PollAndHandleWorkflowTask(
					tv,
					taskpoller.CompleteWorkflowHandler,
					taskpoller.WithContext(ctx),
				)
			}
		}()
	}

	return cancel
}

func scalerGetBacklog(s testcore.Env, tv *testvars.TestVars, part int) (int, error) {
	ctx := testcore.NewContext()
	res, err := s.AdminClient().DescribeTaskQueuePartition(ctx, &adminservice.DescribeTaskQueuePartitionRequest{
		Namespace: s.Namespace().String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(part)},
		},
		BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
	})
	if err != nil {
		return 0, err
	}
	var count int
	for _, versionInfoInternal := range res.VersionsInfoInternal {
		for _, st := range versionInfoInternal.PhysicalTaskQueueInfo.InternalTaskQueueStatus {
			count += int(st.ApproximateBacklogCount)
		}
	}
	return count, nil
}

func scalerBacklogAtLeast(s testcore.Env, tv *testvars.TestVars, target int, parts ...int) func() bool {
	return func() bool {
		for _, part := range parts {
			count, err := scalerGetBacklog(s, tv, part)
			if err != nil || count < target {
				return false
			}
		}
		return true
	}
}

func scalerBacklogEmpty(s testcore.Env, tv *testvars.TestVars, parts ...int) func() bool {
	return func() bool {
		for _, part := range parts {
			count, err := scalerGetBacklog(s, tv, part)
			if err != nil || count > 0 {
				return false
			}
		}
		return true
	}
}

func scalerBacklogUnchanged(s testcore.Env, tv *testvars.TestVars, sleep time.Duration, parts ...int) func(c *await.T) {
	return func(c *await.T) {
		before := make([]int, len(parts))
		for i, part := range parts {
			var err error
			before[i], err = scalerGetBacklog(s, tv, part)
			require.NoError(c, err)
		}

		time.Sleep(sleep) //nolint:forbidigo // trying to test a negative

		for i, part := range parts {
			after, err := scalerGetBacklog(s, tv, part)
			require.NoError(c, err)
			require.Equal(c, before[i], after)
		}
	}
}

func scalerCountPolls(tv *testvars.TestVars, capture *testcore.NamespaceMetricCapture) map[int]int {
	out := make(map[int]int)
	// Note that poll latency records the partition that the poll originally came to, not the
	// one it matched on if it was forwarded.
	for _, pt := range capture.Metric(metrics.PollLatencyPerTaskQueue.Name()) {
		tags := pt.Tags
		if tags["taskqueue"] != tv.TaskQueue().Name ||
			tags["task_type"] != "Workflow" {
			continue
		}
		part, err := strconv.Atoi(tags["partition"])
		if err != nil {
			continue
		}
		out[part]++
	}
	return out
}

// Returns "a - b" per-key, removing zeros.
func scalerSubtractPollCounts(a, b map[int]int) map[int]int {
	a = maps.Clone(a)
	for k, v := range b {
		a[k] = a[k] - v
		if a[k] == 0 {
			delete(a, k)
		}
	}
	return a
}
