package tests

import (
	"context"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type scalerShrink int

const (
	// let write count drop all the way to target in one step, so tests don't have to wait
	// for it to catch up with the read count
	scalerShrinkFast scalerShrink = iota
	// use normal shrink settings, which keep the write count close to the read count.
	// needed for some tests.
	scalerShrinkSlow
)

func scalerEnvOptions(dcPartitions int, shrink scalerShrink) []testcore.TestOption {
	settings := dynamicconfig.PartitionScaleManagerSettings{
		Enabled:            true,
		MaxRate:            100,         // don't limit speed of changes
		BatchSize:          1,           // always go directly to scaler
		BackgroundInterval: time.Second, // ping scaler often and drain faster
		DrainBufferTime:    time.Second, // drain faster
		ShrinkRatio:        1.0,         // allow fast shrinking
		ShrinkDelta:        100,         // allow fast shrinking
	}
	if shrink == scalerShrinkSlow {
		settings.ShrinkRatio = 0.1
		settings.ShrinkDelta = 2
	}
	return []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, dcPartitions),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, dcPartitions),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPartitionScaleManager, settings),
	}
}

type PartitionScalingSuite struct {
	parallelsuite.Suite[*PartitionScalingSuite]
}

func TestPartitionScalingSuite(t *testing.T) {
	parallelsuite.Run(t, &PartitionScalingSuite{})
}

func (s *PartitionScalingSuite) TestPartitionScaling_Up() {
	t := s.T()
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	env := testcore.NewEnv(t, scalerEnvOptions(1, scalerShrinkFast)...)

	t.Log("set to 2 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   2,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partitions 0,1 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 0, 1), 15*time.Second, time.Second)

	t.Log("check that 2,3 have no tasks (leave 4,5 unloaded)")
	s.True(scalerBacklogEmpty(env, 2, 3)())

	t.Log("set to 6 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("wait until partitions 2,3,4,5 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 2, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)
}

func (s *PartitionScalingSuite) TestPartitionScaling_Down() {
	t := s.T()
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	env := testcore.NewEnv(t, scalerEnvOptions(1, scalerShrinkFast)...)

	t.Log("set to 6 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partitions 0-5 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("set to 4 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   4,
	})

	t.Log("wait until 4,5 see no new tasks over a 2s window")
	s.Await(scalerBacklogUnchanged(env, 2*time.Second, 4, 5), 15*time.Second, time.Millisecond)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := env.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	// We want to check that polls went to all 6 partitions directly, even though we decreased
	// the target to 4. Note that tasks will be forwarded, so we'll still drain everything even
	// if we don't poll all 6. So we have to look at metrics.
	pollsByPartition := scalerCountPolls(env.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 6)

	// Note that this test does not test the read count eventually drops!
	// (i.e. polls will continue to go to 4,5 after they are drained)
	// That's tested in TestPartitionScaling_Down_AndStopPolling.
}

func (s *PartitionScalingSuite) TestPartitionScaling_Up_FromDC() {
	t := s.T()
	// default dynamic config to 3
	env := testcore.NewEnv(t, scalerEnvOptions(3, scalerShrinkFast)...)

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partitions 0,1,2 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 0, 1, 2), 15*time.Second, time.Second)

	t.Log("set to 6 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("wait until partitions 3,4,5 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)
}

func (s *PartitionScalingSuite) TestPartitionScaling_Down_FromDC() {
	t := s.T()
	// default dynamic config to 6
	env := testcore.NewEnv(t, scalerEnvOptions(6, scalerShrinkFast)...)

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partitions 0-5 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	t.Log("set to 4 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   4,
	})

	t.Log("wait until 4,5 see no new tasks over a 2s window")
	s.Await(scalerBacklogUnchanged(env, 2*time.Second, 4, 5), 15*time.Second, time.Millisecond)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := env.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5), 15*time.Second, time.Second)

	// We want to check that polls went to all 6 partitions directly, even though we decreased
	// the target to 4. Note that tasks will be forwarded, so we'll still drain everything even
	// if we don't poll all 6. So we have to look at metrics.
	pollsByPartition := scalerCountPolls(env.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 6)
}

func (s *PartitionScalingSuite) TestPartitionScaling_Down_AndStopPolling() {
	t := s.T()
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	env := testcore.NewEnv(t, scalerEnvOptions(1, scalerShrinkFast)...)

	t.Log("set to 6 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	capture := env.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	var polls map[int]int
	t.Log("wait for 10 successful polls on each partition")
	s.Await(func(s *PartitionScalingSuite) {
		polls = scalerCountPolls(env.Tv(), capture)
		t.Log("polls(6)", polls)
		s.Len(polls, 6)
		for _, v := range polls {
			s.GreaterOrEqual(v, 10)
		}
	}, 15*time.Second, time.Second)

	t.Log("set to 3 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   3,
	})

	// initially, polls should continue going to all 6, but new tasks should go to only 3
	// eventually, the three will report that they are drained, and then new polls should only go to 3.
	s.Await(func(s *PartitionScalingSuite) {
		polls2 := scalerCountPolls(env.Tv(), capture)
		diff := scalerSubtractPollCounts(polls2, polls)
		polls = polls2
		t.Log("polls(3)", diff)
		s.Len(diff, 3)
	}, 15*time.Second, time.Second)
}

// TestPartitionScaling_Backlog exercises backlog-count-based scaling (as opposed
// to add-rate-based scaling). Unlike the add-rate tests, this is not timing
// sensitive: partitions open once their accumulated backlog crosses BacklogBase,
// regardless of how fast tasks arrive. We build a backlog by sending tasks
// without polling, watch the scaler open partitions up to Max, then drain and
// confirm everything empties out.
func (s *PartitionScalingSuite) TestPartitionScaling_Backlog() {
	t := s.T()
	// default dynamic config to 1 to ensure we turn on managed scaling immediately
	env := testcore.NewEnv(t, scalerEnvOptions(1, scalerShrinkFast)...)

	t.Log("enable backlog-based scaling (no fixed target, no add-rate windows)")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled:      true,
		BacklogReset: 2,  // clear once backlog drops below 2
		BacklogBase:  5,  // open a partition once backlog grows above 5
		BacklogCap:   20, // soft cap used to spread new tasks
		Max:          4,  // bound growth so the test stays fast and deterministic
	})

	t.Log("start sending 10 tasks/s without polling to build a backlog")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until backlog-based scaling has opened all 4 partitions (each has backlog)")
	s.AwaitTrue(scalerBacklogAtLeast(env, 1, 0, 1, 2, 3), 30*time.Second, time.Second)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls to drain")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all partitions drain (backlog releases, scaler shrinks target)")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3), 30*time.Second, time.Second)
}

// TestPartitionScaling_Disable_ToFewerPartitions checks that disabling the scaler falls back
// to the dynamic config settings, even when that's fewer partitions than the scaler had set.
func (s *PartitionScalingSuite) TestPartitionScaling_Disable_ToFewerPartitions() {
	t := s.T()
	// default dynamic config to 1 to ensure we turn on managed scaling immediately.
	// use slow shrinking so that a regression that leaves backlog state set on disable keeps
	// write above 0, which this test detects. with fast shrinking, write would reach 0 either
	// way and clients would fall back to dynamic config even with stale scale info.
	env := testcore.NewEnv(t, scalerEnvOptions(1, scalerShrinkSlow)...)

	t.Log("set to 4 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   4,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partitions 0-3 have 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 0, 1, 2, 3), 15*time.Second, time.Second)

	t.Log("disable scaler: tasks and polls should go to partition 0 only now")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: false,
	})

	t.Log("wait until 1,2,3 see no new tasks over a 2s window")
	s.Await(scalerBacklogUnchanged(env, 2*time.Second, 1, 2, 3), 15*time.Second, time.Millisecond)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := env.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained (1,2,3 by forwarding to 0)")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3), 15*time.Second, time.Second)

	t.Log("check that polls went to partition 0 only")
	pollsByPartition := scalerCountPolls(env.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 1)
	s.Contains(pollsByPartition, 0)
}

// TestPartitionScaling_Disable_ToMorePartitions is the inverse of FewerPartitions: the scaler
// sets the count below dynamic config count, and disabling it allows tasks/polls to all.
func (s *PartitionScalingSuite) TestPartitionScaling_Disable_ToMorePartitions() {
	t := s.T()
	// default dynamic config to 4, which is what we should fall back to when disabling
	env := testcore.NewEnv(t, scalerEnvOptions(4, scalerShrinkFast)...)

	t.Log("set to 1 partition using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   1,
	})

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partition 0 has 5 tasks backlog")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 0), 15*time.Second, time.Second)

	t.Log("disable scaler: tasks and polls should go to all 4 partitions now")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: false,
	})

	t.Log("wait until partitions 1,2,3 have 5 tasks backlog, i.e. tasks go to all 4 again")
	s.AwaitTrue(scalerBacklogAtLeast(env, 5, 1, 2, 3), 15*time.Second, time.Second)

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := env.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.AwaitTrue(scalerBacklogEmpty(env, 0, 1, 2, 3), 15*time.Second, time.Second)

	// As in TestPartitionScaling_Down, tasks get forwarded, so draining alone doesn't prove that
	// polls reached 1,2,3 directly. Check metrics for that.
	pollsByPartition := scalerCountPolls(env.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 4)
}

func scalerBackgroundTasks(env *testcore.TestEnv, rate float32) func() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		t := time.NewTicker(time.Duration(float32(time.Second) / rate))
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
					Namespace:    env.Namespace().String(),
					WorkflowId:   uuid.NewString(),
					WorkflowType: env.Tv().WorkflowType(),
					TaskQueue:    env.Tv().TaskQueue(),
					Identity:     env.Tv().ClientIdentity(),
					RequestId:    uuid.NewString(),
				})
			}
		}
	}()

	return cancel
}

func scalerBackgroundPolls(env *testcore.TestEnv, workers int) func() {
	ctx, cancel := context.WithCancel(context.Background())

	tp, tv := env.TaskPoller(), env.Tv()
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

func scalerGetBacklog(env *testcore.TestEnv, part int) (int, error) {
	ctx := testcore.NewContext()
	res, err := env.AdminClient().DescribeTaskQueuePartition(ctx, &adminservice.DescribeTaskQueuePartitionRequest{
		Namespace: env.Namespace().String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     env.Tv().TaskQueue().Name,
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

func scalerBacklogAtLeast(env *testcore.TestEnv, target int, parts ...int) func() bool {
	return func() bool {
		for _, part := range parts {
			count, err := scalerGetBacklog(env, part)
			if err != nil || count < target {
				return false
			}
		}
		return true
	}
}

func scalerBacklogEmpty(env *testcore.TestEnv, parts ...int) func() bool {
	return func() bool {
		for _, part := range parts {
			count, err := scalerGetBacklog(env, part)
			if err != nil || count > 0 {
				return false
			}
		}
		return true
	}
}

func scalerBacklogUnchanged(env *testcore.TestEnv, sleep time.Duration, parts ...int) func(s *PartitionScalingSuite) {
	return func(s *PartitionScalingSuite) {
		before := make([]int, len(parts))
		for i, part := range parts {
			var err error
			before[i], err = scalerGetBacklog(env, part)
			s.NoError(err)
		}

		time.Sleep(sleep) //nolint:forbidigo // trying to test a negative

		for i, part := range parts {
			after, err := scalerGetBacklog(env, part)
			s.NoError(err)
			s.Equal(before[i], after)
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
