package tests

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
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
	"go.temporal.io/server/common/number"
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
		// waits are progress-based (see awaitProgress), so allow for slow progress on a loaded runner
		testcore.WithTestTimeout(3 * time.Minute),
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
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 0, 1))

	t.Log("check that 2,3 have no tasks (leave 4,5 unloaded)")
	done, _, status := scalerBacklogEmpty(env, 2, 3)()
	s.True(done, status)

	t.Log("set to 6 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("wait until partitions 2,3,4,5 have 5 tasks backlog")
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 2, 3, 4, 5))

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.awaitProgress(15*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5))
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
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 0, 1, 2, 3, 4, 5))

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
	s.awaitProgress(15*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5))

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
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 0, 1, 2))

	t.Log("set to 6 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   6,
	})

	t.Log("wait until partitions 3,4,5 have 5 tasks backlog")
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 3, 4, 5))

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.awaitProgress(15*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5))
}

func (s *PartitionScalingSuite) TestPartitionScaling_Down_FromDC() {
	t := s.T()
	// default dynamic config to 6
	env := testcore.NewEnv(t, scalerEnvOptions(6, scalerShrinkFast)...)

	t.Log("start sending 10 tasks/s")
	stopTasks := scalerBackgroundTasks(env, 10)
	defer stopTasks()

	t.Log("wait until partitions 0-5 have 5 tasks backlog")
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 0, 1, 2, 3, 4, 5))

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
	s.awaitProgress(15*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3, 4, 5))

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

	t.Log("wait for 5 polls on each partition")
	s.awaitProgress(15*time.Second, scalerPollsAtLeast(env.Tv(), capture, 5, 6))

	t.Log("set to 3 partitions using scaler")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Fixed:   3,
	})

	// initially, polls should continue going to all 6, but new tasks should go to only 3
	// eventually, the three will report that they are drained, and then new polls should only go to 3.
	// Judge that by a run of polls rather than a time window, so it doesn't depend on poll rate.
	t.Log("wait until the last 20 polls went only to 0,1,2")
	s.awaitProgress(15*time.Second, scalerRecentPollsOnlyTo(env.Tv(), capture, 20, 3))
}

// TestPartitionScaling_Backlog exercises backlog-count-based scaling (as opposed
// to add-rate-based scaling). Partitions open once their accumulated backlog crosses
// BacklogBase, regardless of how fast tasks arrive. We build a backlog by sending tasks
// without polling, watch the scaler open partitions up to Max, then drain and
// confirm everything empties out.
//
// Note that the scaler sees backlog counts through Compact8, which rounds anything below
// scalerMinVisibleBacklog down to zero. So the small thresholds below effectively all act as
// scalerMinVisibleBacklog, and each partition needs that many tasks before the next one opens.
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
	s.awaitProgress(30*time.Second, scalerBacklogAtLeast(env, 1, 0, 1, 2, 3))

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("start background polls to drain")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all partitions drain (backlog releases, scaler shrinks target)")
	s.awaitProgress(30*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3))
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
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 0, 1, 2, 3))

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
	s.awaitProgress(15*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3))

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
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 0))

	t.Log("disable scaler: tasks and polls should go to all 4 partitions now")
	env.OverrideDynamicConfig(dynamicconfig.MatchingPartitionScaler, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: false,
	})

	t.Log("wait until partitions 1,2,3 have 5 tasks backlog, i.e. tasks go to all 4 again")
	s.awaitProgress(15*time.Second, scalerBacklogAtLeast(env, 5, 1, 2, 3))

	t.Log("stop sending tasks")
	stopTasks()

	t.Log("capture poll metrics")
	capture := env.StartNamespaceMetricCapture()

	t.Log("start background polls")
	stopPolls := scalerBackgroundPolls(env, 3)
	defer stopPolls()

	t.Log("wait until all are drained")
	s.awaitProgress(15*time.Second, scalerBacklogEmpty(env, 0, 1, 2, 3))

	// As in TestPartitionScaling_Down, tasks get forwarded, so draining alone doesn't prove that
	// polls reached 1,2,3 directly. Check metrics for that.
	pollsByPartition := scalerCountPolls(env.Tv(), capture)
	t.Log("poll counts", pollsByPartition)
	s.Len(pollsByPartition, 4)
}

// scalerProgressCheck reports whether a wait condition is done, plus a progress value that
// increases as the condition gets closer to done, and a status string for logging.
type scalerProgressCheck func() (done bool, progress int, status string)

// awaitProgress waits until check is done. Instead of a fixed deadline, it fails only if
// progress stops improving for stallTimeout.
//
// These tests push a stream of tasks and polls through the server, and on a loaded CI runner
// (especially with sqlite, where parallel tests on a shared cluster use one database file)
// throughput can fall far below the nominal rate. Waiting on progress keeps the tests correct
// under that load, while still failing promptly when scaling actually gets stuck.
func (s *PartitionScalingSuite) awaitProgress(stallTimeout time.Duration, check scalerProgressCheck) {
	t := s.T()
	best := math.MinInt
	var lastStatus string
	for {
		var done bool
		s.Await(func(s *PartitionScalingSuite) {
			var progress int
			var status string
			done, progress, status = check()
			if status != lastStatus {
				t.Log(status)
				lastStatus = status
			}
			if done || progress > best {
				best = max(best, progress)
				return
			}
			s.Failf("no progress", "stalled at: %s", status)
		}, stallTimeout, time.Second)
		if done {
			return
		}
	}
}

func scalerBackgroundTasks(env *testcore.TestEnv, rate float32) func() {
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	var sent, failed atomic.Int64

	go func() {
		t := time.NewTicker(time.Duration(float32(time.Second) / rate))
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
					Namespace:    env.Namespace().String(),
					WorkflowId:   uuid.NewString(),
					WorkflowType: env.Tv().WorkflowType(),
					TaskQueue:    env.Tv().TaskQueue(),
					Identity:     env.Tv().ClientIdentity(),
					RequestId:    uuid.NewString(),
				})
				if err != nil {
					failed.Add(1)
				} else {
					sent.Add(1)
				}
			}
		}
	}()

	// The ticker drops ticks while a start call is slow, so log the rate actually achieved.
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			elapsed := time.Since(start)
			env.T().Logf("sent %d tasks in %v (%.1f/s, target %.0f/s), %d failed",
				sent.Load(), elapsed.Round(time.Millisecond), float64(sent.Load())/elapsed.Seconds(), rate, failed.Load())
		})
	}
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

func scalerGetBacklogs(env *testcore.TestEnv, parts ...int) (map[int]int, error) {
	out := make(map[int]int, len(parts))
	for _, part := range parts {
		count, err := scalerGetBacklog(env, part)
		if err != nil {
			return nil, err
		}
		out[part] = count
	}
	return out, nil
}

// scalerMinVisibleBacklog is the smallest non-zero backlog count the scale manager can see,
// since it syncs counts as Compact8.
var scalerMinVisibleBacklog = int(number.DecodeCompact8(1))

// scalerBacklogAtLeast checks that all parts have at least target backlog. Progress is the
// total backlog on parts, counting each up to scalerMinVisibleBacklog (or target, if larger):
// with backlog-based scaling, a partition must fill to that level before the next one opens.
func scalerBacklogAtLeast(env *testcore.TestEnv, target int, parts ...int) scalerProgressCheck {
	return func() (bool, int, string) {
		backlogs, err := scalerGetBacklogs(env, parts...)
		if err != nil {
			return false, math.MinInt, fmt.Sprint("backlog error: ", err)
		}
		done := true
		var progress int
		for _, count := range backlogs {
			done = done && count >= target
			progress += min(count, max(target, scalerMinVisibleBacklog))
		}
		return done, progress, fmt.Sprintf("backlog %v (want >= %d)", backlogs, target)
	}
}

// scalerBacklogEmpty checks that all parts have no backlog. Progress is how much is left in total.
func scalerBacklogEmpty(env *testcore.TestEnv, parts ...int) scalerProgressCheck {
	return func() (bool, int, string) {
		backlogs, err := scalerGetBacklogs(env, parts...)
		if err != nil {
			return false, math.MinInt, fmt.Sprint("backlog error: ", err)
		}
		var remaining int
		for _, count := range backlogs {
			remaining += count
		}
		return remaining == 0, -remaining, fmt.Sprintf("backlog %v (want empty)", backlogs)
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

// scalerPolledPartitions returns the partition of each poll, in the order they were recorded.
func scalerPolledPartitions(tv *testvars.TestVars, capture *testcore.NamespaceMetricCapture) []int {
	var out []int
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
		out = append(out, part)
	}
	return out
}

func scalerCountPolls(tv *testvars.TestVars, capture *testcore.NamespaceMetricCapture) map[int]int {
	out := make(map[int]int)
	for _, part := range scalerPolledPartitions(tv, capture) {
		out[part]++
	}
	return out
}

// scalerPollsAtLeast checks that each of partitions [0, numParts) got at least target polls.
// Progress is how close they are in total.
func scalerPollsAtLeast(tv *testvars.TestVars, capture *testcore.NamespaceMetricCapture, target, numParts int) scalerProgressCheck {
	return func() (bool, int, string) {
		polls := scalerCountPolls(tv, capture)
		var missing int
		for part := range numParts {
			missing += max(0, target-polls[part])
		}
		return missing == 0, -missing, fmt.Sprintf("polls %v (want >= %d on each of %d)", polls, target, numParts)
	}
}

// scalerRecentPollsOnlyTo checks that the last n polls all went to partitions [0, numParts),
// and that each of those partitions got at least one of them. Progress is the length of the
// current run of polls to [0, numParts), so a stray poll to a higher partition resets it.
func scalerRecentPollsOnlyTo(tv *testvars.TestVars, capture *testcore.NamespaceMetricCapture, n, numParts int) scalerProgressCheck {
	return func() (bool, int, string) {
		polled := scalerPolledPartitions(tv, capture)
		var run int
		for i := len(polled) - 1; i >= 0 && polled[i] < numParts && run < n; i-- {
			run++
		}
		recent := make(map[int]int)
		for _, part := range polled[len(polled)-min(n, len(polled)):] {
			recent[part]++
		}
		return run == n && len(recent) == numParts, run,
			fmt.Sprintf("last %d polls %v (want only and all of [0,%d))", n, recent, numParts)
	}
}
