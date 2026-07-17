package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type PrioritySuite struct {
	parallelsuite.Suite[*PrioritySuite]
}

func TestPrioritySuite(t *testing.T) {
	parallelsuite.Run(t, &PrioritySuite{})
}

func (s *PrioritySuite) newTestEnv(opts ...testcore.TestOption) (*testcore.TestEnv, *testvars.TestVars) {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingGetTasksBatchSize, 20),
		testcore.WithDynamicConfig(dynamicconfig.MatchingGetTasksReloadAt, 5),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *PrioritySuite) TestActivity_Basic() {
	const N = 20
	const Levels = 5

	env, tv := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	for wfidx := range N {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks
	for range N {
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Len(task.History.Events, 3)

				var wfidx int
				_, err := fmt.Sscanf(task.WorkflowExecution.WorkflowId, "wf%d", &wfidx)
				s.NoError(err)

				var commands []*commandpb.Command

				for i, pri := range rand.Perm(Levels) {
					pri += 1 // 1-based
					input, err := payloads.Encode(wfidx, pri)
					s.NoError(err)
					priMsg := &commonpb.Priority{PriorityKey: int32(pri)}
					if pri == (Levels+1)/2 {
						priMsg = nil // nil should be treated as default (3)
					}
					commands = append(commands, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             fmt.Sprintf("act%d", i),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								Priority:               priMsg,
								Input:                  input,
							},
						},
					})
				}

				return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}

	// wait for activity tasks to appear in the matching backlog (from transfer queue)
	s.Eventually(func() bool {
		res, err := env.AdminClient().DescribeTaskQueuePartition(s.Context(), &adminservice.DescribeTaskQueuePartitionRequest{
			Namespace: env.Namespace().String(),
			TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
				TaskQueue:     tv.TaskQueue().Name,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
				PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: 0},
			},
			BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
		})
		if err != nil {
			return false
		}
		var count int64
		for _, versionInfoInternal := range res.VersionsInfoInternal {
			for _, st := range versionInfoInternal.PhysicalTaskQueueInfo.InternalTaskQueueStatus {
				count += st.ApproximateBacklogCount
			}
		}
		return count == N*Levels
	}, 10*time.Second, 100*time.Millisecond)

	// process activity tasks
	var runs []int
	for range N * Levels {
		_, err := env.TaskPoller().PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				var wfidx, pri int
				s.NoError(payloads.Decode(task.Input, &wfidx, &pri))
				// s.T().Log("activity", "pri", pri, "wfidx", wfidx)
				runs = append(runs, pri)
				nothing, err := payloads.Encode()
				s.NoError(err)
				return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}

	w := wrongorderness(runs)
	s.T().Log("wrongorderness:", w)
	s.Less(w, 0.1)
}

func (s *PrioritySuite) TestSubqueue_Migration() {
	env, tv := s.newTestEnv()

	// start with old matcher
	env.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	// start 100 workflows
	for range 100 {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   uuid.NewString(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks and create 300 activities
	for range 100 {
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Len(task.History.Events, 3)

				var commands []*commandpb.Command

				for i := range 3 {
					input, err := payloads.Encode(i)
					s.NoError(err)
					commands = append(commands, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             fmt.Sprintf("act%d", i),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								Input:                  input,
							},
						},
					})
				}

				return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}

	processActivity := func() {
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := env.TaskPoller().PollAndHandleActivityTask(
				tv,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					nothing, err := payloads.Encode()
					s.NoError(err)
					return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
				},
				taskpoller.WithContext(s.Context()),
			)
			assert.NoError(c, err)
		}, 5*time.Second, time.Millisecond)
	}

	s.T().Log("process first 100 activities")
	for range 100 {
		processActivity()
	}

	s.T().Log("switching to new matcher")
	env.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true)

	s.T().Log("processing next 100 activities")
	for range 100 {
		processActivity()
	}

	s.T().Log("switching back to old matcher")
	env.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	s.T().Log("processing last 100 activities")
	for range 100 {
		processActivity()
	}
}

func (s *PrioritySuite) TestStickyInteraction_SinglePartition() {
	const N = 10

	// set these to make the mechanism react faster for the test:
	shortTime := 20 * time.Millisecond
	env, tv := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingBacklogNegligibleAge, shortTime),
		testcore.WithDynamicConfig(dynamicconfig.MatchingEphemeralDataUpdateInterval, shortTime),
		// one partition for now:
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	describeSticky := func() (*adminservice.DescribeTaskQueuePartitionResponse, error) {
		return env.AdminClient().DescribeTaskQueuePartition(s.Context(), &adminservice.DescribeTaskQueuePartitionRequest{
			Namespace: env.Namespace().String(),
			TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
				TaskQueue:     tv.TaskQueue().Name,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				PartitionId:   &taskqueuespb.TaskQueuePartition_StickyName{StickyName: tv.StickyTaskQueue().Name},
			},
			BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
		})
	}

	// poll sticky queue once, otherwise it won't be used
	s.T().Log("polling sticky to load")
	stickyPoller := env.TaskPoller().PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: &taskqueuepb.TaskQueue{
			Name:       tv.StickyTaskQueue().Name,
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: tv.TaskQueue().Name,
		},
	})
	stickyCtx, stickyCancel := context.WithCancel(s.Context())
	go stickyPoller.HandleTask(tv, taskpoller.DrainWorkflowTask, taskpoller.WithContext(stickyCtx)) // nolint:errcheck
	// wait for poll to reach matching service and load the queue
	s.EventuallyWithT(func(c *assert.CollectT) {
		res, err := describeSticky()
		require.NoError(c, err)
		require.NotEmpty(c, res.VersionsInfoInternal[""].GetPhysicalTaskQueueInfo().GetPollers())
	}, 5*time.Second, 10*time.Millisecond)
	// cancel as soon as it's registered
	s.T().Log("canceling sticky poll")
	stickyCancel()
	// wait for cancel to propagate
	time.Sleep(100 * time.Millisecond) // nolint:forbidigo // there's no way to wait for this

	// create some wfts at default priority
	s.T().Log("creating wfts on normal")
	for wfidx := range N {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process initial tasks on normal queue
	s.T().Log("processing wfts")
	for range N {
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{&commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_START_TIMER,
						Attributes: &commandpb.Command_StartTimerCommandAttributes{
							StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
								TimerId:            uuid.NewString(),
								StartToFireTimeout: durationpb.New(time.Millisecond),
							},
						},
					}},
					StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
						WorkerTaskQueue:        tv.StickyTaskQueue(),
						ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					},
				}, nil
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}

	// after 1 millisecond, we should have N tasks queued on the sticky queue at normal priority
	s.T().Log("checking sticky backlog")
	s.EventuallyWithT(func(c *assert.CollectT) {
		res, err := describeSticky()
		require.NoError(c, err)
		require.EqualValues(c, N, res.VersionsInfoInternal[""].PhysicalTaskQueueInfo.TaskQueueStats.ApproximateBacklogCount)
	}, 5*time.Second, 10*time.Millisecond)

	// create N more workflows at high priority and N at lower
	s.T().Log("creating high/low wfts on normal")
	for wfidx := range N {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   fmt.Sprintf("highpri%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Priority:     &commonpb.Priority{PriorityKey: 1},
		})
		s.NoError(err)

		_, err = env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   fmt.Sprintf("lowpri%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Priority:     &commonpb.Priority{PriorityKey: 5},
		})
		s.NoError(err)
	}

	// the ephemeral data mechanism is asynchronous, so wait a little while for it to kick in.
	// there's no way to check if this is done yet without actually polling, which would mess
	// up the results, so just sleep.
	time.Sleep(3 * shortTime) // nolint:forbidigo

	// now poll the sticky queue. we should get the high priority tasks from the normal queue
	// then the normal-priority from sticky, then the low priority from normal.
	s.T().Log("polling sticky")
	var receivedOrder []string
	for range 3 * N {
		_, _ = stickyPoller.HandleTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				wfid := task.WorkflowExecution.WorkflowId
				s.T().Log("task for wf", wfid)
				receivedOrder = append(receivedOrder, wfid)
				return nil, nil
			},
			taskpoller.WithContext(s.Context()),
		)
	}

	// validate the order
	var priorityOrder []int
	for _, wfid := range receivedOrder {
		if strings.HasPrefix(wfid, "highpri") {
			priorityOrder = append(priorityOrder, 1)
		} else if strings.HasPrefix(wfid, "lowpri") {
			priorityOrder = append(priorityOrder, 3)
		} else {
			priorityOrder = append(priorityOrder, 2) // default priority (wf*)
		}
	}

	w := wrongorderness(priorityOrder)
	s.T().Log("wrongorderness:", w)
	s.Less(w, 0.1, "tasks should mostly arrive in priority order (high, default, low)")
}

func wrongorderness(vs []int) float64 {
	l := len(vs)
	wrong := 0
	for i, v := range vs[:l-1] {
		for _, w := range vs[i+1:] {
			if v > w {
				wrong++
			}
		}
	}
	return float64(wrong) / float64(l*(l-1)/2)
}

type FairnessSuite struct {
	parallelsuite.Suite[*FairnessSuite]
}

func TestFairnessSuite(t *testing.T) {
	parallelsuite.Run(t, &FairnessSuite{}, false)
}

func TestFairnessAutoEnableSuite(t *testing.T) {
	parallelsuite.Run(t, &FairnessSuite{}, true)
}

// fairnessPartitions is the number of read/write partitions used by FairnessSuite.
func (s *FairnessSuite) fairnessPartitions() int {
	return 1
}

func (s *FairnessSuite) newTestEnv(doAutoEnable bool, opts ...testcore.TestOption) (*testcore.TestEnv, *testvars.TestVars) {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingGetTasksBatchSize, 20),
		testcore.WithDynamicConfig(dynamicconfig.MatchingGetTasksReloadAt, 5),
		testcore.WithDynamicConfig(dynamicconfig.NumPendingActivitiesLimitError, 1000),
		// TODO: disable this and use default later?
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, s.fairnessPartitions()),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, s.fairnessPartitions()),
	}
	if doAutoEnable {
		baseOpts = append(baseOpts,
			testcore.WithDynamicConfig(dynamicconfig.MatchingAutoEnableV2, true),
			testcore.WithDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false),
			testcore.WithDynamicConfig(dynamicconfig.MatchingEnableFairness, false),
		)
	} else {
		baseOpts = append(baseOpts,
			testcore.WithDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true),
			testcore.WithDynamicConfig(dynamicconfig.MatchingEnableFairness, true),
		)
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *FairnessSuite) triggerAutoEnable(env *testcore.TestEnv, tv *testvars.TestVars) {
	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    env.Namespace().String(),
		WorkflowId:   "trigger",
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Priority: &commonpb.Priority{
			PriorityKey: 3,
		},
	})
	s.NoError(err)

	s.Eventually(func() bool {
		resp, err := env.AdminClient().GetTaskQueueTasks(s.Context(), &adminservice.GetTaskQueueTasksRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			BatchSize:     10,
			MinPass:       1,
		})
		return err == nil && len(resp.GetTasks()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			var commands []*commandpb.Command
			commands = append(commands,
				&commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             "trigger",
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(time.Minute),
						},
					},
				},
			)
			return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
		},
		taskpoller.WithContext(s.Context()),
	)
	s.NoError(err)

	_, err = env.TaskPoller().PollAndHandleActivityTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
		},
		taskpoller.WithContext(s.Context()),
	)
	s.NoError(err)

	_, err = env.FrontendClient().DeleteWorkflowExecution(s.Context(), &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "trigger",
		},
	})
	s.NoError(err)
}

func (s *FairnessSuite) Test_Activity_Basic(doAutoEnable bool) {
	const Workflows = 15
	const Tasks = 15
	const Keys = 10

	env, tv := s.newTestEnv(doAutoEnable)
	if doAutoEnable {
		s.triggerAutoEnable(env, tv)
	}

	zipf := rand.NewZipf(rand.New(rand.NewSource(12345)), 2, 2, Keys-1)

	for wfidx := range Workflows {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks
	for range Workflows {
		_, err := env.TaskPoller().PollAndHandleWorkflowTask(
			tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.Len(task.History.Events, 3)

				var wfidx int
				_, err := fmt.Sscanf(task.WorkflowExecution.WorkflowId, "wf%d", &wfidx)
				s.NoError(err)

				var commands []*commandpb.Command

				for i := range Tasks {
					fkey := int(zipf.Uint64())
					input, err := payloads.Encode(wfidx, fkey)
					s.NoError(err)
					commands = append(commands, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             fmt.Sprintf("act%d", i),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								Priority: &commonpb.Priority{
									FairnessKey: fmt.Sprintf("key%d", fkey),
								},
								Input: input,
							},
						},
					})
				}

				return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}

	// process activity tasks
	var runs []int
	for range Workflows * Tasks {
		_, err := env.TaskPoller().PollAndHandleActivityTask(
			tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				var wfidx, fkey int
				s.NoError(payloads.Decode(task.Input, &wfidx, &fkey))
				s.T().Log("activity", "fkey", fkey, "wfidx", wfidx)
				runs = append(runs, fkey)
				nothing, err := payloads.Encode()
				s.NoError(err)
				return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
			},
			taskpoller.WithContext(s.Context()),
		)
		s.NoError(err)
	}

	u := unfairness(runs)
	s.T().Log("unfairness:", u)
	s.Less(u, 1.0)
}

func unfairness(vs []int) float64 {
	firsts := make(map[int]int)
	for i, v := range vs {
		if _, ok := firsts[v]; !ok {
			firsts[v] = i
		}
	}
	var totalDelay int
	for _, first := range firsts {
		totalDelay += first
	}
	return float64(totalDelay) / float64(len(firsts)*len(firsts))
}

func (s *FairnessSuite) testMigration(env *testcore.TestEnv, tv *testvars.TestVars, newMatcher, fairness bool) {

	// Speed up periodic sync so drain completion is detected faster
	env.OverrideDynamicConfig(dynamicconfig.MatchingUpdateAckInterval, 100*time.Millisecond)

	forTest := func(v any) any {
		return []dynamicconfig.ConstrainedValue{
			// test tqs (both wf and activity)
			dynamicconfig.ConstrainedValue{
				Constraints: dynamicconfig.Constraints{
					Namespace:     env.Namespace().String(),
					TaskQueueName: tv.TaskQueue().Name,
				},
				Value: v,
			},
			// default (match values in SetupSuite to avoid flapping)
			dynamicconfig.ConstrainedValue{Value: true},
		}
	}
	setConfig := func(stage string, newNewMatcher, newFairness bool) {
		newMatcher, fairness = newNewMatcher, newFairness
		s.T().Log("setting config: "+stage, "newMatcher", newMatcher, "fairness", fairness)
		env.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, forTest(newMatcher))
		env.OverrideDynamicConfig(dynamicconfig.MatchingEnableFairness, forTest(fairness))
	}
	waitForTasks := func(tp enumspb.TaskQueueType, onDraining, onActive int64) {
		s.T().Helper()
		s.EventuallyWithT(func(c *assert.CollectT) {
			tasksOnDraining, tasksOnActive, loadedOnDraining, loadedOnActive, _, err := s.countTasksByDrainingActive(env, tv, tp)
			require.NoError(c, err)
			require.Equal(c, onDraining, tasksOnDraining)
			require.Equal(c, onActive, tasksOnActive)
			// ensure that expected tasks are actually loaded to avoid poller getting regular
			// task before draining loads
			if tasksOnDraining > 0 {
				require.NotZero(c, loadedOnDraining)
			}
			if tasksOnActive > 0 {
				require.NotZero(c, loadedOnActive)
			}
		}, 15*time.Second, 250*time.Millisecond)
	}
	waitForNoDraining := func(tp enumspb.TaskQueueType) {
		s.T().Helper()
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, _, _, _, hasDraining, err := s.countTasksByDrainingActive(env, tv, tp)
			require.NoError(c, err)
			require.False(c, hasDraining, "draining queue should be unloaded after drain completes")
		}, 15*time.Second, 250*time.Millisecond)
	}

	setConfig("initial", newMatcher, fairness)

	// start 20 workflows. 20 tasks will be queued on wft queue.
	s.T().Log("starting workflows")
	for range 20 {
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   uuid.NewString(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0, 20)

	processWft := func() {
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := env.TaskPoller().PollAndHandleWorkflowTask(
				tv,
				func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					s.Len(task.History.Events, 3)

					var commands []*commandpb.Command

					for i := range 2 {
						input, err := payloads.Encode(i)
						s.NoError(err)
						commands = append(commands, &commandpb.Command{
							CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
							Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
								ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
									ActivityId:             fmt.Sprintf("act%d", i),
									ActivityType:           tv.ActivityType(),
									TaskQueue:              tv.TaskQueue(),
									ScheduleToCloseTimeout: durationpb.New(time.Minute),
									Input:                  input,
								},
							},
						})
					}

					return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
				},
				taskpoller.WithContext(s.Context()),
			)
			assert.NoError(c, err)
		}, 5*time.Second, time.Millisecond)
	}

	// process half the workflow tasks and create two activities each.
	// 10 tasks will be left on old workflow queue, 20 tasks will be queued on current activity queue.
	s.T().Log("processing first half of wfts")
	for range 10 {
		processWft()
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0, 10)
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0, 20)

	// switch fairness. queues will be reloaded. wft queue should drain old queue.
	setConfig("switching fairness", true, !fairness)

	waitForTasks(enumspb.TASK_QUEUE_TYPE_WORKFLOW, 10, 0)
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 20, 0)

	// process the other half of workflow tasks. these should come from the draining queue now.
	// 20 tasks will be queued on new activity queue (still 20 on old).
	s.T().Log("processing last half of wfts")
	for range 5 {
		processWft()
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_WORKFLOW, 5, 0)
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 20, 10)
	for range 5 {
		processWft()
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0, 0)
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 20, 20)
	// Verify wft draining queue is unloaded after drain completes
	waitForNoDraining(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.T().Log("wfts done")

	// process activities 1/3 at a time
	processActivity := func() {
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := env.TaskPoller().PollAndHandleActivityTask(
				tv,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					nothing, err := payloads.Encode()
					s.NoError(err)
					return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
				},
				taskpoller.WithContext(s.Context()),
			)
			assert.NoError(c, err)
		}, 5*time.Second, time.Millisecond)
	}

	s.T().Log("processing first 1/3 activities")
	for range 13 {
		processActivity()
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 7, 20)

	setConfig("switching fairness again", true, !fairness)
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 20, 7)

	s.T().Log("processing next 1/3 activities")
	for range 14 {
		processActivity()
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 6, 7)

	setConfig("switching fairness last time", true, !fairness)
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 7, 6)

	s.T().Log("processing last 1/3 activities")
	for range 13 {
		processActivity()
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0, 0)
	// Verify activity draining queue is unloaded after drain completes
	waitForNoDraining(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
}

func (s *FairnessSuite) countTasksByDrainingActive(env *testcore.TestEnv, tv *testvars.TestVars, tp enumspb.TaskQueueType) ( //nolint:revive // function-result-limit
	tasksOnDraining, tasksOnActive, loadedOnDraining, loadedOnActive int64, hasDraining bool, retErr error,
) {
	for i := range s.fairnessPartitions() {
		res, err := env.AdminClient().DescribeTaskQueuePartition(s.Context(), &adminservice.DescribeTaskQueuePartitionRequest{
			Namespace: env.Namespace().String(),
			TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
				TaskQueue:     tv.TaskQueue().Name,
				TaskQueueType: tp,
				PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(i)},
			},
			BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
		})
		if err != nil {
			return 0, 0, 0, 0, false, err
		}
		for _, versionInfoInternal := range res.VersionsInfoInternal {
			for _, st := range versionInfoInternal.PhysicalTaskQueueInfo.InternalTaskQueueStatus {
				if st.Draining {
					hasDraining = true
					tasksOnDraining += st.ApproximateBacklogCount
					loadedOnDraining += st.LoadedTasks
				} else {
					tasksOnActive += st.ApproximateBacklogCount
					loadedOnActive += st.LoadedTasks
				}
			}
		}
	}
	return
}

func (s *FairnessSuite) TestMigration_FromClassic(doAutoEnable bool) {
	// classic->fair, fair->pri. fair metadata will be created on transition.
	env, tv := s.newTestEnv(doAutoEnable)
	s.testMigration(env, tv, false, false)
}

func (s *FairnessSuite) TestMigration_FromPri(doAutoEnable bool) {
	// pri->fair, fair->pri. fair metadata will be created before transition.
	env, tv := s.newTestEnv(doAutoEnable)
	s.testMigration(env, tv, true, false)
}

func (s *FairnessSuite) TestMigration_FromFair(doAutoEnable bool) {
	// fair->pri, pri->fair. fair metadata will be created first.
	env, tv := s.newTestEnv(doAutoEnable)
	s.testMigration(env, tv, true, true)
}

func (s *FairnessSuite) TestUpdateWorkflowExecutionOptions_InvalidatesPendingTask(doAutoEnable bool) {
	if doAutoEnable {
		s.T().Skip("flaky with autoenable")
	}
	env, tv := s.newTestEnv(doAutoEnable)
	capture := env.StartNamespaceMetricCapture()

	originalPriority := &commonpb.Priority{FairnessKey: "KEY"}
	updatedPriority := &commonpb.Priority{FairnessKey: "NEW_KEY"}

	// Queue up new workflow.
	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    env.Namespace().String(),
		WorkflowId:   "workflow",
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Priority:     originalPriority,
	})
	s.NoError(err)

	// Wait for workflow task to be backlogged.
	s.Eventually(func() bool {
		resp, err := env.AdminClient().GetTaskQueueTasks(s.Context(), &adminservice.GetTaskQueueTasksRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			BatchSize:     10,
			MinPass:       1,
		})
		return err == nil && len(resp.GetTasks()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Update workflow options to set a new priority.
	updateResp, err := env.FrontendClient().UpdateWorkflowExecutionOptions(s.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow",
			RunId:      startResp.GetRunId(),
		},
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			Priority: updatedPriority,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	})
	s.NoError(err)
	s.NotNil(updateResp.GetWorkflowExecutionOptions())
	s.ProtoEqual(updatedPriority, updateResp.GetWorkflowExecutionOptions().GetPriority())

	// Query workflow to verify workflow has the updated priority.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow",
			RunId:      startResp.GetRunId(),
		},
	})
	s.NoError(err)
	s.NotNil(descResp.GetWorkflowExecutionInfo())
	s.NotNil(descResp.GetWorkflowExecutionInfo().GetPriority())
	s.ProtoEqual(updatedPriority, descResp.GetWorkflowExecutionInfo().GetPriority())

	// Poll for workflow task and schedule an activity.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.ContainsHistoryEvents(`
				3 WorkflowExecutionOptionsUpdated { "Priority": { "FairnessKey": "NEW_KEY" } }
				4 WorkflowTaskStarted
			`, task.History.Events)

			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             tv.ActivityID(),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								Priority:               originalPriority,
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
							},
						},
					},
				},
			}, nil
		},
		taskpoller.WithContext(s.Context()),
	)
	s.NoError(err)

	// Verify that 2 workflow tasks were sent to matching; and 1 was marked obsolete.
	addWorkflowTaskCount := 0
	obsoleteWorkflowTaskCount := 0
	for _, rec := range capture.Metric(metrics.ClientRequests.Name()) {
		for key, val := range rec.Tags {
			if key == metrics.OperationTagName && val == metrics.MatchingClientAddWorkflowTaskScope {
				addWorkflowTaskCount++
			}
		}
	}
	s.Equal(2, addWorkflowTaskCount, "Expected 2 workflow tasks to be dispatched to matching")
	for _, rec := range capture.Metric(metrics.ClientFailures.Name()) {
		for key, val := range rec.Tags {
			if key == metrics.ErrorTypeTagName && val == fmt.Sprintf("%T", serviceerror.ObsoleteMatchingTask{}) {
				obsoleteWorkflowTaskCount++
			}
		}
	}
	s.Equal(1, obsoleteWorkflowTaskCount, "Expected 1 workflow task to be obsolete")

	// Wait for activity task to be backlogged
	s.Eventually(func() bool {
		resp, err := env.AdminClient().GetTaskQueueTasks(s.Context(), &adminservice.GetTaskQueueTasksRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     tv.TaskQueue().Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			BatchSize:     10,
			MinPass:       1,
		})
		return err == nil && len(resp.GetTasks()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Update activity options to set a new priority
	_, err = env.FrontendClient().UpdateActivityOptions(s.Context(), &workflowservice.UpdateActivityOptionsRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow",
			RunId:      startResp.GetRunId(),
		},
		Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: tv.ActivityID()},
		ActivityOptions: &activitypb.ActivityOptions{
			Priority: updatedPriority,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	})
	s.NoError(err)

	// Query workflow to verify activity has the updated priority.
	descResp, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow",
			RunId:      startResp.GetRunId(),
		},
	})
	s.NoError(err)
	s.NotNil(descResp.GetPendingActivities())
	s.Len(descResp.GetPendingActivities(), 1)
	s.ProtoEqual(updatedPriority, descResp.GetPendingActivities()[0].GetPriority())
	s.ProtoEqual(updatedPriority, descResp.GetPendingActivities()[0].GetActivityOptions().GetPriority())

	// Poll for activity task and verify it has the updated priority.
	_, err = env.TaskPoller().PollAndHandleActivityTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Equal(tv.ActivityID(), task.ActivityId)
			s.ProtoEqual(updatedPriority, task.Priority)
			return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
		},
		taskpoller.WithContext(s.Context()),
	)
	s.NoError(err)

	// Verify that 2 activity tasks were sent to matching; and 1 was marked obsolete
	addActivityTaskCount := 0
	obsoleteActivityTaskCount := 0
	for _, rec := range capture.Metric(metrics.ClientRequests.Name()) {
		for key, val := range rec.Tags {
			if key == metrics.OperationTagName && val == metrics.MatchingClientAddActivityTaskScope {
				addActivityTaskCount++
			}
		}
	}
	s.Equal(2, addActivityTaskCount, "Expected 2 activity tasks to be dispatched to matching")
	for _, rec := range capture.Metric(metrics.ClientFailures.Name()) {
		for key, val := range rec.Tags {
			if key == metrics.ErrorTypeTagName && val == fmt.Sprintf("%T", serviceerror.ObsoleteMatchingTask{}) {
				obsoleteActivityTaskCount++
			}
		}
	}
	s.Equal(1+obsoleteWorkflowTaskCount, obsoleteActivityTaskCount, "Expected 1 activity task to be obsolete")
}
