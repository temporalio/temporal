package tests

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PrioritySuite struct {
	testcore.FunctionalTestBase
}

func TestPrioritySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PrioritySuite))
}

func (s *PrioritySuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key():     true,
		dynamicconfig.MatchingGetTasksBatchSize.Key(): 20,
		dynamicconfig.MatchingGetTasksReloadAt.Key():  5,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *PrioritySuite) TestPriority_Activity_Basic() {
	const N = 100
	const Levels = 5

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for wfidx := range N {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks
	for range N {
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
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
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	// process activity tasks
	var runs []int
	for range N * Levels {
		_, err := s.TaskPoller().PollAndHandleActivityTask(
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
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	w := wrongorderness(runs)
	s.T().Log("wrongorderness:", w)
	s.Less(w, 0.1)
}

func (s *PrioritySuite) TestSubqueue_Migration() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// start with old matcher
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	// start 100 workflows
	for range 100 {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   uuid.NewString(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks and create 300 activities
	for range 100 {
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
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
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	processActivity := func() {
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := s.TaskPoller().PollAndHandleActivityTask(
				tv,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					nothing, err := payloads.Encode()
					s.NoError(err)
					return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
				},
				taskpoller.WithContext(ctx),
			)
			assert.NoError(c, err)
		}, 5*time.Second, time.Millisecond)
	}

	s.T().Log("process first 100 activities")
	for range 100 {
		processActivity()
	}

	s.T().Log("switching to new matcher")
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, true)

	s.T().Log("processing next 100 activities")
	for range 100 {
		processActivity()
	}

	s.T().Log("switching back to old matcher")
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	s.T().Log("processing last 100 activities")
	for range 100 {
		processActivity()
	}
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
	testcore.FunctionalTestBase
	partitions int
}

func TestFairnessSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(FairnessSuite))
}

func (s *FairnessSuite) SetupSuite() {
	s.partitions = 1
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingUseNewMatcher.Key():          true,
		dynamicconfig.MatchingEnableFairness.Key():         true,
		dynamicconfig.MatchingGetTasksBatchSize.Key():      20,
		dynamicconfig.MatchingGetTasksReloadAt.Key():       5,
		dynamicconfig.NumPendingActivitiesLimitError.Key(): 1000,
		// TODO: disable this and use default later?
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  s.partitions,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): s.partitions,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *FairnessSuite) TestFairness_Activity_Basic() {
	const Workflows = 15
	const Tasks = 15
	const Keys = 10

	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zipf := rand.NewZipf(rand.New(rand.NewSource(12345)), 2, 2, Keys-1)

	for wfidx := range Workflows {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   fmt.Sprintf("wf%d", wfidx),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}

	// process workflow tasks
	for range Workflows {
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(
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
			taskpoller.WithContext(ctx),
		)
		s.NoError(err)
	}

	// process activity tasks
	var runs []int
	for range Workflows * Tasks {
		_, err := s.TaskPoller().PollAndHandleActivityTask(
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
			taskpoller.WithContext(ctx),
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

func (s *FairnessSuite) testMigration(newMatcher, fairness bool) {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(dynamicconfig.MatchingEnableMigration, true)

	forTest := func(v any) any {
		return []dynamicconfig.ConstrainedValue{
			// test tqs (both wf and activity)
			dynamicconfig.ConstrainedValue{
				Constraints: dynamicconfig.Constraints{
					Namespace:     s.Namespace().String(),
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
		s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, forTest(newMatcher))
		s.OverrideDynamicConfig(dynamicconfig.MatchingEnableFairness, forTest(fairness))
	}
	waitForTasks := func(tp enumspb.TaskQueueType, onDraining, onActive int64) {
		s.T().Helper()
		s.EventuallyWithT(func(c *assert.CollectT) {
			tasksOnDraining, tasksOnActive, err := s.countTasksByDrainingActive(ctx, tv, tp)
			require.NoError(c, err)
			require.Equal(c, onDraining, tasksOnDraining)
			require.Equal(c, onActive, tasksOnActive)
		}, 15*time.Second, 250*time.Millisecond)
	}

	setConfig("initial", newMatcher, fairness)

	// start 20 workflows. 20 tasks will be queued on wft queue.
	s.T().Log("starting workflows")
	for range 20 {
		_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace().String(),
			WorkflowId:   uuid.NewString(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
		})
		s.NoError(err)
	}
	waitForTasks(enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0, 20)

	processWft := func() {
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := s.TaskPoller().PollAndHandleWorkflowTask(
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
				taskpoller.WithContext(ctx),
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
	s.T().Log("wfts done")

	// process activities 1/3 at a time
	processActivity := func() {
		s.EventuallyWithT(func(c *assert.CollectT) {
			_, err := s.TaskPoller().PollAndHandleActivityTask(
				tv,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					nothing, err := payloads.Encode()
					s.NoError(err)
					return &workflowservice.RespondActivityTaskCompletedRequest{Result: nothing}, nil
				},
				taskpoller.WithContext(ctx),
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
}

func (s *FairnessSuite) countTasksByDrainingActive(ctx context.Context, tv *testvars.TestVars, tp enumspb.TaskQueueType) (
	tasksOnDraining, tasksOnActive int64, retErr error,
) {
	for i := range s.partitions {
		res, err := s.AdminClient().DescribeTaskQueuePartition(ctx, &adminservice.DescribeTaskQueuePartitionRequest{
			Namespace: s.Namespace().String(),
			TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
				TaskQueue:     tv.TaskQueue().Name,
				TaskQueueType: tp,
				PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(i)},
			},
			BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
		})
		if err != nil {
			return 0, 0, err
		}
		for _, versionInfoInternal := range res.VersionsInfoInternal {
			for _, st := range versionInfoInternal.PhysicalTaskQueueInfo.InternalTaskQueueStatus {
				if st.Draining {
					tasksOnDraining += st.ApproximateBacklogCount
				} else {
					tasksOnActive += st.ApproximateBacklogCount
				}
			}
		}
	}
	return
}

func (s *FairnessSuite) TestFairness_Migration_FromClassic() {
	// classic->fair, fair->pri. fair metadata will be created on transition.
	s.testMigration(false, false)
}

func (s *FairnessSuite) TestFairness_Migration_FromPri() {
	// pri->fair, fair->pri. fair metadata will be created before transition.
	s.testMigration(true, false)
}

func (s *FairnessSuite) TestFairness_Migration_FromFair() {
	// fair->pri, pri->fair. fair metadata will be created first.
	s.testMigration(true, true)
}
