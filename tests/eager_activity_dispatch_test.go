package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestEagerActivityWithMatchingGrant_Unversioned(t *testing.T) {
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableActivityEagerExecution, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableEagerActivityDispatchCheck, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4),
	)
	env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceWritePartition, 1))
	env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceReadPartition, 1))

	tv := env.Tv()
	startEagerActivityTestWorkflow(t, env, tv)

	poller := env.TaskPoller()
	response, err := poller.PollAndHandleWorkflowTask(
		tv,
		scheduleActivityForEagerDispatchTest(t, tv, true),
	)
	require.NoError(t, err)
	require.Len(t, response.GetActivityTasks(), 1)

	eagerActivity := response.GetActivityTasks()[0]
	require.Equal(t, tv.ActivityID(), eagerActivity.GetActivityId())
	_, err = poller.HandleActivityTask(tv, eagerActivity, taskpoller.CompleteActivityTask(tv))
	require.NoError(t, err)

	_, err = poller.PollAndHandleWorkflowTask(
		tv,
		taskpoller.CompleteWorkflowHandler,
		taskpoller.WithTimeout(20*time.Second),
	)
	require.NoError(t, err)
}

func TestEagerActivityFallsBackWhenBacklogExists(t *testing.T) {
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableActivityEagerExecution, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableEagerActivityDispatchCheck, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingBacklogNegligibleAge, time.Duration(0)),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	tvBacklogged := env.Tv().WithWorkflowIDNumber(1).WithActivityIDNumber(1)
	tvEager := env.Tv().WithWorkflowIDNumber(2).WithActivityIDNumber(2)
	poller := env.TaskPoller()

	startEagerActivityTestWorkflow(t, env, tvBacklogged)
	response, err := poller.PollAndHandleWorkflowTask(
		tvBacklogged,
		scheduleActivityForEagerDispatchTest(t, tvBacklogged, false),
	)
	require.NoError(t, err)
	require.Empty(t, response.GetActivityTasks())

	require.Eventually(t, func() bool {
		response, err := env.FrontendClient().DescribeTaskQueue(env.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     tvBacklogged.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			ReportStats:   true,
		})
		return err == nil && response.GetStats().GetApproximateBacklogCount() >= 1
	}, 10*time.Second, 100*time.Millisecond, "first activity never reached the matching backlog")

	startEagerActivityTestWorkflow(t, env, tvEager)
	response, err = poller.PollAndHandleWorkflowTask(
		tvEager,
		scheduleActivityForEagerDispatchTest(t, tvEager, true),
	)
	require.NoError(t, err)
	require.Empty(t, response.GetActivityTasks(), "an eager activity must not jump an existing backlog")

	activityIDs := make([]string, 0, 2)
	for _, tv := range []*testvars.TestVars{tvBacklogged, tvEager} {
		_, err := poller.PollAndHandleActivityTask(tv,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				activityIDs = append(activityIDs, task.GetActivityId())
				return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
			},
			taskpoller.WithTimeout(20*time.Second),
		)
		require.NoError(t, err)
	}
	require.Equal(t, []string{tvBacklogged.ActivityID(), tvEager.ActivityID()}, activityIDs)
}

func startEagerActivityTestWorkflow(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars) {
	t.Helper()
	_, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.RequestID(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Identity:                 tv.WorkerIdentity(),
		WorkflowExecutionTimeout: durationpb.New(time.Minute),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
}

func scheduleActivityForEagerDispatchTest(
	t *testing.T,
	tv *testvars.TestVars,
	eager bool,
) func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
	t.Helper()
	return func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             tv.ActivityID(),
						ActivityType:           tv.ActivityType(),
						TaskQueue:              tv.TaskQueue(),
						ScheduleToCloseTimeout: durationpb.New(time.Minute),
						StartToCloseTimeout:    durationpb.New(time.Minute),
						RequestEagerExecution:  eager,
					},
				},
			}},
		}, nil
	}
}
