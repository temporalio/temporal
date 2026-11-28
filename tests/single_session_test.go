package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/service/matching/hooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/*
TestSingleSessionAcquisitionAndReplacement tests admission, routing, and replacement:
1. Start A on the normal queue and reject a session poll while the admission flag is disabled.
2. Enable admission, acquire A through S, and schedule an activity in A's session.
3. Disable admission, signal A, and verify both sticky backlogs remain while both poll types are rejected.
4. Re-enable admission and deliver the original activity and workflow tasks at attempt 1.
5. Start B, unload S, and verify reloading S cannot acquire B while A remains running.
6. Resume A with a replacement poller using the same stable identity S, then let a fresh S2 acquire B.
7. Verify A keeps its original normal task queue and remains running.
*/
func TestSingleSessionAcquisitionAndReplacement(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv()
	namespace := env.Namespace().String()
	client := env.FrontendClient()
	matching := env.GetTestCluster().MatchingClient()
	poller := taskpoller.New(t, client, namespace)
	session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
	}}
	_, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	workflowPollRequest := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace, TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(),
	}
	activityPollRequest := &workflowservice.PollActivityTaskQueueRequest{
		Namespace: namespace, TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(),
	}
	_, err = client.PollWorkflowTaskQueue(testcore.NewContext(t.Context()), workflowPollRequest)
	var disabled *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &disabled)
	env.OverrideDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true)

	// With SessionOptions.StableIdentity set to the sticky task queue name, the worker will only poll the sticky task queue with poll forwarding to
	// normal queue to only happen for the very first workflow task
	stickyPoller := poller.PollWorkflowTask(workflowPollRequest)
	_, err = stickyPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId: tv.ActivityID(), ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				SessionOptions:      &sessionpb.ActivitySessionOptions{Placement: &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}}},
			}},
		}}}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	env.OverrideDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, false)
	_, err = client.SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace, WorkflowExecution: tv.WorkflowExecution(), SignalName: tv.SignalName(),
	})
	require.NoError(t, err)
	await.Require(t.Context(), t, func(t *await.T) {
		for _, taskType := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
			response, err := matching.DescribeTaskQueuePartition(t.Context(), &matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: env.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: taskType,
					PartitionId: &taskqueuespb.TaskQueuePartition_StickyName{StickyName: tv.StickyTaskQueue().GetName()},
				},
				Versions: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true}, ReportStats: true, OnlyIfLoaded: true,
			})
			require.NoError(t, err)
			require.Positive(t, response.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats().GetApproximateBacklogCount())
		}
	}, 10*time.Second, 10*time.Millisecond)
	_, err = client.PollWorkflowTaskQueue(testcore.NewContext(t.Context()), workflowPollRequest)
	require.ErrorAs(t, err, &disabled)
	_, err = client.PollActivityTaskQueue(testcore.NewContext(t.Context()), activityPollRequest)
	require.ErrorAs(t, err, &disabled)
	env.OverrideDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true)

	_, err = poller.PollActivityTask(activityPollRequest).HandleTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		require.Equal(t, tv.ActivityID(), task.GetActivityId())
		require.Equal(t, int32(1), task.GetAttempt())
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	_, err = stickyPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		require.Equal(t, int32(1), task.GetAttempt())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	// Starting a new workflow execution with a different workflow ID. This is done to show that tasks from this workflow execution
	// shall never be executed by the worker that is part of the first stable identity. The first task from this workflow execution
	// shall be picked up from the normal queue, after which it shall be assigned a new stable identity.
	other := tv.WithWorkflowIDNumber(2)
	_, err = client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: other.WorkflowID(), RequestId: other.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	await.Require(t.Context(), t, func(t *await.T) {
		response, err := client.DescribeTaskQueue(t.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: namespace, TaskQueue: tv.TaskQueue(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ReportStats: true,
		})
		require.NoError(t, err)
		require.Positive(t, response.GetStats().GetApproximateBacklogCount())
	}, 10*time.Second, 10*time.Millisecond)

	_, err = matching.ForceUnloadTaskQueuePartition(t.Context(), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: env.NamespaceID().String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			PartitionId: &taskqueuespb.TaskQueuePartition_StickyName{StickyName: tv.StickyTaskQueue().GetName()},
		},
	})
	require.NoError(t, err)
	empty, err := matching.PollWorkflowTaskQueue(t.Context(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: env.NamespaceID().String(),
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(), Identity: tv.WorkerIdentity(),
		},
		Conditions: &matchingservice.PollConditions{NoWait: true},
	})
	require.NoError(t, err)
	require.Empty(t, empty.GetTaskToken(), "assigned S cannot acquire B after reload while A is still running")

	_, err = client.SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace, WorkflowExecution: tv.WorkflowExecution(), SignalName: tv.SignalName(),
	})
	require.NoError(t, err)
	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(), Identity: tv.WorkerIdentity() + "-replacement",
	}).HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	freshQueue := tv.StickyTaskQueue()
	freshQueue.Name = tv.StickyTaskQueue().GetName() + "-2"
	// Changing the worker identity out here (synonymous to changing the ACS identity to something like "S2"
	// to show that the worker identity is different from the original one, and is also able to acquire the new workflow's task)
	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: freshQueue,
		SessionOptions: &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: freshQueue.GetName()},
		}},
		WorkerInstanceKey: tv.WorkerInstanceKey(),
	}).HandleTask(other, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, other.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	description, err := client.DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace, Execution: tv.WorkflowExecution(),
	})
	require.NoError(t, err)
	require.Equal(t, tv.TaskQueue().GetName(), description.GetExecutionConfig().GetTaskQueue().GetName())
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.GetWorkflowExecutionInfo().GetStatus())
}

/*
TestSingleSessionConcurrentAcquisition tests exclusive acquisition with concurrent polls:
1. Start A and B on the same normal queue and wait for both tasks to reach its backlog.
2. Release eight Matching polls together, all using the same stable identity S.
3. Verify exactly one poll receives a workflow task and the other polls return empty.
4. Complete the winning workflow task and let a fresh S2 acquire the other execution.
*/
func TestSingleSessionConcurrentAcquisition(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, time.Second),
		testcore.WithDynamicConfig(dynamicconfig.MatchingBacklogNegligibleAge, 20*time.Millisecond),
		testcore.WithDynamicConfig(dynamicconfig.MatchingEphemeralDataUpdateInterval, 20*time.Millisecond),
	)
	tv := env.Tv()
	client := env.FrontendClient()
	for _, execution := range []string{tv.WorkflowID(), tv.WithWorkflowIDNumber(2).WorkflowID()} {
		_, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace: env.Namespace().String(), WorkflowId: execution, RequestId: tv.Any().String(),
			WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
			WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
	}
	await.Require(t.Context(), t, func(t *await.T) {
		response, err := client.DescribeTaskQueue(t.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: env.Namespace().String(), TaskQueue: tv.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, ReportStats: true,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, response.GetStats().GetApproximateBacklogCount(), int64(2))
	}, 10*time.Second, 10*time.Millisecond)

	session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
	}}
	type pollResult struct {
		response *matchingservice.PollWorkflowTaskQueueResponse
		err      error
	}
	const pollers = 8
	results := make(chan pollResult, pollers)
	start := make(chan struct{})
	matching := env.GetTestCluster().MatchingClient()
	for range pollers {
		go func() {
			<-start
			response, err := matching.PollWorkflowTaskQueue(t.Context(), &matchingservice.PollWorkflowTaskQueueRequest{
				NamespaceId: env.NamespaceID().String(),
				PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
					TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(), Identity: tv.WorkerIdentity(),
				},
			})
			results <- pollResult{response: response, err: err}
		}()
	}
	close(start)
	var acquired []*matchingservice.PollWorkflowTaskQueueResponse
	for range pollers {
		result := await.Rcv(t, results)
		require.NoError(t, result.err)
		if len(result.response.GetTaskToken()) > 0 {
			acquired = append(acquired, result.response)
		}
	}
	require.Len(t, acquired, 1, "only one initial acquisition wins for S")
	winner := acquired[0]
	expectedOther := tv.WithWorkflowIDNumber(2).WorkflowID()
	if winner.GetWorkflowExecution().GetWorkflowId() == expectedOther {
		expectedOther = tv.WorkflowID()
	} else {
		require.Equal(t, tv.WorkflowID(), winner.GetWorkflowExecution().GetWorkflowId())
	}
	_, err := client.RespondWorkflowTaskCompleted(t.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(), TaskToken: winner.GetTaskToken(), Identity: tv.WorkerIdentity(),
	})
	require.NoError(t, err)

	freshQueue := tv.StickyTaskQueue()
	freshQueue.Name = freshQueue.GetName() + "-2"
	_, err = taskpoller.New(t, client, env.Namespace().String()).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(), TaskQueue: freshQueue, Identity: tv.WorkerIdentity(),
		SessionOptions: &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: freshQueue.GetName()},
		}},
		WorkerInstanceKey: tv.WorkerInstanceKey(),
	}).HandleTask(tv.WithWorkflowID(expectedOther), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, expectedOther, task.GetWorkflowExecution().GetWorkflowId(), "the unrelated execution remains available to S2")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
}

/*
TestSingleSessionWorkflowBacklogWithoutPollers tests offline backlog and queue reload:
1. Acquire A through S and complete its first workflow task.
2. Unload S and signal A without starting another poller.
3. Verify Matching reloads S with a backlog and no pollers, with no normal backlog or recorded schedule-to-start timeout,
and DescribeTaskQueue on S (as a worker controller calls it) reports that backlog.
4. Unload S again, verify DescribeTaskQueue on S still reports the backlog, and resume the original workflow task at
attempt 1 from a replacement worker process (same stable identity, new instance key).
5. Verify the backlog drains to zero.
*/
func TestSingleSessionWorkflowBacklogWithoutPollers(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv()
	client := env.FrontendClient()
	namespace := env.Namespace().String()
	_, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
	}}
	poller := taskpoller.New(t, client, namespace).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(),
	})
	_, err = poller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	matching := env.GetTestCluster().MatchingClient()
	partition := &taskqueuespb.TaskQueuePartition{
		TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		PartitionId: &taskqueuespb.TaskQueuePartition_StickyName{StickyName: tv.StickyTaskQueue().GetName()},
	}
	// Unloading the task queue partition to simulate a worker was absent for a while.
	_, err = matching.ForceUnloadTaskQueuePartition(t.Context(), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: env.NamespaceID().String(), TaskQueuePartition: partition,
	})
	require.NoError(t, err)
	// Signaling the workflow execution to trigger a workflow task.
	_, err = client.SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace, WorkflowExecution: tv.WorkflowExecution(), SignalName: tv.SignalName(),
	})
	require.NoError(t, err)

	// A worker controller reads the sticky backlog by the worker's stable identity, with the normal task queue name.
	describeSticky := func(ctx context.Context, t require.TestingT) int64 {
		response, err := client.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace: namespace, TaskQueue: tv.StickyTaskQueue(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, ReportStats: true,
		})
		require.NoError(t, err)
		return response.GetStats().GetApproximateBacklogCount()
	}
	// After unloading the sticky task queue partition, the newly scheduled workflow task shall be spooled by loading the task queue partition again.
	await.Require(t.Context(), t, func(t *await.T) {
		history := env.SdkClient().GetWorkflowHistory(t.Context(), tv.WorkflowID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		var timeoutCount int
		var signaled bool
		for history.HasNext() {
			event, err := history.Next()
			require.NoError(t, err)
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signaled = true
			}
			if scheduled := event.GetWorkflowTaskScheduledEventAttributes(); scheduled != nil && signaled {
				require.Equal(t, tv.StickyTaskQueue().GetName(), scheduled.GetTaskQueue().GetName())
				require.Equal(t, enumspb.TASK_QUEUE_KIND_STICKY, scheduled.GetTaskQueue().GetKind())
			}
			if timedOut := event.GetWorkflowTaskTimedOutEventAttributes(); timedOut != nil {
				require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, timedOut.GetTimeoutType())
				timeoutCount++
			}
		}
		require.Zero(t, timeoutCount, "the bound workflow task remains queued until a worker resumes")
		response, err := matching.DescribeTaskQueuePartition(t.Context(), &matchingservice.DescribeTaskQueuePartitionRequest{
			NamespaceId: env.NamespaceID().String(), TaskQueuePartition: partition,
			Versions:    &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
			ReportStats: true, ReportPollers: true, OnlyIfLoaded: true,
		})
		require.NoError(t, err)
		physical := response.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo()
		require.Empty(t, physical.GetPollers())
		require.Positive(t, physical.GetTaskQueueStats().GetApproximateBacklogCount())
		require.Equal(t, physical.GetTaskQueueStats().GetApproximateBacklogCount(), describeSticky(t.Context(), t))
		normal, err := client.DescribeTaskQueue(t.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: namespace, TaskQueue: tv.TaskQueue(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, ReportStats: true,
		})
		require.NoError(t, err)
		require.Zero(t, normal.GetStats().GetApproximateBacklogCount())
	}, 40*time.Second, 200*time.Millisecond)

	_, err = matching.ForceUnloadTaskQueuePartition(t.Context(), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: env.NamespaceID().String(), TaskQueuePartition: partition,
	})
	require.NoError(t, err)
	require.Positive(t, describeSticky(t.Context(), t), "describing the unloaded sticky queue loads it and reports its backlog")
	replacement := taskpoller.New(t, client, namespace).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: "replacement-" + tv.WorkerInstanceKey(),
	})
	_, err = replacement.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		require.Equal(t, int32(1), task.GetAttempt(), "the original workflow task resumes without a retry")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	await.Require(t.Context(), t, func(t *await.T) {
		require.Zero(t, describeSticky(t.Context(), t))
	}, 10*time.Second, 200*time.Millisecond)
}

/*
TestSingleSessionAcquisitionUsesNormalPolling tests initial forwarding and subsequent sticky routing
with and without worker-versioning (Pinned only)
*/
func TestSingleSessionAcquisitionUsesNormalPolling(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		readPartition  int
		writePartition int
		pinned         bool
	}{
		{name: "root", readPartition: 0, writePartition: 0},
		{name: "non-root", readPartition: 2, writePartition: 2},
		{name: "pinned non-root", readPartition: 2, writePartition: 2, pinned: true},
		{name: "pinned parent forwarding", readPartition: 2, writePartition: 0, pinned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newVersioning3TestEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 3),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 3),
				testcore.WithDynamicConfig(dynamicconfig.MatchingForwarderMaxOutstandingTasks, 0),
				testcore.WithDynamicConfig(dynamicconfig.MatchingPriorityBacklogForwarding, false),
			)
			env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceReadPartition, tc.readPartition))
			env.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceWritePartition, tc.writePartition))
			tv := env.Tv()
			client := env.FrontendClient()
			matching := env.GetTestCluster().MatchingClient()
			stickyQueue := tv.StickyTaskQueue()
			startRequest := &workflowservice.StartWorkflowExecutionRequest{
				Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
				WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
				WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(30 * time.Second),
			}
			pollRequest := &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: stickyQueue, Identity: tv.WorkerIdentity(), WorkerInstanceKey: tv.WorkerInstanceKey(),
				SessionOptions: &sessionpb.WorkerSessionOptions{
					Admission: &sessionpb.WorkerSessionOptions_SingleSession{
						SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: stickyQueue.GetName()},
					},
				},
			}
			versionKey := ""
			if tc.pinned {
				startRequest.VersioningOverride = tv.VersioningOverridePinned()
				pollRequest.DeploymentOptions = tv.WorkerDeploymentOptions(true)
				versionKey = tv.DeploymentVersionStringV32()
				_, err := matching.SyncDeploymentUserData(t.Context(), &matchingservice.SyncDeploymentUserDataRequest{
					NamespaceId: env.NamespaceID().String(), TaskQueue: tv.TaskQueue().GetName(),
					TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
					DeploymentName: tv.DeploymentSeries(),
					UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
						tv.BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE},
					},
				})
				require.NoError(t, err)
				_, err = client.StartWorkflowExecution(t.Context(), startRequest)
				require.NoError(t, err)
				await.Require(t.Context(), t, func(t *await.T) {
					response, err := matching.DescribeTaskQueuePartition(t.Context(), &matchingservice.DescribeTaskQueuePartitionRequest{
						NamespaceId: env.NamespaceID().String(),
						TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
							TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
							PartitionId: &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(tc.writePartition)},
						},
						Versions:    &taskqueuepb.TaskQueueVersionSelection{Unversioned: true, AllActive: true},
						ReportStats: true, ReportPollers: true, OnlyIfLoaded: true,
					})
					require.NoError(t, err)
					versions := response.GetVersionsInfoInternal()
					require.Zero(t, versions[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats().GetApproximateBacklogCount())
					require.Positive(t, versions[versionKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats().GetApproximateBacklogCount())
					for _, version := range versions {
						require.Empty(t, version.GetPhysicalTaskQueueInfo().GetPollers())
					}
				}, 10*time.Second, 20*time.Millisecond)
			}

			poller := taskpoller.New(t, client, env.Namespace().String()).PollWorkflowTask(pollRequest)
			acquired := make(chan *workflowservice.PollWorkflowTaskQueueResponse, 2)
			handle := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				acquired <- task
				response := &workflowservice.RespondWorkflowTaskCompletedRequest{}
				if tc.pinned {
					response.VersioningBehavior = enumspb.VERSIONING_BEHAVIOR_PINNED
					response.DeploymentOptions = tv.WorkerDeploymentOptions(true)
				}
				return response, nil
			}
			completed := make(chan error, 1)
			go func() {
				_, err := poller.HandleTask(tv, handle, taskpoller.WithContext(t.Context()))
				completed <- err
			}()

			// Only the sticky poll is started; its acquisition reaches the normal read partition without advertisements.
			// The main idea of this test is that for workers that have opted-in to Sessions and Worker-Versioning, their first poll should
			// land on a normal task queue partition. After that, the poll metadata shall dictate if this poller is matched with a versioned task
			// or a non-versioned task.
			await.Require(t.Context(), t, func(t *await.T) {
				response, err := matching.DescribeTaskQueuePartition(t.Context(), &matchingservice.DescribeTaskQueuePartitionRequest{
					NamespaceId: env.NamespaceID().String(),
					TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
						TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
						PartitionId: &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(tc.readPartition)},
					},
					Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true, AllActive: true},
					ReportPollers: true, OnlyIfLoaded: true,
				})
				require.NoError(t, err)
				pollers := response.GetVersionsInfoInternal()[versionKey].GetPhysicalTaskQueueInfo().GetPollers()
				require.NotEmpty(t, pollers)
				require.Equal(t, tv.WorkerIdentity(), pollers[0].GetIdentity())
			}, 10*time.Second, 20*time.Millisecond)
			if !tc.pinned {
				_, err := client.StartWorkflowExecution(t.Context(), startRequest)
				require.NoError(t, err)
			}
			require.NoError(t, await.Rcv(t, completed))
			require.Equal(t, tv.WorkflowID(), await.Rcv(t, acquired).GetWorkflowExecution().GetWorkflowId())

			sticky, err := matching.DescribeTaskQueuePartition(t.Context(), &matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: env.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
					PartitionId: &taskqueuespb.TaskQueuePartition_StickyName{StickyName: stickyQueue.GetName()},
				},
				Versions: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true, AllActive: true},
			})
			require.NoError(t, err)
			require.Len(t, sticky.GetVersionsInfoInternal(), 1)
			require.Contains(t, sticky.GetVersionsInfoInternal(), "", "S has no versioned physical queues")

			_, err = client.SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: env.Namespace().String(), WorkflowExecution: tv.WorkflowExecution(), SignalName: tv.SignalName(),
			})
			require.NoError(t, err)
			_, err = poller.HandleTask(tv, handle, taskpoller.WithContext(t.Context()))
			require.NoError(t, err)
			require.Equal(t, tv.WorkflowID(), await.Rcv(t, acquired).GetWorkflowExecution().GetWorkflowId())
		})
	}
}

type singleSessionTaskHookEvent struct {
	kind     enumspb.TaskQueueKind
	taskType enumspb.TaskQueueType
	details  hooks.TaskAddHookDetails
}

type singleSessionTaskHookFactory struct {
	taskQueue string
	events    chan<- singleSessionTaskHookEvent
}

func (f *singleSessionTaskHookFactory) Create(details *hooks.TaskHookFactoryCreateDetails) hooks.TaskHook {
	// Ignore internal queues created by the test cluster so only this workflow contributes events.
	if details.Partition.TaskQueue().Name() != f.taskQueue {
		return nil
	}
	return &singleSessionTaskHook{
		kind:     details.Partition.Kind(),
		taskType: details.Partition.TaskType(),
		events:   f.events,
	}
}

type singleSessionTaskHook struct {
	kind     enumspb.TaskQueueKind
	taskType enumspb.TaskQueueType
	events   chan<- singleSessionTaskHookEvent
}

func (h *singleSessionTaskHook) Start() {}
func (h *singleSessionTaskHook) Stop()  {}

func (h *singleSessionTaskHook) ProcessTaskAdd(ctx context.Context, event *hooks.TaskAddHookDetails) {
	// Keep a snapshot because callbacks and assertions run on different goroutines.
	details := *event
	details.DeploymentVersion = common.CloneProto(event.DeploymentVersion)
	select {
	case h.events <- singleSessionTaskHookEvent{kind: h.kind, taskType: h.taskType, details: details}:
	case <-ctx.Done():
	}
}

// TestSingleSessionTaskAddHooks verifies session context reaches task-add hooks.
// The first workflow task is unbound; acquiring it binds the workflow to sticky queue S. An opted-in activity,
// a query sent while S has no poller, and the workflow task following the activity must then carry S in their hook callbacks.
// The workflow is pinned to v1 while the queue's current version is v2, so every callback must report v1.
func TestSingleSessionTaskAddHooks(t *testing.T) {
	t.Parallel()
	env := newVersioning3TestEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv().WithBuildIDNumber(1)
	current := tv.WithBuildIDNumber(2)

	// Install the observer before this test's queue partitions load, and force spooling for a deterministic backlog outcome.
	events := make(chan singleSessionTaskHookEvent, 16)
	var factory hooks.TaskHookFactory = &singleSessionTaskHookFactory{taskQueue: tv.TaskQueue().GetName(), events: events}
	env.InjectHook(testhooks.NewHook(testhooks.MatchingTaskHookFactory, factory))
	env.InjectHook(testhooks.NewHook(testhooks.MatchingDisableSyncMatch, true))
	client := env.FrontendClient()

	// A different current version catches hooks that report the routing target instead of the task's pinned version.
	_, err := env.GetTestCluster().MatchingClient().SyncDeploymentUserData(t.Context(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId: env.NamespaceID().String(), TaskQueue: tv.TaskQueue().GetName(),
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		DeploymentName: tv.DeploymentSeries(),
		UpdateRoutingConfig: &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  current.ExternalDeploymentVersion(),
			CurrentVersionChangedTime: timestamppb.Now(), RevisionNumber: 1,
		},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			tv.BuildID():      {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE},
			current.BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE},
		},
	})
	require.NoError(t, err)

	// This initial task is added before session acquisition, so its hook must have no session options.
	_, err = client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(), VersioningOverride: tv.VersioningOverridePinned(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)

	// Polling S forwards acquisition to the normal queue; accepting the first task establishes the binding to S.
	session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
	}}
	poller := taskpoller.New(t, client, env.Namespace().String())
	workflowPoller := poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(), DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})

	// The activity opts into the workflow session, letting History resolve its logical queue to S.
	_, err = workflowPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED, DeploymentOptions: tv.WorkerDeploymentOptions(true),
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId: tv.ActivityID(), ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
					StartToCloseTimeout: durationpb.New(10 * time.Second),
					SessionOptions:      &sessionpb.ActivitySessionOptions{Placement: &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}}},
				}},
			}},
		}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	// Unloading S makes its worker look absent, so the query reaches the no-poller hook, which must carry S to wake it.
	_, err = env.GetTestCluster().MatchingClient().ForceUnloadTaskQueuePartition(t.Context(), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: env.NamespaceID().String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue: tv.TaskQueue().GetName(), TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			PartitionId: &taskqueuespb.TaskQueuePartition_StickyName{StickyName: tv.StickyTaskQueue().GetName()},
		},
	})
	require.NoError(t, err)
	queryErr := make(chan error, 1)
	go func() {
		_, err := client.QueryWorkflow(t.Context(), &workflowservice.QueryWorkflowRequest{
			Namespace: env.Namespace().String(), Execution: tv.WorkflowExecution(), Query: tv.Query(),
		})
		queryErr <- err
	}()
	// Poll S only after the query's hook fires; an earlier poller would suppress the no-poller hook.
	// No workflow task is pending, so the first sticky workflow callback belongs to the query.
	var received []singleSessionTaskHookEvent
	for {
		event := await.Rcv(t, events)
		received = append(received, event)
		if event.kind == enumspb.TASK_QUEUE_KIND_STICKY && event.taskType == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			break
		}
	}
	_, err = workflowPoller.HandleLegacyQuery(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error) {
		return &workflowservice.RespondQueryTaskCompletedRequest{CompletedType: enumspb.QUERY_RESULT_TYPE_ANSWERED}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	require.NoError(t, await.Rcv(t, queryErr))

	// Completing the activity on S schedules another workflow task on the same bound sticky queue.
	_, err = poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(), DeploymentOptions: tv.WorkerDeploymentOptions(true),
	}).HandleTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, tv.ActivityID(), task.GetActivityId())
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	_, err = workflowPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED, DeploymentOptions: tv.WorkerDeploymentOptions(true),
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			}},
		}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	// Match the four callbacks (first WFT, activity, query, next WFT) as a multiset because independent partitions
	// may deliver them in any order. Only the unbound first workflow task, on the normal queue, lacks S.
	for len(received) < 4 {
		received = append(received, await.Rcv(t, events))
	}
	type hookKey struct {
		kind     enumspb.TaskQueueKind
		taskType enumspb.TaskQueueType
	}
	var keys []hookKey
	for _, event := range received {
		expectedOptions := hooks.SingleSessionOptions{WorkerStableIdentity: tv.StickyTaskQueue().GetName()}
		if event.kind == enumspb.TASK_QUEUE_KIND_NORMAL {
			expectedOptions = hooks.SingleSessionOptions{}
		}
		require.Equal(t, expectedOptions, event.details.SingleSessionOptions)
		protorequire.ProtoEqual(t, tv.ExternalDeploymentVersion(), event.details.DeploymentVersion)
		require.Equal(t, hooks.SyncMatchOutcomeNotMatched, event.details.SyncMatchOutcome)
		require.False(t, event.details.IsSyncMatch)
		keys = append(keys, hookKey{kind: event.kind, taskType: event.taskType})
	}
	require.ElementsMatch(t, []hookKey{
		{kind: enumspb.TASK_QUEUE_KIND_NORMAL, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		{kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		{kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		{kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	}, keys)
}

/*
TestSingleSessionBindingSurvivesRebuild tests that an exclusive binding is rebuilt from history:
 1. A single-session worker acquires the workflow and completes its first workflow task.
 2. The workflow is reset after the bind, reset to before the bind, or its mutable state is rebuilt from history.
 3. After a reset or rebuild past the bind, the next workflow task goes to the same worker's sticky queue. After a
    reset to before the bind, the workflow is unbound and its task goes to the normal queue.
*/
func TestSingleSessionBindingSurvivesRebuild(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// rebuild resets or rebuilds the workflow and returns the run that receives the next workflow task.
		rebuild    func(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) string
		wantSticky bool
	}{
		{name: "reset after the bind", wantSticky: true, rebuild: func(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) string {
			return resetWorkflowTo(t, env, execution, 4)
		}},
		{name: "reset before the bind", rebuild: func(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) string {
			return resetWorkflowTo(t, env, execution, 3)
		}},
		{name: "rebuild from history", wantSticky: true, rebuild: func(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) string {
			_, err := env.AdminClient().RebuildMutableState(t.Context(), &adminservice.RebuildMutableStateRequest{
				Namespace: env.Namespace().String(), Execution: execution,
			})
			require.NoError(t, err)
			_, err = env.FrontendClient().SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: env.Namespace().String(), WorkflowExecution: execution, SignalName: "wake", Identity: "client",
			})
			require.NoError(t, err)
			return execution.GetRunId()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
			)
			tv := env.Tv()
			client := env.FrontendClient()
			namespace := env.Namespace().String()
			started, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
				Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
				WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
				WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			})
			require.NoError(t, err)
			session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
				SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
			}}
			poller := taskpoller.New(t, client, namespace)
			sessionPoller := poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(),
			})
			// Events 1-4: workflow started, workflow task scheduled, started (the bind) and completed.
			_, err = sessionPoller.HandleTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			}, taskpoller.WithContext(t.Context()))
			require.NoError(t, err)
			requireStartedEventRecordsBinding(t, env, &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: started.GetRunId()})

			runID := tc.rebuild(t, env, &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: started.GetRunId()})

			nextPoller := sessionPoller
			if !tc.wantSticky {
				nextPoller = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: tv.TaskQueue()})
			}
			_, err = nextPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				require.Equal(t, runID, task.GetWorkflowExecution().GetRunId())
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			}, taskpoller.WithTimeout(10*time.Second))
			require.NoError(t, err)
		})
	}
}

/*
TestSingleSessionAcquisitionByUpdateTaskSurvivesRebuild tests that an acquisition by a speculative workflow task's
start is persisted, although such a start is otherwise kept in memory only:
 1. A worker without session options completes the first workflow task, so the workflow is unbound.
 2. An update schedules a speculative workflow task, which single-session worker S acquires. The acquisition is
    persisted at once.
 3. S accepts the update, and the workflow's mutable state is rebuilt from history.
 4. The next workflow task goes to S's sticky queue.
*/
func TestSingleSessionAcquisitionByUpdateTaskSurvivesRebuild(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv()
	client := env.FrontendClient()
	namespace := env.Namespace().String()
	started, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	execution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: started.GetRunId()}
	poller := taskpoller.New(t, client, namespace)
	_, err = poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: tv.TaskQueue()}).HandleTask(tv,
		func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)

	startUpdate(t, env, tv)
	sessionPoller := poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), WorkerInstanceKey: tv.WorkerInstanceKey(),
		SessionOptions: &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
		}},
	})
	_, err = sessionPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Len(t, task.GetMessages(), 1)
		protorequire.ProtoEqual(t, &sessionpb.SessionInfo{ExclusiveWorker: true, WorkerQueue: tv.StickyTaskQueue().GetName()},
			persistedBinding(t, env, execution))
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Messages: updateutils.New(t).UpdateAcceptMessages(tv, task.GetMessages()[0]),
		}, nil
	}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)
	requireStartedEventRecordsBinding(t, env, execution)

	_, err = env.AdminClient().RebuildMutableState(t.Context(), &adminservice.RebuildMutableStateRequest{
		Namespace: namespace, Execution: execution,
	})
	require.NoError(t, err)
	_, err = client.SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace, WorkflowExecution: execution, SignalName: "wake", Identity: "client",
	})
	require.NoError(t, err)
	_, err = sessionPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, started.GetRunId(), task.GetWorkflowExecution().GetRunId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)
}

/*
TestSingleSessionBindingSurvivesResetToAcquisition tests a reset to the workflow task that bound the worker:
 1. A single-session worker acquires the workflow from the normal queue and completes its first workflow task.
 2. The workflow is reset to that workflow task. The task was scheduled on the normal queue, so the binding can only
    come back from the worker queue recorded on its started event.
 3. The reset run's workflow task is delivered to the same worker's sticky queue.
*/
func TestSingleSessionBindingSurvivesResetToAcquisition(t *testing.T) {
	t.Parallel()
	e := newSingleSessionEnv(t, 10*time.Second, nil)

	resetRunID := resetWorkflowTo(t, e.env, &commonpb.WorkflowExecution{WorkflowId: e.tv.WorkflowID(), RunId: e.runID}, e.acquiredEventID)

	err := e.pollWorkflowTaskOnS(10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, resetRunID, task.GetWorkflowExecution().GetRunId(), "expected the reset run's task")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	require.NoError(t, err)
}

/*
TestSingleSessionInheritedByNextRun tests that an exclusive binding survives a new run of the workflow:
 1. A single-session worker acquires the workflow and closes the run with a continue-as-new, a failure under a retry
    policy, or a completion under a cron schedule.
 2. The next run's first workflow task is delivered to the same worker's sticky queue. Without the inherited binding it
    would go to the normal queue, where the single-session worker can no longer acquire work. A cron run does not
    inherit the binding, so its first workflow task goes to the normal queue.
*/
func TestSingleSessionInheritedByNextRun(t *testing.T) {
	t.Parallel()
	for _, tc := range nextRunCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
				testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
			)
			tv := env.Tv()
			client := env.FrontendClient()
			namespace := env.Namespace().String()
			started, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
				Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
				WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(), RetryPolicy: tc.retryPolicy, CronSchedule: tc.cronSchedule,
				WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			})
			require.NoError(t, err)
			session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
				SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
			}}
			poller := taskpoller.New(t, client, namespace).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(),
			})
			_, err = poller.HandleTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{tc.command(tv)}}, nil
			}, taskpoller.WithContext(t.Context()))
			require.NoError(t, err)

			if tc.unbound {
				poller = taskpoller.New(t, client, namespace).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: tv.TaskQueue()})
			}
			_, err = poller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				require.NotEqual(t, started.GetRunId(), task.GetWorkflowExecution().GetRunId(), "expected the next run's task")
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			}, taskpoller.WithTimeout(10*time.Second))
			require.NoError(t, err)
		})
	}
}

/*
TestSingleSessionInheritedBindingSurvivesRebuild tests that a continue-as-new run keeps its inherited exclusive binding
when its mutable state is rebuilt from history:
 1. A single-session worker acquires the workflow and continues it as new.
 2. The new run's started event records the inherited binding, and the new run's mutable state is rebuilt.
 3. The new run's first workflow task is delivered to the same worker's sticky queue.
*/
func TestSingleSessionInheritedBindingSurvivesRebuild(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv()
	client := env.FrontendClient()
	namespace := env.Namespace().String()
	_, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	stableIdentity := tv.StickyTaskQueue().GetName()
	session := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: stableIdentity},
	}}
	poller := taskpoller.New(t, client, namespace).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, WorkerInstanceKey: tv.WorkerInstanceKey(),
	})
	_, err = poller.HandleTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{continueAsNewCmd(tv.WorkflowType(), tv.TaskQueue())}}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	described, err := client.DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
	})
	require.NoError(t, err)
	newRun := described.GetWorkflowExecutionInfo().GetExecution()
	history := env.SdkClient().GetWorkflowHistory(t.Context(), newRun.GetWorkflowId(), newRun.GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	startedEvent, err := history.Next()
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &sessionpb.SessionInfo{ExclusiveWorker: true, WorkerQueue: stableIdentity},
		startedEvent.GetWorkflowExecutionStartedEventAttributes().GetInheritedSessionInfo())
	_, err = env.AdminClient().RebuildMutableState(t.Context(), &adminservice.RebuildMutableStateRequest{
		Namespace: namespace, Execution: newRun,
	})
	require.NoError(t, err)

	_, err = poller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, newRun.GetRunId(), task.GetWorkflowExecution().GetRunId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)
}

// singleSessionEnv is a workflow acquired by single-session worker S.
type singleSessionEnv struct {
	t       *testing.T
	env     *testcore.TestEnv
	tv      *testvars.TestVars
	poller  *taskpoller.TaskPoller
	session *sessionpb.WorkerSessionOptions
	// deploymentOptions are S's worker versioning options on its polls; nil for an unversioned S.
	deploymentOptions *deploymentpb.WorkerDeploymentOptions
	// runID is the started workflow's run, and acquiredEventID the completed event of the workflow task that bound S.
	runID           string
	acquiredEventID int64
}

// newSingleSessionEnv starts a workflow and lets single-session worker S acquire it with its first workflow task, which
// completes with the commands built by commands, if any.
func newSingleSessionEnv(t *testing.T, workflowTaskTimeout time.Duration, commands func(*testvars.TestVars) []*commandpb.Command) *singleSessionEnv {
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv()
	started, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(workflowTaskTimeout),
	})
	require.NoError(t, err)
	e := &singleSessionEnv{
		t: t, env: env, tv: tv, poller: taskpoller.New(t, env.FrontendClient(), env.Namespace().String()),
		session: &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
		}},
		runID: started.GetRunId(),
	}
	err = e.pollWorkflowTaskOnS(10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		e.acquiredEventID = task.GetStartedEventId() + 1
		response := &workflowservice.RespondWorkflowTaskCompletedRequest{}
		if commands != nil {
			response.Commands = commands(tv)
		}
		return response, nil
	})
	require.NoError(t, err)
	return e
}

// pollWorkflowTaskOnS polls S's sticky queue as S.
func (e *singleSessionEnv) pollWorkflowTaskOnS(
	timeout time.Duration,
	handler func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) error {
	_, err := e.poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: e.tv.StickyTaskQueue(), SessionOptions: e.session, WorkerInstanceKey: e.tv.WorkerInstanceKey(),
		DeploymentOptions: e.deploymentOptions,
	}).HandleTask(e.tv, handler, taskpoller.WithTimeout(timeout))
	return err
}

// requireNoWorkflowTaskOnNormalQueue requires that a worker without sessions polling the normal queue gets nothing.
func (e *singleSessionEnv) requireNoWorkflowTaskOnNormalQueue() {
	_, err := e.poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: e.tv.TaskQueue()}).HandleTask(e.tv,
		func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			require.FailNow(e.t, "a workflow bound to a single-session worker must not reach the normal queue")
			return nil, nil
		}, taskpoller.WithTimeout(stickyPollTimeout))
	require.Error(e.t, err)
}

func (e *singleSessionEnv) signal() {
	_, err := e.env.FrontendClient().SignalWorkflowExecution(e.t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: e.env.Namespace().String(), WorkflowExecution: e.tv.WorkflowExecution(), SignalName: "wake", Identity: "client",
	})
	require.NoError(e.t, err)
}

/*
TestSingleSessionWorkflowTaskFailureStaysOnWorker tests that a live single-session worker keeps its workflow after a
failed or timed-out workflow task:
 1. Single-session worker S acquires the workflow, and a signal schedules the next workflow task on S.
 2. S fails that workflow task, or starts it and never responds so it times out.
 3. The retry is delivered on S's sticky queue, never on the normal queue, and S completes it.
*/
func TestSingleSessionWorkflowTaskFailureStaysOnWorker(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// respond fails or abandons the workflow task.
		respond func(e *singleSessionEnv) (*workflowservice.RespondWorkflowTaskCompletedRequest, error)
		// abandoned is true when S's late failure is rejected because the task already timed out.
		abandoned bool
	}{
		{name: "failure", respond: func(*singleSessionEnv) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return nil, context.Canceled
		}},
		{name: "timeout", respond: func(e *singleSessionEnv) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			await.RequireTrue(e.t, func() bool {
				for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
					if event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType() == enumspb.TIMEOUT_TYPE_START_TO_CLOSE {
						return true
					}
				}
				return false
			}, 10*time.Second, 100*time.Millisecond)
			return nil, context.Canceled
		}, abandoned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSingleSessionEnv(t, 2*time.Second, nil)
			e.signal()
			err := e.pollWorkflowTaskOnS(10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return tc.respond(e)
			})
			if tc.abandoned {
				require.Error(t, err, "S's late failure is rejected because the task already timed out")
			} else {
				require.NoError(t, err, "S reports the workflow task as failed")
			}

			e.requireNoWorkflowTaskOnNormalQueue()
			err = e.pollWorkflowTaskOnS(10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				require.Greater(t, task.GetAttempt(), int32(1), "expected the retry of the failed workflow task")
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			})
			require.NoError(t, err)
		})
	}
}

/*
TestSingleSessionUpdateTaskStaysOnWorker tests updates on a workflow bound to a single-session worker:
 1. Single-session worker S acquires the workflow.
 2. An update creates a speculative workflow task, which is delivered on S's sticky queue, and S fails it.
 3. The retry is also delivered on S's sticky queue, never on the normal queue.
*/
func TestSingleSessionUpdateTaskStaysOnWorker(t *testing.T) {
	t.Parallel()
	e := newSingleSessionEnv(t, 10*time.Second, nil)
	startUpdate(t, e.env, e.tv)

	// S fails the update's workflow task, and then fails the retry too, which must also arrive on S's sticky queue.
	for range 2 {
		handled := false
		err := e.pollWorkflowTaskOnS(10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			handled = true
			return nil, context.Canceled
		})
		require.NoError(t, err)
		require.True(t, handled)
	}
	e.requireNoWorkflowTaskOnNormalQueue()
}

/*
TestSingleSessionActivityRetryStaysOnWorker tests that a session activity's retry stays with the single-session worker:
 1. Single-session worker S acquires the workflow and schedules a session activity, which is delivered on S's sticky
    activity queue.
 2. S fails the activity with a retryable error.
 3. The retry is delivered on S's sticky activity queue as attempt 2.
*/
func TestSingleSessionActivityRetryStaysOnWorker(t *testing.T) {
	t.Parallel()
	e := newSingleSessionEnv(t, 10*time.Second, func(tv *testvars.TestVars) []*commandpb.Command {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId: tv.ActivityID(), ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
				ScheduleToCloseTimeout: durationpb.New(time.Minute), StartToCloseTimeout: durationpb.New(10 * time.Second),
				RetryPolicy:    &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1},
				SessionOptions: &sessionpb.ActivitySessionOptions{Placement: &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}}},
			}},
		}}
	})
	activityPoller := e.poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: e.tv.StickyTaskQueue(), SessionOptions: e.session, WorkerInstanceKey: e.tv.WorkerInstanceKey(),
	})

	_, err := activityPoller.HandleTask(e.tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, int32(1), task.GetAttempt())
		return nil, temporal.NewApplicationError("retry", "retryable")
	}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)

	_, err = activityPoller.HandleTask(e.tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, e.tv.ActivityID(), task.GetActivityId())
		require.Equal(t, int32(2), task.GetAttempt(), "expected the retry of the session activity")
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)
}

// autoUpgradeOverride is an auto-upgrade versioning override. Unlike a pinned override, it needs no registered version.
func autoUpgradeOverride() *workflowpb.VersioningOverride {
	return &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true}}
}

// updateVersioningOverride sets the execution's versioning override through UpdateWorkflowExecutionOptions, or removes
// it when override is nil.
func updateVersioningOverride(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution, override *workflowpb.VersioningOverride) error {
	_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(t.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                env.Namespace().String(),
		WorkflowExecution:        execution,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: override},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	return err
}

// versioningOverrideUpdates are the two ways UpdateWorkflowExecutionOptions can touch the versioning override of an
// unversioned workflow bound to a session worker. Only setting an auto-upgrade override changes its effective
// versioning behavior, so only that is rejected; removing the override it does not have changes nothing.
func versioningOverrideUpdates() []struct {
	name     string
	override *workflowpb.VersioningOverride
	rejected bool
} {
	return []struct {
		name     string
		override *workflowpb.VersioningOverride
		rejected bool
	}{
		{name: "set", override: autoUpgradeOverride(), rejected: true},
		{name: "remove"},
	}
}

// requireVersioningOverrideUpdate requires err to be the rejection of an override that would change the version of a
// workflow bound to a session worker when rejected, or no error otherwise.
func requireVersioningOverrideUpdate(t *testing.T, err error, rejected bool) {
	if !rejected {
		require.NoError(t, err)
		return
	}
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)
	require.ErrorContains(t, err, "would change the version of a workflow bound to a worker servicing sessions")
}

func resetStickyTaskQueue(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) error {
	_, err := env.FrontendClient().ResetStickyTaskQueue(t.Context(), &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: env.Namespace().String(), Execution: execution,
	})
	return err
}

// newCurrentVersionEnv is a single-partition versioning test environment whose task queue's current version is tv's
// deployment version.
func newCurrentVersionEnv(t *testing.T, opts ...testcore.TestOption) *VersioningTestEnv {
	env := newVersioning3TestEnv(t, append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	}, opts...)...)
	tv := env.Tv()
	_, err := env.GetTestCluster().MatchingClient().SyncDeploymentUserData(t.Context(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId: env.NamespaceID().String(), TaskQueue: tv.TaskQueue().GetName(),
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		DeploymentName: tv.DeploymentSeries(),
		UpdateRoutingConfig: &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  tv.ExternalDeploymentVersion(),
			CurrentVersionChangedTime: timestamppb.Now(), RevisionNumber: 1,
		},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			tv.BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
		},
	})
	require.NoError(t, err)
	return env
}

// startWithVersioningOverride starts tv's workflow with the override built by override, or with none when it is nil.
func startWithVersioningOverride(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars, override func(*testvars.TestVars) *workflowpb.VersioningOverride) {
	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(30 * time.Second),
	}
	if override != nil {
		request.VersioningOverride = override(tv)
	}
	_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), request)
	require.NoError(t, err)
}

// newVersionedSingleSessionEnv starts a workflow with the override built by override (none when nil) on a task queue
// whose current version is that of S, a versioned single-session worker that has not acquired it yet.
func newVersionedSingleSessionEnv(t *testing.T, override func(*testvars.TestVars) *workflowpb.VersioningOverride) *singleSessionEnv {
	env := newCurrentVersionEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableSingleSessionWorkers, true))
	tv := env.Tv()
	startWithVersioningOverride(t, env.TestEnv, tv, override)
	return &singleSessionEnv{
		t: t, env: env.TestEnv, tv: tv, poller: taskpoller.New(t, env.FrontendClient(), env.Namespace().String()),
		session: &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
		}},
		deploymentOptions: tv.WorkerDeploymentOptions(true),
	}
}

// completeVersionedWorkflowTask completes the next workflow task on the normal queue as a versioned worker without
// session options.
func completeVersionedWorkflowTask(t *testing.T, poller *taskpoller.TaskPoller, tv *testvars.TestVars) {
	_, err := poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.TaskQueue(), DeploymentOptions: tv.WorkerDeploymentOptions(true),
	}).HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED, DeploymentOptions: tv.WorkerDeploymentOptions(true),
		}, nil
	}, taskpoller.WithTimeout(10*time.Second))
	require.NoError(t, err)
}

// requireWorkflowTaskFailedWith requires the last WorkflowTaskFailed event in the execution's history to have cause.
func requireWorkflowTaskFailedWith(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution, cause enumspb.WorkflowTaskFailedCause) {
	var failedCause enumspb.WorkflowTaskFailedCause
	for _, event := range env.GetHistory(env.Namespace().String(), execution) {
		if attributes := event.GetWorkflowTaskFailedEventAttributes(); attributes != nil {
			failedCause = attributes.GetCause()
		}
	}
	require.Equal(t, cause, failedCause)
}

// upgradeOnContinueAsNewCmd continues the workflow as new, asking its next run to upgrade to the current version.
func upgradeOnContinueAsNewCmd(tv *testvars.TestVars) *commandpb.Command {
	command := continueAsNewCmd(tv.WorkflowType(), tv.TaskQueue())
	command.GetContinueAsNewWorkflowExecutionCommandAttributes().InitialVersioningBehavior =
		enumspb.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE
	return command
}

/*
TestSingleSessionRejectsVersioningOverride tests that a workflow bound to a single-session worker cannot be given a
versioning override that changes its version:
 1. Single-session worker S acquires the unversioned workflow.
 2. UpdateWorkflowExecutionOptions setting an auto-upgrade override is rejected, since it changes the effective
    versioning behavior; removing the override the workflow does not have succeeds, since it changes nothing.
 3. The binding is unchanged: the next workflow task is delivered on S's sticky queue, never on the normal queue.
*/
func TestSingleSessionRejectsVersioningOverride(t *testing.T) {
	t.Parallel()
	for _, tc := range versioningOverrideUpdates() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSingleSessionEnv(t, 10*time.Second, nil)

			requireVersioningOverrideUpdate(t, updateVersioningOverride(t, e.env, e.tv.WorkflowExecution(), tc.override), tc.rejected)

			e.signal()
			e.requireNoWorkflowTaskOnNormalQueue()
			err := e.pollWorkflowTaskOnS(10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			})
			require.NoError(t, err)
		})
	}
}

/*
TestSingleSessionRejectsResetStickyTaskQueue tests that resetting the sticky queue does not release a single-session
binding:
 1. Single-session worker S acquires the workflow.
 2. ResetStickyTaskQueue is rejected with FailedPrecondition.
 3. The binding is unchanged: the next workflow task is delivered on S's sticky queue, never on the normal queue.
*/
func TestSingleSessionRejectsResetStickyTaskQueue(t *testing.T) {
	t.Parallel()
	e := newSingleSessionEnv(t, 10*time.Second, nil)

	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, resetStickyTaskQueue(t, e.env, e.tv.WorkflowExecution()), &failedPrecondition)

	e.signal()
	e.requireNoWorkflowTaskOnNormalQueue()
	err := e.pollWorkflowTaskOnS(10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	require.NoError(t, err)
}

/*
TestSingleSessionRejectsUpgradeOnContinueAsNew tests that a pinned workflow bound to a single-session worker cannot
upgrade on continue-as-new:
 1. Versioned single-session worker S acquires the workflow and completes its workflow task as pinned.
 2. S continues the workflow as new with an auto-upgrade initial versioning behavior: the workflow task fails with
    BAD_CONTINUE_AS_NEW_ATTRIBUTES.
 3. The retry is delivered on S, and a plain continue-as-new completes it.
*/
func TestSingleSessionRejectsUpgradeOnContinueAsNew(t *testing.T) {
	t.Parallel()
	e := newVersionedSingleSessionEnv(t, nil)
	complete := func(commands ...*commandpb.Command) error {
		return e.pollWorkflowTaskOnS(10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: commands, VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED, DeploymentOptions: e.deploymentOptions,
			}, nil
		})
	}
	require.NoError(t, complete())

	e.signal()
	err := complete(upgradeOnContinueAsNewCmd(e.tv))
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)
	requireWorkflowTaskFailedWith(t, e.env, e.tv.WorkflowExecution(), enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES)

	require.NoError(t, complete(continueAsNewCmd(e.tv.WorkflowType(), e.tv.TaskQueue())))
}

/*
TestSingleSessionPinnedOverrideOnBoundWorkflow tests pinned versioning overrides on a pinned workflow bound to a
versioned single-session worker:
 1. S, a versioned single-session worker on the task queue's current version, acquires the workflow and completes its
    workflow task as pinned.
 2. A pinned override to the workflow's own version, given explicitly or filled in from it, changes neither its
    version nor its behavior, so it is accepted. A pinned override to another version is rejected.
 3. The binding is unchanged: the next workflow task is delivered on S.
*/
func TestSingleSessionPinnedOverrideOnBoundWorkflow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		override func(*testvars.TestVars) *workflowpb.VersioningOverride
		rejected bool
	}{
		{name: "own version", override: (*testvars.TestVars).VersioningOverridePinned},
		{name: "own version filled in", override: func(*testvars.TestVars) *workflowpb.VersioningOverride {
			return &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED},
			}}
		}},
		{name: "another version", override: func(tv *testvars.TestVars) *workflowpb.VersioningOverride {
			return tv.WithBuildIDNumber(2).VersioningOverridePinned()
		}, rejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newVersionedSingleSessionEnv(t, nil)
			complete := func() error {
				return e.pollWorkflowTaskOnS(10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					return &workflowservice.RespondWorkflowTaskCompletedRequest{
						VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED, DeploymentOptions: e.deploymentOptions,
					}, nil
				})
			}
			require.NoError(t, complete())
			// Register another version in the task queue, so a pinned override to it passes version validation and only
			// the session rule can reject it.
			_, err := e.env.GetTestCluster().MatchingClient().SyncDeploymentUserData(t.Context(), &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId: e.env.NamespaceID().String(), TaskQueue: e.tv.TaskQueue().GetName(),
				TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
				DeploymentName: e.tv.DeploymentSeries(),
				UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
					e.tv.WithBuildIDNumber(2).BuildID(): {Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE},
				},
			})
			require.NoError(t, err)

			requireVersioningOverrideUpdate(t, updateVersioningOverride(t, e.env, e.tv.WorkflowExecution(), tc.override(e.tv)), tc.rejected)

			e.signal()
			e.requireNoWorkflowTaskOnNormalQueue()
			require.NoError(t, complete(), "the next workflow task is delivered on S")
		})
	}
}

/*
TestSingleSessionVersionedWorkerBehaviors tests which versioning behaviors a workflow bound to a versioned single-session
worker can run with:
 1. S, a versioned single-session worker on the task queue's current version, acquires the workflow.
 2. pinned: S completes the workflow task as pinned, and its next workflow task is also delivered on S.
 3. auto-upgrade: S completes as auto-upgrade, which fails the task with FEATURE_DISABLED. S is then redeployed as
    pinned, and the retry, still delivered on S, completes.
 4. auto-upgrade override: the workflow starts with an auto-upgrade versioning override, so S never acquires it: S's
    poll gets nothing, and a worker without session options completes its workflow task.
 5. pinned override: the workflow starts pinned to S's version, so S acquires it, S's completion passes even when S
    declares auto-upgrade, and the next workflow task is delivered on S.
*/
func TestSingleSessionVersionedWorkerBehaviors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// firstBehavior is the behavior S declares when completing the acquired workflow task.
		firstBehavior       enumspb.VersioningBehavior
		autoUpgradeOverride bool
		pinnedOverride      bool
		rejected            bool
	}{
		{name: "pinned", firstBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED},
		{name: "pinned override", firstBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, pinnedOverride: true},
		{name: "auto-upgrade", firstBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, rejected: true},
		{name: "auto-upgrade override", autoUpgradeOverride: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var override func(*testvars.TestVars) *workflowpb.VersioningOverride
			if tc.autoUpgradeOverride {
				override = func(*testvars.TestVars) *workflowpb.VersioningOverride { return autoUpgradeOverride() }
			}
			if tc.pinnedOverride {
				override = (*testvars.TestVars).VersioningOverridePinned
			}
			e := newVersionedSingleSessionEnv(t, override)
			tv := e.tv
			complete := func(behavior enumspb.VersioningBehavior) error {
				return e.pollWorkflowTaskOnS(10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
					return &workflowservice.RespondWorkflowTaskCompletedRequest{
						VersioningBehavior: behavior, DeploymentOptions: tv.WorkerDeploymentOptions(true),
					}, nil
				})
			}

			if tc.autoUpgradeOverride {
				err := e.pollWorkflowTaskOnS(stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					require.FailNow(t, "a workflow with an auto-upgrade override must not be acquired by a single-session worker")
					return nil, nil
				})
				require.Error(t, err)
				completeVersionedWorkflowTask(t, e.poller, tv)
				return
			}

			err := complete(tc.firstBehavior)
			if !tc.rejected {
				require.NoError(t, err)
				e.signal()
				require.NoError(t, complete(enumspb.VERSIONING_BEHAVIOR_PINNED), "the next workflow task is delivered on S")
				return
			}

			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
			requireWorkflowTaskFailedWith(t, e.env, tv.WorkflowExecution(), enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED)
			require.NoError(t, complete(enumspb.VERSIONING_BEHAVIOR_PINNED), "the retry is delivered on S and completes as pinned")
		})
	}
}
