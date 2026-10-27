package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSingleSessionAcquisitionAndReplacement(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableStickyActivityExecution, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	tv := env.Tv()
	namespace := env.Namespace().String()
	client := env.FrontendClient()
	poller := taskpoller.New(t, client, namespace)
	session := &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
	}}
	_, err := client.StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)

	// With SessionOptions.StableIdentity set to the sticky task queue name, the worker will only poll the sticky task queue with poll forwarding to
	// normal queue to only happen for the very first workflow task
	stickyPoller := poller.PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session,
	})
	_, err = stickyPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId: tv.ActivityID(), ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				SessionOptions:      &sessionpb.ActivitySessionOptions{Session: &sessionpb.ActivitySessionOptions_UseWorkflowSession{UseWorkflowSession: true}},
			}},
		}}}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)

	_, err = poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session,
	}).HandleTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		require.Equal(t, tv.ActivityID(), task.GetActivityId())
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
	_, err = stickyPoller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
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

	matching := env.GetTestCluster().MatchingClient()
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
			TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, Identity: tv.WorkerIdentity(),
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
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, Identity: tv.WorkerIdentity() + "-replacement",
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
		SessionOptions: &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: freshQueue.GetName()},
		}},
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

func TestSingleSessionConcurrentAcquisition(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, time.Second),
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

	session := &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
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
					TaskQueue: tv.StickyTaskQueue(), SessionOptions: session, Identity: tv.WorkerIdentity(),
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
		SessionOptions: &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
			SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: freshQueue.GetName()},
		}},
	}).HandleTask(tv.WithWorkflowID(expectedOther), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, expectedOther, task.GetWorkflowExecution().GetWorkflowId(), "the unrelated execution remains available to S2")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
}

func TestSingleSessionWorkflowBacklogWithoutPollers(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
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
	session := &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tv.StickyTaskQueue().GetName()},
	}}
	poller := taskpoller.New(t, client, namespace).PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tv.StickyTaskQueue(), SessionOptions: session,
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

	// After unloading the sticky task queue partition, the newly scheduled workflow task shall be spooled by loading the task queue partition again.
	// This block also checks that repeated timeouts keep replacement tasks on the same task queue
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
		require.GreaterOrEqual(t, timeoutCount, 3, "worker remains absent through multiple five-second retry cycles")
		response, err := matching.DescribeTaskQueuePartition(t.Context(), &matchingservice.DescribeTaskQueuePartitionRequest{
			NamespaceId: env.NamespaceID().String(), TaskQueuePartition: partition,
			Versions:    &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
			ReportStats: true, ReportPollers: true, OnlyIfLoaded: true,
		})
		require.NoError(t, err)
		physical := response.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo()
		require.Empty(t, physical.GetPollers())
		require.Positive(t, physical.GetTaskQueueStats().GetApproximateBacklogCount())
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
	_, err = poller.HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
		require.Equal(t, int32(1), task.GetAttempt(), "schedule-to-start expiry does not increase the attempt")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}, taskpoller.WithContext(t.Context()))
	require.NoError(t, err)
}
