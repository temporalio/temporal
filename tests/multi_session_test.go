package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workerpb "go.temporal.io/api/worker/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// sessionMoveEnv drives a workflow through raw pollers that act as session workers. A worker "dies" by no
// longer polling its sticky queue; Matching then treats it as unavailable, and History unbinds the workflow when it
// pushes a task there.
type sessionMoveEnv struct {
	t      *testing.T
	env    *testcore.TestEnv
	tv     *testvars.TestVars
	poller *taskpoller.TaskPoller
	// firstRunID is the run started by newSessionMoveEnv.
	firstRunID string
}

func newSessionMoveEnv(t *testing.T, workflowTaskTimeout time.Duration, opts ...testcore.TestOption) *sessionMoveEnv {
	return newSessionMoveEnvForNextRun(t, workflowTaskTimeout, nextRunCase{}, opts...)
}

// newSessionMoveEnvForNextRun is newSessionMoveEnv for a workflow started with the retry policy or cron schedule that
// nextRun needs.
func newSessionMoveEnvForNextRun(t *testing.T, workflowTaskTimeout time.Duration, nextRun nextRunCase, opts ...testcore.TestOption) *sessionMoveEnv {
	env := testcore.NewEnv(t, append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableMultiSessionWorkers, true),
	}, opts...)...)
	tv := env.Tv()
	started, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(5 * time.Minute), WorkflowTaskTimeout: durationpb.New(workflowTaskTimeout),
		RetryPolicy: nextRun.retryPolicy, CronSchedule: nextRun.cronSchedule,
	})
	require.NoError(t, err)
	return &sessionMoveEnv{
		t: t, env: env, tv: tv, poller: taskpoller.New(t, env.FrontendClient(), env.Namespace().String()), firstRunID: started.GetRunId(),
	}
}

func (e *sessionMoveEnv) stickyQueue(worker string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{Name: e.tv.TaskQueue().GetName() + "-sticky-" + worker, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: e.tv.TaskQueue().GetName()}
}

// instanceKey is the worker process's instance key, which Matching uses to look up its heartbeats. A multi-session
// worker's sticky queue is named after it.
func (e *sessionMoveEnv) instanceKey(worker string) string {
	return e.stickyQueue(worker).GetName()
}

func (e *sessionMoveEnv) stickyAttributes(worker string, scheduleToStart time.Duration) *taskqueuepb.StickyExecutionAttributes {
	return &taskqueuepb.StickyExecutionAttributes{WorkerTaskQueue: e.stickyQueue(worker), ScheduleToStartTimeout: durationpb.New(scheduleToStart)}
}

// pollWorkflowTask polls as a session worker (worker != "") or as a worker without session options.
func (e *sessionMoveEnv) pollWorkflowTask(
	worker string,
	queue *taskqueuepb.TaskQueue,
	timeout time.Duration,
	handler func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) error {
	request := &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: queue, Identity: "worker-" + worker}
	if worker != "" {
		request.SessionOptions = multiSessionWorkerOptions()
		request.WorkerInstanceKey = e.instanceKey(worker)
	}
	_, err := e.poller.PollWorkflowTask(request).HandleTask(e.tv, handler, taskpoller.WithTimeout(timeout))
	return err
}

// stickyPollTimeout is just above the frontend's 2s minimum long-poll timeout, below which a poll is rejected
// before reaching Matching. Short polls leave more room inside Matching's 10s sticky poller window, which is
// what keeps a registered worker "alive" in these tests.
const stickyPollTimeout = 2500 * time.Millisecond

// registerStickyPoller makes a worker look alive to Matching by polling its sticky queue once.
func (e *sessionMoveEnv) registerStickyPoller(worker string) {
	err := e.pollWorkflowTask(worker, e.stickyQueue(worker), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.FailNow(e.t, "no workflow task expected while registering a sticky poller")
		return nil, nil
	})
	require.Error(e.t, err)
}

// registerStickyActivityPoller makes a worker look alive on its sticky activity queue, as a real worker that polls
// it from startup would. Without it, Matching treats the worker as gone for activities, and pushing a session
// activity there unbinds the workflow.
func (e *sessionMoveEnv) registerStickyActivityPoller(worker string) {
	_, err := e.poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: e.stickyQueue(worker), Identity: "worker-" + worker, SessionOptions: multiSessionWorkerOptions(),
		WorkerInstanceKey: e.instanceKey(worker),
	}).HandleTask(e.tv, func(*workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.FailNow(e.t, "no activity task expected while registering a sticky activity poller")
		return nil, nil
	}, taskpoller.WithTimeout(stickyPollTimeout))
	require.Error(e.t, err)
}

// activityPollRequest polls queue as a session worker (worker != "") or as a worker without session options.
func (e *sessionMoveEnv) activityPollRequest(worker string, queue *taskqueuepb.TaskQueue) *workflowservice.PollActivityTaskQueueRequest {
	request := &workflowservice.PollActivityTaskQueueRequest{TaskQueue: queue, Identity: "worker-" + worker}
	if worker != "" {
		request.SessionOptions = multiSessionWorkerOptions()
		request.WorkerInstanceKey = e.instanceKey(worker)
	}
	return request
}

func (e *sessionMoveEnv) pollActivityTask(
	worker string,
	queue *taskqueuepb.TaskQueue,
	timeout time.Duration,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	var task *workflowservice.PollActivityTaskQueueResponse
	_, err := e.poller.PollActivityTask(e.activityPollRequest(worker, queue)).HandleTask(e.tv,
		func(t *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			task = t
			return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
		}, taskpoller.WithTimeout(timeout))
	return task, err
}

// completeActivityAs runs the session activity from queue as a session worker (worker != "") or as a worker without
// session options, and returns its task.
func (e *sessionMoveEnv) completeActivityAs(worker string, queue *taskqueuepb.TaskQueue) *workflowservice.PollActivityTaskQueueResponse {
	return e.runActivityAs(worker, queue, func() (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	})
}

// failActivityAs is completeActivityAs with a retryable failure, so the activity backs off and retries.
func (e *sessionMoveEnv) failActivityAs(worker string, queue *taskqueuepb.TaskQueue) *workflowservice.PollActivityTaskQueueResponse {
	return e.runActivityAs(worker, queue, func() (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return nil, temporal.NewApplicationError("retry", "retryable")
	})
}

func (e *sessionMoveEnv) runActivityAs(
	worker string,
	queue *taskqueuepb.TaskQueue,
	respond func() (*workflowservice.RespondActivityTaskCompletedRequest, error),
) *workflowservice.PollActivityTaskQueueResponse {
	var task *workflowservice.PollActivityTaskQueueResponse
	_, err := e.poller.PollActivityTask(e.activityPollRequest(worker, queue)).HandleTask(e.tv,
		func(t *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			task = t
			return respond()
		}, taskpoller.WithTimeout(30*time.Second))
	require.NoError(e.t, err)
	return task
}

func (e *sessionMoveEnv) signal() {
	_, err := e.env.FrontendClient().SignalWorkflowExecution(e.t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: e.env.Namespace().String(), WorkflowExecution: e.tv.WorkflowExecution(), SignalName: "wake", Identity: "client",
	})
	require.NoError(e.t, err)
}

func (e *sessionMoveEnv) scheduleSessionActivity(id string) *commandpb.Command {
	command := e.scheduleActivity(id)
	command.GetScheduleActivityTaskCommandAttributes().SessionOptions = &sessionpb.ActivitySessionOptions{
		Placement: &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}},
	}
	return command
}

// scheduleActivity schedules an activity that is not part of the workflow session.
func (e *sessionMoveEnv) scheduleActivity(id string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId: id, ActivityType: e.tv.ActivityType(), TaskQueue: e.tv.TaskQueue(),
			ScheduleToCloseTimeout: durationpb.New(2 * time.Minute), StartToCloseTimeout: durationpb.New(time.Minute),
		}},
	}
}

// bindToA starts the session on worker A from the normal queue and schedules one session activity. Unless A polled its
// sticky activity queue first, Matching considers it unavailable for the activity, so History unbinds the workflow:
// the activity waits for the next binding, and a workflow task is scheduled on the normal queue.
func (e *sessionMoveEnv) bindToA(stickyScheduleToStart time.Duration) {
	e.bindToAWith(stickyScheduleToStart, e.scheduleSessionActivity("session-activity"))
}

// bindToAWith is bindToA with the given commands instead of the single session activity.
func (e *sessionMoveEnv) bindToAWith(stickyScheduleToStart time.Duration, commands ...*commandpb.Command) {
	err := e.pollWorkflowTask("A", e.tv.TaskQueue(), 30*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: commands, StickyAttributes: e.stickyAttributes("A", stickyScheduleToStart),
		}, nil
	})
	require.NoError(e.t, err)
}

// completeOn completes a workflow task as worker with commands, and checks the latest started event records the binding
// mutable state holds.
func (e *sessionMoveEnv) completeOn(worker string, queue *taskqueuepb.TaskQueue, timeout time.Duration, commands ...*commandpb.Command) {
	err := e.pollWorkflowTask(worker, queue, timeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		response := &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}
		if worker != "" {
			response.StickyAttributes = e.stickyAttributes(worker, time.Second)
		}
		return response, nil
	})
	require.NoError(e.t, err)
	requireStartedEventRecordsBinding(e.t, e.env, e.tv.WorkflowExecution())
}

func (e *sessionMoveEnv) requireSessionActivityOn(queue *taskqueuepb.TaskQueue) {
	task, err := e.pollActivityTask("", queue, 30*time.Second)
	require.NoError(e.t, err)
	require.Equal(e.t, "session-activity", task.GetActivityId())
}

// requireNextRunOn requires the next run's first workflow task on queue, started by worker ("" for a worker without
// session options).
func (e *sessionMoveEnv) requireNextRunOn(worker string, queue *taskqueuepb.TaskQueue) {
	err := e.pollWorkflowTask(worker, queue, 10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.NotEqual(e.t, e.firstRunID, task.GetWorkflowExecution().GetRunId(), "expected the next run's task")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	require.NoError(e.t, err)
}

// awaitQueuedOn waits until a task is queued on worker's sticky queue of taskType.
func (e *sessionMoveEnv) awaitQueuedOn(worker string, taskType enumspb.TaskQueueType) {
	await.Require(e.t.Context(), e.t, func(t *await.T) {
		response, err := e.env.FrontendClient().DescribeTaskQueue(t.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: e.env.Namespace().String(), TaskQueue: e.stickyQueue(worker), TaskQueueType: taskType, ReportStats: true,
		})
		require.NoError(t, err)
		require.Positive(t, response.GetStats().GetApproximateBacklogCount())
	}, 10*time.Second, 100*time.Millisecond)
}

// awaitUnbound waits until the workflow is not bound to any worker. It reads the cached mutable state: a forced reload
// would clear the workflow's in-memory update registry and abort its in-flight updates.
func (e *sessionMoveEnv) awaitUnbound() {
	await.Require(e.t.Context(), e.t, func(t *await.T) {
		response, err := e.env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: e.env.Namespace().String(), Execution: e.tv.WorkflowExecution(), SkipForceReload: true,
		})
		require.NoError(t, err)
		require.Nil(t, response.GetDatabaseMutableState().GetExecutionInfo().GetSessionState())
	}, 30*time.Second, 100*time.Millisecond)
}

// shutDownWorker shuts worker down as an SDK does, which drains its queues.
func (e *sessionMoveEnv) shutDownWorker(worker string) {
	_, err := e.env.FrontendClient().ShutdownWorker(e.t.Context(), &workflowservice.ShutdownWorkerRequest{
		Namespace: e.env.Namespace().String(), StickyTaskQueue: e.stickyQueue(worker).GetName(), Identity: "worker-" + worker,
		WorkerInstanceKey: e.instanceKey(worker), TaskQueue: e.tv.TaskQueue().GetName(),
		WorkerHeartbeat: &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: e.instanceKey(worker), TaskQueue: e.tv.TaskQueue().GetName(), Status: enumspb.WORKER_STATUS_SHUTDOWN,
			SessionOptions: multiSessionWorkerOptions(),
		},
	})
	require.NoError(e.t, err)
}

// recordHeartbeat records worker's last heartbeat as a multi-session worker. With no heartbeat after it, Matching
// evicts the worker once the registry TTL passes, which drains its queues.
func (e *sessionMoveEnv) recordHeartbeat(worker string) {
	_, err := e.env.FrontendClient().RecordWorkerHeartbeat(e.t.Context(), &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: e.env.Namespace().String(), Identity: "worker-" + worker,
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{{
			WorkerInstanceKey: e.instanceKey(worker), TaskQueue: e.tv.TaskQueue().GetName(), Status: enumspb.WORKER_STATUS_RUNNING,
			SessionOptions: multiSessionWorkerOptions(),
		}},
	})
	require.NoError(e.t, err)
}

// requireCompletionRejected requires a worker without session options to start a workflow task from the normal queue
// and have its completion rejected, because session activities wait for a session worker to bind the workflow. It must
// be the workflow's first such rejection: later attempts are transient and add no event to history.
func (e *sessionMoveEnv) requireCompletionRejected() {
	err := e.pollWorkflowTask("", e.tv.TaskQueue(), 30*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(e.t, err, &invalidArgument)
	rejections := 0
	for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
		if event.GetWorkflowTaskFailedEventAttributes().GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_SESSION_WORKER_REQUIRED {
			rejections++
		}
	}
	require.Equal(e.t, 1, rejections)
}

// requireNoActivityOn requires that worker ("" for a worker without session options) polling queue gets nothing.
func (e *sessionMoveEnv) requireNoActivityOn(worker string, queue *taskqueuepb.TaskQueue) {
	task, err := e.pollActivityTask(worker, queue, 3*time.Second)
	require.Error(e.t, err)
	require.Nil(e.t, task)
}

/*
TestWorkflowSessionMovesWithWorkflowTask tests the main move path:
 1. Worker A starts the workflow, binds the session, and schedules a session activity; A is dead, so the workflow
    is unbound and the activity waits.
 2. The next workflow task goes to the normal queue and worker B starts it.
 3. The workflow binds to B and the pending session activity is reissued onto B's sticky queue.
*/
func TestWorkflowSessionMovesWithWorkflowTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)

	e.signal()
	e.registerStickyActivityPoller("B")
	e.completeOn("B", e.tv.TaskQueue(), 30*time.Second)

	e.requireSessionActivityOn(e.stickyQueue("B"))
}

/*
TestWorkflowSessionRebindsToSameWorker tests a worker that looked dead starting its own workflow's task again:
 1. Worker A starts the workflow, binds the session, and schedules a session activity, which is queued on A's sticky
    activity queue because A polls it.
 2. A never polls its sticky workflow queue, so pushing the next workflow task there unbinds the workflow: the task
    goes to the normal queue, and the session activity's queued copy is invalidated.
 3. A starts the task from the normal queue, which binds the workflow to A again like any session worker, and reissues
    the session activity with a new stamp.
 4. The next workflow task goes to A's sticky queue, and the session activity is delivered there, never on the normal
    queue.
*/
func TestWorkflowSessionRebindsToSameWorker(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.registerStickyActivityPoller("A")
	e.bindToA(time.Second)
	// An unbind or a bind bumps the activity's stamp, which obsoletes the task queued for A.
	activityStamp := func() int32 {
		response, err := e.env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: e.env.Namespace().String(), Execution: e.tv.WorkflowExecution(),
		})
		require.NoError(t, err)
		activities := response.GetDatabaseMutableState().GetActivityInfos()
		require.Len(t, activities, 1)
		for _, activity := range activities {
			return activity.GetStamp()
		}
		return 0
	}
	stamp := activityStamp()

	e.signal()
	e.awaitUnbound()
	e.completeOn("A", e.tv.TaskQueue(), 30*time.Second)
	require.NotEqual(t, stamp, activityStamp())

	e.registerStickyPoller("A")
	e.signal()
	e.completeOn("A", e.stickyQueue("A"), 5*time.Second)
	e.requireNoActivityOn("", e.tv.TaskQueue())
	e.requireSessionActivityOn(e.stickyQueue("A"))
}

/*
TestWorkflowSessionStaysOnBusyWorker tests hard stickiness while the worker is alive:
 1. Worker A binds the session and polls its sticky queues once, so Matching sees it as alive for workflow tasks and
    for the session activity it schedules.
 2. A workflow task waits in A's backlog while B polls the normal queue.
 3. The task never times out to the normal queue: B gets nothing and A completes it.
*/
func TestWorkflowSessionStaysOnBusyWorker(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.registerStickyActivityPoller("A")
	e.bindToA(time.Second)
	e.registerStickyPoller("A")

	e.signal()
	err := e.pollWorkflowTask("B", e.tv.TaskQueue(), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.FailNow(t, "a live session worker's task must not reach another worker")
		return nil, nil
	})
	require.Error(t, err)
	e.completeOn("A", e.stickyQueue("A"), 5*time.Second)

	timedOut := false
	for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
		if attrs := event.GetWorkflowTaskTimedOutEventAttributes(); attrs != nil && attrs.GetTimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
			timedOut = true
		}
	}
	require.False(t, timedOut, "a bound workflow task has no schedule-to-start timeout")
}

/*
TestWorkflowSessionStaysOnHeartbeatingWorker tests a busy worker kept alive by its heartbeats alone:
 1. Worker A binds the session from the normal queue and never polls its sticky queue, so Matching has no recent
    poller for it, as for a worker busy for longer than the sticky poller window.
 2. A records a heartbeat, so the worker heartbeat registry still knows it.
 3. The next workflow task is queued for A instead of falling back: B on the normal queue gets nothing, and A gets
    the task on its sticky queue. Without the heartbeat, the task would go to B (TestWorkflowSessionMovesWithWorkflowTask).
*/
func TestWorkflowSessionStaysOnHeartbeatingWorker(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(time.Second)
	e.recordHeartbeat("A")

	e.signal()
	err := e.pollWorkflowTask("B", e.tv.TaskQueue(), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.FailNow(t, "a heartbeating session worker's task must not reach another worker")
		return nil, nil
	})
	require.Error(t, err)
	e.completeOn("A", e.stickyQueue("A"), 5*time.Second)
}

/*
TestWorkflowSessionDrainHoldsQueuedActivity tests draining a dead worker's queued session activity:
 1. Worker A is alive on its sticky activity queue when it binds the session, so the session activity is queued there.
 2. A dies with the activity still queued; nothing else would ever move it.
 3. A shuts down, or its heartbeats stop until Matching evicts it, which drains its queues: the workflow is unbound,
    the activity waits for the next binding and is never delivered on the normal queue, and a workflow task is
    scheduled on the normal queue.
 4. A worker without session options starts that workflow task, but its completion is rejected, and it never receives
    the activity; or session worker B starts it, binding the workflow to B, and the activity is delivered on B's
    sticky queue.
*/
func TestWorkflowSessionDrainHoldsQueuedActivity(t *testing.T) {
	t.Parallel()
	for _, tc := range sessionWorkerStopCases() {
		for _, pickup := range drainPickupCases() {
			t.Run(tc.name+"/"+pickup.name, func(t *testing.T) {
				t.Parallel()
				e := newSessionMoveEnv(t, 10*time.Second, tc.envOptions...)
				e.registerStickyActivityPoller("A")
				e.bindToA(time.Second)
				e.requireNoActivityOn("", e.tv.TaskQueue())

				tc.stop(e, "A")
				e.awaitUnbound()
				e.requireNoActivityOn("", e.tv.TaskQueue())

				if pickup.worker == "" {
					e.requireCompletionRejected()
					e.requireNoActivityOn("", e.tv.TaskQueue())
					return
				}
				e.registerStickyActivityPoller(pickup.worker)
				e.completeOn(pickup.worker, e.tv.TaskQueue(), 30*time.Second)
				e.requireSessionActivityOn(e.stickyQueue(pickup.worker))
			})
		}
	}
}

/*
TestWorkflowSessionDrainMovesQueuedWorkflowTask tests draining a dead worker's queued workflow task:
 1. Worker A binds the session and is alive on its sticky queue, so the next workflow task is queued there.
 2. A dies with the task queued; a session workflow task has no schedule-to-start timeout to move it.
 3. A shuts down, or its heartbeats stop until Matching evicts it, which drains its queues: the workflow is unbound and
    the task is delivered on the normal queue.
 4. A worker without session options completes it; or session worker B completes it, binding the workflow to B, and
    schedules a session activity, which is delivered on B's sticky queue.
*/
func TestWorkflowSessionDrainMovesQueuedWorkflowTask(t *testing.T) {
	t.Parallel()
	for _, tc := range sessionWorkerStopCases() {
		for _, pickup := range drainPickupCases() {
			t.Run(tc.name+"/"+pickup.name, func(t *testing.T) {
				t.Parallel()
				e := newSessionMoveEnv(t, 10*time.Second, tc.envOptions...)
				e.bindToAWith(time.Second)
				e.registerStickyPoller("A")
				e.signal()
				e.awaitQueuedOn("A", enumspb.TASK_QUEUE_TYPE_WORKFLOW)

				tc.stop(e, "A")

				if pickup.worker == "" {
					e.completeOn("", e.tv.TaskQueue(), 30*time.Second)
					return
				}
				e.registerStickyActivityPoller(pickup.worker)
				e.completeOn(pickup.worker, e.tv.TaskQueue(), 30*time.Second, e.scheduleSessionActivity("session-activity"))
				e.requireSessionActivityOn(e.stickyQueue(pickup.worker))
			})
		}
	}
}

// drainPickupCase is a worker that takes a drained task from the normal queue.
type drainPickupCase struct {
	name string
	// worker is the session worker that takes the task, or "" for a worker without session options.
	worker string
}

func drainPickupCases() []drainPickupCase {
	return []drainPickupCase{
		{name: "worker without session options"},
		{name: "session worker", worker: "B"},
	}
}

// sessionWorkerStopCase is one way a multi-session worker stops, after which Matching drains its queues.
type sessionWorkerStopCase struct {
	name string
	// envOptions configure the test cluster this way of stopping needs.
	envOptions []testcore.TestOption
	stop       func(e *sessionMoveEnv, worker string)
}

func sessionWorkerStopCases() []sessionWorkerStopCase {
	return []sessionWorkerStopCase{
		{name: "shutdown", stop: (*sessionMoveEnv).shutDownWorker},
		{
			name: "heartbeat expiry",
			// The registry's eviction loop reads its interval only between runs, so these must be set when Matching
			// starts, on a dedicated cluster.
			envOptions: []testcore.TestOption{
				testcore.WithDynamicConfig(dynamicconfig.MatchingWorkerRegistryEntryTTL, time.Second),
				testcore.WithDynamicConfig(dynamicconfig.MatchingWorkerRegistryEvictionInterval, time.Second),
			},
			stop: (*sessionMoveEnv).recordHeartbeat,
		},
	}
}

/*
TestWorkflowSessionRecoversWhenNewWorkerCrashesMidTask tests the window between a bind and its completion:
 1. A is dead, so the workflow is unbound. B polls its sticky queues once, starts the next workflow task from the
    normal queue, which binds the workflow to B, and crashes while running the task.
 2. The retry goes to B's sticky queue, because the bind already set stickiness, and waits there with no
    schedule-to-start timeout.
 3. B's shutdown drains its queue, unbinding the workflow, and worker C takes over the session from the normal queue.
*/
func TestWorkflowSessionRecoversWhenNewWorkerCrashesMidTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 2*time.Second)
	e.bindToA(time.Second)

	// B is alive to Matching before it takes over, so the retry and the reissued session activity land in its backlogs
	// rather than unbinding the workflow.
	e.registerStickyActivityPoller("B")
	e.registerStickyPoller("B")
	e.signal()
	err := e.pollWorkflowTask("B", e.tv.TaskQueue(), 30*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		// B crashes: it never responds before the task's start-to-close timeout fires.
		await.RequireTrue(t, func() bool {
			for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
				if event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType() == enumspb.TIMEOUT_TYPE_START_TO_CLOSE {
					return true
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond)
		return nil, context.Canceled
	})
	require.Error(t, err, "B's late failure response is rejected because its task already timed out")

	e.awaitQueuedOn("B", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	e.shutDownWorker("B")
	e.registerStickyActivityPoller("C")
	e.completeOn("C", e.tv.TaskQueue(), time.Minute)
	e.requireSessionActivityOn(e.stickyQueue("C"))

	for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
		require.NotEqual(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType(),
			"the retry waits on B's sticky queue without timing out until the queue is drained")
	}
}

/*
TestWorkflowSessionRejectsCompletionWithoutSessionsWhileUnbound tests that only a session worker can take over an
unbound workflow with waiting session activities:
 1. A binds the session and dies with a session activity pending, so the workflow is unbound and a workflow task is
    scheduled on the normal queue.
 2. Worker Z without session options starts that workflow task, but its completion is rejected: the workflow task
    fails with SESSION_WORKER_REQUIRED. Z's completion of the retry is rejected too, and adds no failure to
    history, since the retry is transient. Z never receives the session activity.
 3. Session worker Y starts the retried workflow task, binding the workflow to Y; its completion is accepted, and the
    activity is delivered on Y's sticky queue.
*/
func TestWorkflowSessionRejectsCompletionWithoutSessionsWhileUnbound(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)
	e.awaitUnbound()

	e.requireCompletionRejected()
	// The retry is transient: its rejection adds no second WorkflowTaskFailed event.
	e.requireCompletionRejected()
	e.requireNoActivityOn("", e.tv.TaskQueue())

	e.registerStickyActivityPoller("Y")
	e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
	e.requireSessionActivityOn(e.stickyQueue("Y"))
}

/*
TestWorkflowSessionUnboundWorkflowRunsOnWorkerWithoutSessions tests that an unbound workflow with no session work
behaves like any other workflow:
 1. Worker A binds the session without scheduling session activities, and dies.
 2. Pushing an update's workflow task to A's sticky queue unbinds the workflow, and the task goes to the normal queue.
 3. Worker Z without session options accepts the update, and completes the next workflow task from the normal queue.

A worker without sessions starting a bound workflow's task, which also unbinds it, cannot be reached here: every task
reaches the normal queue only after an unbind. TestUpdateNonExclusiveWorkerBinding covers it.
*/
func TestWorkflowSessionUnboundWorkflowRunsOnWorkerWithoutSessions(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(time.Second)

	startUpdate(t, e.env, e.tv)
	e.awaitUnbound()
	err := e.pollWorkflowTask("", e.tv.TaskQueue(), 30*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Len(t, task.GetMessages(), 1)
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Messages: updateutils.New(t).UpdateAcceptMessages(e.tv, task.GetMessages()[0]),
		}, nil
	})
	require.NoError(t, err)

	e.signal()
	e.completeOn("", e.tv.TaskQueue(), 30*time.Second)
}

/*
TestWorkflowSessionUpdateUnbindsFromDeadWorker tests an update on a workflow whose multi-session worker is dead:
 1. Worker A binds the session and dies: it never polls its sticky queue and records no heartbeat.
 2. An update's speculative workflow task is pushed to A's sticky queue; Matching reports A unavailable, so History
    unbinds the workflow right away, which persists the task as a normal workflow task, and sends it to the normal
    queue.
 3. Session worker Y starts it, binding the workflow to Y, and accepts and completes the update, or rejects it.
 4. The caller gets Y's result. The workflow task stays in history even when the update is rejected, because the unbind
    persisted it.
*/
func TestWorkflowSessionUpdateUnbindsFromDeadWorker(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		rejected bool
	}{
		{name: "accepted"},
		{name: "rejected", rejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSessionMoveEnv(t, 10*time.Second)
			e.bindToAWith(time.Second)

			type updateResult struct {
				response *workflowservice.UpdateWorkflowExecutionResponse
				err      error
			}
			results := make(chan updateResult, 1)
			go func() {
				response, err := e.env.FrontendClient().UpdateWorkflowExecution(t.Context(), &workflowservice.UpdateWorkflowExecutionRequest{
					Namespace: e.env.Namespace().String(), WorkflowExecution: e.tv.WorkflowExecution(),
					Request:    &updatepb.Request{Meta: &updatepb.Meta{UpdateId: e.tv.UpdateID()}, Input: &updatepb.Input{Name: "update"}},
					WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
				})
				results <- updateResult{response: response, err: err}
			}()
			e.awaitUnbound()

			err := e.pollWorkflowTask("Y", e.tv.TaskQueue(), 30*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				require.Len(t, task.GetMessages(), 1)
				messages := updateutils.New(t).UpdateAcceptCompleteMessages(e.tv, task.GetMessages()[0])
				if tc.rejected {
					messages = updateutils.New(t).UpdateRejectMessages(e.tv, task.GetMessages()[0])
				}
				return &workflowservice.RespondWorkflowTaskCompletedRequest{Messages: messages, StickyAttributes: e.stickyAttributes("Y", time.Second)}, nil
			})
			require.NoError(t, err)

			result := await.Rcv(t, results)
			require.NoError(t, result.err)
			outcome := result.response.GetOutcome()
			if tc.rejected {
				require.Equal(t, "rejection-of-"+e.tv.UpdateID(), outcome.GetFailure().GetMessage())
			} else {
				protorequire.ProtoEqual(t, payloads.EncodeString("success-result-of-"+e.tv.UpdateID()), outcome.GetSuccess())
			}
			protorequire.ProtoEqual(t, &sessionpb.SessionInfo{WorkerQueue: e.instanceKey("Y")}, persistedBinding(t, e.env, e.tv.WorkflowExecution()))
			requireStartedEventRecordsBinding(t, e.env, e.tv.WorkflowExecution())
			completed := 0
			for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
					completed++
				}
			}
			require.Equal(t, 2, completed, "A's workflow task and Y's update task")
		})
	}
}

/*
TestWorkflowSessionKeepsWorkerAfterFailedUpdateTask tests a failed speculative workflow task on a live worker:
 1. Worker A binds the session and polls its sticky queue, so it is alive.
 2. An update creates a speculative workflow task on A's sticky queue, and A fails it.
 3. The session stays on A: the retry is delivered on A's sticky queue, never the normal queue.
*/
func TestWorkflowSessionKeepsWorkerAfterFailedUpdateTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(time.Minute)
	e.registerStickyPoller("A")

	startUpdate(t, e.env, e.tv)

	// A fails the update's workflow task, and then fails the retry too, which must also arrive on A's sticky queue.
	for range 2 {
		handled := false
		err := e.pollWorkflowTask("A", e.stickyQueue("A"), 10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			handled = true
			return nil, context.Canceled
		})
		require.NoError(t, err)
		require.True(t, handled)
	}

	err := e.pollWorkflowTask("B", e.tv.TaskQueue(), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.FailNow(t, "the retry after a failed update task must stay on the session worker")
		return nil, nil
	})
	require.Error(t, err)
}

// startUpdate sends an update in the background until it is accepted or the test ends. On a workflow with nothing
// else pending, it schedules a speculative workflow task.
func startUpdate(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars) {
	updateCtx, cancelUpdate := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancelUpdate)
	go func() {
		_, _ = env.FrontendClient().UpdateWorkflowExecution(updateCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(), WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID()},
				Input: &updatepb.Input{Name: "update"},
			},
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
		})
	}()
}

/*
TestWorkflowSessionActivityWaitsForNextBinding tests a session activity whose bound worker is dead:
 1. Worker A binds the session and schedules a session activity, but never polls its sticky activity queue, so pushing
    the activity there unbinds the workflow.
 2. The activity waits and is never delivered on the normal queue, not even to session worker Y; the unbind schedules
    a workflow task there instead, without any signal.
 3. Session worker Y starts that workflow task, binding the workflow to Y, and the activity is delivered on Y's sticky
    queue.
*/
func TestWorkflowSessionActivityWaitsForNextBinding(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)
	e.awaitUnbound()
	e.requireNoActivityOn("Y", e.tv.TaskQueue())

	e.registerStickyActivityPoller("Y")
	e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
	e.requireSessionActivityOn(e.stickyQueue("Y"))
}

/*
TestWorkflowSessionActivityRetryWaitsForNextBinding tests a retry whose bound worker died during the backoff:
 1. Worker A binds the session, runs the session activity's first attempt from its sticky queue, and fails it, so the
    activity backs off for longer than Matching's sticky poller window.
 2. A dies. When the retry timer fires, the retry is pushed to A's sticky queue, Matching reports A unavailable, and
    History unbinds the workflow: the retry is never delivered on the normal queue, and a workflow task is scheduled
    there.
 3. Session worker Y starts that workflow task, binding the workflow to Y; the retry is delivered on Y's sticky queue,
    and the next workflow task goes to Y's sticky queue.
*/
func TestWorkflowSessionActivityRetryWaitsForNextBinding(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	command := e.scheduleSessionActivity("session-activity")
	// Longer than Matching's 10s sticky poller window, so A's last poll no longer keeps it alive when the retry is pushed.
	command.GetScheduleActivityTaskCommandAttributes().RetryPolicy = &commonpb.RetryPolicy{InitialInterval: durationpb.New(11 * time.Second)}
	e.registerStickyActivityPoller("A")
	e.bindToAWith(time.Second, command)
	require.Equal(t, int32(1), e.failActivityAs("A", e.stickyQueue("A")).GetAttempt())

	e.awaitUnbound()
	e.requireNoActivityOn("", e.tv.TaskQueue())

	// Y is alive for both task types, so neither the retry nor the workflow task its completion schedules unbinds Y.
	e.registerStickyPoller("Y")
	e.registerStickyActivityPoller("Y")
	e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
	require.Equal(t, int32(2), e.completeActivityAs("Y", e.stickyQueue("Y")).GetAttempt())
	e.completeOn("Y", e.stickyQueue("Y"), 10*time.Second)
}

/*
TestWorkflowSessionActivityStaysOnLiveWorker tests the happy path of a session activity on a live bound worker:
 1. Worker A polls its sticky activity queue and binds the session, so the session activity is queued at A.
 2. Session worker Y polling the normal queue never gets it, even while A has not picked it up yet.
 3. A starts it from its own sticky queue, which keeps the binding: the next workflow task goes to A's sticky queue.
*/
func TestWorkflowSessionActivityStaysOnLiveWorker(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.registerStickyActivityPoller("A")
	e.bindToA(30 * time.Second)

	e.requireNoActivityOn("Y", e.tv.TaskQueue())
	e.registerStickyPoller("A")
	require.Equal(t, "session-activity", e.completeActivityAs("A", e.stickyQueue("A")).GetActivityId())

	e.completeOn("A", e.stickyQueue("A"), 10*time.Second)
}

/*
TestWorkflowSessionNotMovedByNonSessionActivity tests that only session activities move the session:
 1. Worker A binds the session and schedules an activity outside the session, which goes to the normal queue.
 2. Session worker Y starts and completes that activity.
 3. The binding stays on A: the workflow task the completion schedules is delivered on A's sticky queue.
*/
func TestWorkflowSessionNotMovedByNonSessionActivity(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(30*time.Second, e.scheduleActivity("regular-activity"))

	e.registerStickyPoller("A")
	require.Equal(t, "regular-activity", e.completeActivityAs("Y", e.tv.TaskQueue()).GetActivityId())

	e.completeOn("A", e.stickyQueue("A"), 10*time.Second)
}

/*
TestWorkflowSessionBindReissuesAllPendingActivities tests that a bind carries a batch of session work:
 1. Worker A binds the session and schedules 10 session activities; A is dead, so the workflow is unbound, and the
    activities wait and are never delivered on the normal queue.
 2. Session worker Y starts the workflow task the unbind scheduled, binding the workflow to Y.
 3. All 10 activities are reissued onto Y's sticky queue in the same bind.
*/
func TestWorkflowSessionBindReissuesAllPendingActivities(t *testing.T) {
	t.Parallel()
	const activities = 10
	e := newSessionMoveEnv(t, 10*time.Second)
	var commands []*commandpb.Command
	for i := range activities {
		commands = append(commands, e.scheduleSessionActivity(fmt.Sprintf("session-activity-%d", i)))
	}
	e.bindToAWith(30*time.Second, commands...)
	e.awaitUnbound()
	e.requireNoActivityOn("", e.tv.TaskQueue())

	// Y is alive for both task types, so the workflow task the first completion schedules does not unbind Y before the
	// other activities are started.
	e.registerStickyPoller("Y")
	e.registerStickyActivityPoller("Y")
	e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)

	reissued := map[string]bool{}
	for range activities {
		reissued[e.completeActivityAs("Y", e.stickyQueue("Y")).GetActivityId()] = true
	}
	require.Len(t, reissued, activities)
	e.requireNoActivityOn("", e.tv.TaskQueue())
}

/*
TestWorkflowSessionUnbindReschedulesPendingWorkflowTask tests that a pending workflow task follows an unbind to the
normal queue:
 1. Worker A binds the session and is alive for workflow tasks only. A signal arrives while A runs a workflow task, so
    the completion that schedules a session activity also schedules a workflow task for A, which can be queued in A's
    sticky backlog.
 2. Pushing the session activity to A's sticky queue unbinds the workflow, which reschedules the pending workflow task
    onto the normal queue; a copy in A's backlog is obsolete.
 3. Session worker Y starts it, binding the workflow to Y: A never gets the obsolete copy, and the session activity is
    delivered on Y's sticky queue.
*/
func TestWorkflowSessionUnbindReschedulesPendingWorkflowTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(30 * time.Second)

	e.registerStickyPoller("A")
	e.signal()
	err := e.pollWorkflowTask("A", e.stickyQueue("A"), 10*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		e.signal()
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands:         []*commandpb.Command{e.scheduleSessionActivity("session-activity")},
			StickyAttributes: e.stickyAttributes("A", 30*time.Second),
		}, nil
	})
	require.NoError(t, err)
	e.awaitUnbound()

	e.registerStickyActivityPoller("Y")
	e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
	err = e.pollWorkflowTask("A", e.stickyQueue("A"), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.FailNow(t, "the copy in A's backlog must be obsolete after the unbind")
		return nil, nil
	})
	require.Error(t, err)
	e.requireSessionActivityOn(e.stickyQueue("Y"))
}

/*
TestWorkflowSessionInheritedByNextRun tests that a new run of the workflow keeps the session on its worker:
 1. Session worker Y starts the first workflow task, binding the workflow to Y, and closes the run with a
    continue-as-new, a failure under a retry policy, or a completion under a cron schedule.
 2. The next run's first workflow task is delivered on Y's sticky queue instead of the normal queue, except for a
    cron run, which starts unbound on the normal queue.
*/
func TestWorkflowSessionInheritedByNextRun(t *testing.T) {
	t.Parallel()
	for _, tc := range nextRunCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSessionMoveEnvForNextRun(t, 10*time.Second, tc)
			e.registerStickyPoller("Y")
			err := e.pollWorkflowTask("Y", e.tv.TaskQueue(), 30*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{tc.command(e.tv)}, StickyAttributes: e.stickyAttributes("Y", 5*time.Second),
				}, nil
			})
			require.NoError(t, err)

			if tc.unbound {
				e.requireNextRunOn("", e.tv.TaskQueue())
				return
			}
			e.requireNextRunOn("Y", e.stickyQueue("Y"))
		})
	}
}

/*
TestWorkflowSessionInheritedBindingFallsBackWhenWorkerGone tests a next run whose inherited worker is gone:
 1. Worker Y binds the workflow from the normal queue but never polls its sticky queue, so Matching treats it as gone.
 2. Y continues the workflow as new; the next run inherits the binding to Y.
 3. Its first workflow task falls back to the normal queue, where any worker can start it.
*/
func TestWorkflowSessionInheritedBindingFallsBackWhenWorkerGone(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	err := e.pollWorkflowTask("Y", e.tv.TaskQueue(), 30*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{continueAsNewCmd(e.tv.WorkflowType(), e.tv.TaskQueue())}, StickyAttributes: e.stickyAttributes("Y", time.Second),
		}, nil
	})
	require.NoError(t, err)

	e.requireNextRunOn("", e.tv.TaskQueue())
}

// nextRunCase is one way a workflow starts its next run, for tests of what the next run inherits.
type nextRunCase struct {
	name         string
	retryPolicy  *commonpb.RetryPolicy
	cronSchedule string
	// unbound is true when the next run does not inherit the binding: a cron run starts unbound.
	unbound bool
	// command closes the first run.
	command func(tv *testvars.TestVars) *commandpb.Command
}

func nextRunCases() []nextRunCase {
	return []nextRunCase{
		{name: "continue-as-new", command: func(tv *testvars.TestVars) *commandpb.Command {
			return continueAsNewCmd(tv.WorkflowType(), tv.TaskQueue())
		}},
		{name: "retry", retryPolicy: &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1, MaximumAttempts: 2},
			command: func(*testvars.TestVars) *commandpb.Command { return failCmd("retry me") }},
		{name: "cron", cronSchedule: "@every 1s", unbound: true, command: func(*testvars.TestVars) *commandpb.Command { return completeCmd() }},
	}
}

// multiSessionWorkerOptions are a multi-session worker's session options. Its sticky queue is named after its instance key.
func multiSessionWorkerOptions() *sessionpb.WorkerSessionOptions {
	return &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_MultiSession{
		MultiSession: &sessionpb.MultiSessionOptions{},
	}}
}

/*
TestWorkflowSessionBindingSurvivesRebuild tests that a non-exclusive binding is rebuilt from history:
 1. A multi-session worker binds the workflow, another one takes it over, or a worker without session options unbinds it.
 2. The workflow is reset to its last workflow task, or its mutable state is rebuilt from history.
 3. The next workflow task goes to the bound worker's sticky queue, or to the normal queue after an unbind.
*/
func TestWorkflowSessionBindingSurvivesRebuild(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// bind leaves the workflow bound to the returned worker ("" for unbound) after its last workflow task.
		bind func(e *sessionMoveEnv) string
		// rebuild resets or rebuilds the workflow and returns the run that receives the next workflow task.
		rebuild func(e *sessionMoveEnv) string
	}{
		{
			name: "reset after a bind",
			bind: func(e *sessionMoveEnv) string {
				e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
				return "Y"
			},
			rebuild: resetToLastWorkflowTask,
		},
		{
			name: "rebuild from history after a bind",
			bind: func(e *sessionMoveEnv) string {
				e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
				return "Y"
			},
			rebuild: rebuildFromHistory,
		},
		{
			name: "reset after a move",
			bind: func(e *sessionMoveEnv) string {
				e.bindToA(time.Second)
				e.signal()
				// Y is alive for the session activity it receives, so the binding to Y stays.
				e.registerStickyActivityPoller("Y")
				e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
				return "Y"
			},
			rebuild: resetToLastWorkflowTask,
		},
		{
			name: "reset after an unbind",
			bind: func(e *sessionMoveEnv) string {
				// No session activity, so a worker without session options can complete the workflow's tasks.
				e.bindToAWith(time.Second)
				e.signal()
				e.completeOn("", e.tv.TaskQueue(), 30*time.Second)
				return ""
			},
			rebuild: resetToLastWorkflowTask,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSessionMoveEnv(t, 10*time.Second)
			e.registerStickyPoller("Y")
			worker := tc.bind(e)

			runID := tc.rebuild(e)

			queue := e.tv.TaskQueue()
			if worker != "" {
				queue = e.stickyQueue(worker)
			}
			err := e.pollWorkflowTask(worker, queue, 10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				require.Equal(t, runID, task.GetWorkflowExecution().GetRunId())
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			})
			require.NoError(t, err)
		})
	}
}

/*
TestWorkflowSessionBindingByUpdateTaskSurvivesRebuild tests that a binding made by a speculative workflow task's start
is persisted, although such a start is otherwise kept in memory only:
 1. A worker without session options completes the first workflow task, so the workflow is unbound.
 2. An update schedules a speculative workflow task, which multi-session worker B starts from the normal queue. The
    start binds the workflow to B and is persisted at once.
 3. B accepts the update, and the workflow's mutable state is rebuilt from history.
 4. The next workflow task goes to B's sticky queue.
*/
func TestWorkflowSessionBindingByUpdateTaskSurvivesRebuild(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.completeOn("", e.tv.TaskQueue(), 30*time.Second)
	e.registerStickyPoller("B")

	startUpdate(t, e.env, e.tv)
	err := e.pollWorkflowTask("B", e.tv.TaskQueue(), 30*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Len(t, task.GetMessages(), 1)
		protorequire.ProtoEqual(t, &sessionpb.SessionInfo{WorkerQueue: e.instanceKey("B")}, persistedBinding(t, e.env, e.tv.WorkflowExecution()))
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Messages:         updateutils.New(t).UpdateAcceptMessages(e.tv, task.GetMessages()[0]),
			StickyAttributes: e.stickyAttributes("B", time.Second),
		}, nil
	})
	require.NoError(t, err)
	requireStartedEventRecordsBinding(t, e.env, e.tv.WorkflowExecution())

	runID := rebuildFromHistory(e)

	err = e.pollWorkflowTask("B", e.stickyQueue("B"), 10*time.Second, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.Equal(t, runID, task.GetWorkflowExecution().GetRunId())
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	require.NoError(t, err)
}

// rebuildFromHistory rebuilds the workflow's mutable state from history, signals it, and returns its run's ID.
func rebuildFromHistory(e *sessionMoveEnv) string {
	described, err := e.env.FrontendClient().DescribeWorkflowExecution(e.t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: e.env.Namespace().String(), Execution: e.tv.WorkflowExecution(),
	})
	require.NoError(e.t, err)
	execution := described.GetWorkflowExecutionInfo().GetExecution()
	_, err = e.env.AdminClient().RebuildMutableState(e.t.Context(), &adminservice.RebuildMutableStateRequest{
		Namespace: e.env.Namespace().String(), Execution: execution,
	})
	require.NoError(e.t, err)
	e.signal()
	return execution.GetRunId()
}

// resetToLastWorkflowTask resets the workflow to its last completed workflow task and returns the reset run's ID.
func resetToLastWorkflowTask(e *sessionMoveEnv) string {
	var lastCompleted int64
	history := e.env.SdkClient().GetWorkflowHistory(e.t.Context(), e.tv.WorkflowID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		event, err := history.Next()
		require.NoError(e.t, err)
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastCompleted = event.GetEventId()
		}
	}
	return resetWorkflowTo(e.t, e.env, e.tv.WorkflowExecution(), lastCompleted)
}

// requireStartedEventRecordsBinding requires the last workflow task started event to record the binding mutable state
// holds, so a rebuild from history restores it.
func requireStartedEventRecordsBinding(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) {
	want := persistedBinding(t, env, execution)
	var recorded *sessionpb.SessionInfo
	history := env.SdkClient().GetWorkflowHistory(t.Context(), execution.GetWorkflowId(), execution.GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		event, err := history.Next()
		require.NoError(t, err)
		if attributes := event.GetWorkflowTaskStartedEventAttributes(); attributes != nil {
			recorded = attributes.GetSessionInfo()
		}
	}
	protorequire.ProtoEqual(t, want, recorded)
}

// persistedBinding returns the binding in the workflow's persisted mutable state, or nil when it is unbound.
func persistedBinding(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) *sessionpb.SessionInfo {
	response, err := env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(), Execution: execution,
	})
	require.NoError(t, err)
	info := response.GetDatabaseMutableState().GetExecutionInfo()
	if info.GetSessionState() == nil {
		return nil
	}
	return &sessionpb.SessionInfo{ExclusiveWorker: info.GetSessionState().GetExclusiveWorker(), WorkerQueue: info.GetStickyTaskQueue()}
}

// resetWorkflowTo resets execution to workflowTaskFinishEventID and returns the reset run's ID.
func resetWorkflowTo(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution, workflowTaskFinishEventID int64) string {
	response, err := env.FrontendClient().ResetWorkflowExecution(t.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowExecution: execution, Reason: "test",
		WorkflowTaskFinishEventId: workflowTaskFinishEventID, RequestId: uuid.NewString(),
	})
	require.NoError(t, err)
	return response.GetRunId()
}

/*
TestWorkflowSessionRejectsVersioningOverride tests that a workflow bound to a multi-session worker cannot be given a
versioning override that changes its version:
 1. Worker A binds the session and is alive on its sticky queue.
 2. UpdateWorkflowExecutionOptions setting an auto-upgrade override is rejected, since it changes the effective
    versioning behavior; removing the override the workflow does not have succeeds, since it changes nothing.
 3. The binding is unchanged: the next workflow task is delivered on A's sticky queue, and A completes it.
*/
func TestWorkflowSessionRejectsVersioningOverride(t *testing.T) {
	t.Parallel()
	for _, tc := range versioningOverrideUpdates() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSessionMoveEnv(t, 10*time.Second)
			e.bindToAWith(time.Second)
			e.registerStickyPoller("A")

			requireVersioningOverrideUpdate(t, updateVersioningOverride(t, e.env, e.tv.WorkflowExecution(), tc.override), tc.rejected)
			protorequire.ProtoEqual(t, &sessionpb.SessionInfo{WorkerQueue: e.instanceKey("A")}, persistedBinding(t, e.env, e.tv.WorkflowExecution()))

			e.signal()
			e.completeOn("A", e.stickyQueue("A"), 5*time.Second)
		})
	}
}

/*
TestWorkflowSessionRejectsResetStickyTaskQueue tests that resetting the sticky queue does not release a multi-session
binding:
 1. Worker A binds the session and is alive on its sticky queue.
 2. ResetStickyTaskQueue is rejected with FailedPrecondition.
 3. The binding is unchanged: the workflow stays bound to A, and its next workflow task is delivered on A's sticky
    queue.
*/
func TestWorkflowSessionRejectsResetStickyTaskQueue(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(time.Second)
	e.registerStickyPoller("A")

	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, resetStickyTaskQueue(t, e.env, e.tv.WorkflowExecution()), &failedPrecondition)
	protorequire.ProtoEqual(t, &sessionpb.SessionInfo{WorkerQueue: e.instanceKey("A")}, persistedBinding(t, e.env, e.tv.WorkflowExecution()))

	e.signal()
	e.completeOn("A", e.stickyQueue("A"), 5*time.Second)
}

/*
TestWorkflowSessionRejectsUpgradeOnContinueAsNew tests that a workflow bound to a multi-session worker cannot upgrade on
continue-as-new:
 1. Worker A binds the session and is alive on its sticky queue.
 2. A continues the workflow as new with an auto-upgrade initial versioning behavior: the workflow task fails with
    BAD_CONTINUE_AS_NEW_ATTRIBUTES.
 3. The retry is delivered on A's sticky queue, and a plain continue-as-new completes it.
*/
func TestWorkflowSessionRejectsUpgradeOnContinueAsNew(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToAWith(time.Second)
	e.registerStickyPoller("A")

	e.signal()
	err := e.pollWorkflowTask("A", e.stickyQueue("A"), 5*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{upgradeOnContinueAsNewCmd(e.tv)}, StickyAttributes: e.stickyAttributes("A", time.Second),
		}, nil
	})
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)
	requireWorkflowTaskFailedWith(t, e.env, e.tv.WorkflowExecution(), enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES)

	err = e.pollWorkflowTask("A", e.stickyQueue("A"), 5*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{continueAsNewCmd(e.tv.WorkflowType(), e.tv.TaskQueue())}, StickyAttributes: e.stickyAttributes("A", time.Second),
		}, nil
	})
	require.NoError(t, err)
}

/*
TestWorkflowSessionRefusesWorkflowWithVersioningOverride tests that a multi-session worker never binds a workflow with a
versioning override:
 1. A workflow starts with an auto-upgrade versioning override, or with none.
 2. Multi-session worker Y polls the normal queue: with the override it gets nothing, and the workflow stays unbound;
    without one, Y binds it.
 3. A worker without session options completes the workflow task Y did not take.

A pinned override cannot be tested here: it routes the workflow's tasks to a deployment version, and multi-session
workers cannot poll with worker versioning.
*/
func TestWorkflowSessionRefusesWorkflowWithVersioningOverride(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		override func(*testvars.TestVars) *workflowpb.VersioningOverride
	}{
		{name: "auto-upgrade override", override: func(*testvars.TestVars) *workflowpb.VersioningOverride { return autoUpgradeOverride() }},
		{name: "no override"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableMultiSessionWorkers, true))
			tv := env.Tv()
			startWithVersioningOverride(t, env, tv, tc.override)
			e := &sessionMoveEnv{t: t, env: env, tv: tv, poller: taskpoller.New(t, env.FrontendClient(), env.Namespace().String())}

			if tc.override == nil {
				e.completeOn("Y", e.tv.TaskQueue(), 10*time.Second)
				protorequire.ProtoEqual(t, &sessionpb.SessionInfo{WorkerQueue: e.instanceKey("Y")}, persistedBinding(t, env, tv.WorkflowExecution()))
				return
			}
			err := e.pollWorkflowTask("Y", e.tv.TaskQueue(), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				require.FailNow(t, "a workflow with a versioning override must not reach a multi-session worker")
				return nil, nil
			})
			require.Error(t, err)
			require.Nil(t, persistedBinding(t, env, tv.WorkflowExecution()))
			e.completeOn("", e.tv.TaskQueue(), 10*time.Second)
		})
	}
}
