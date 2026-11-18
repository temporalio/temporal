package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// sessionMoveEnv drives a workflow through raw pollers that act as session workers. A worker "dies" by no
// longer polling its sticky queue; Matching then treats it as unavailable and History falls back to the
// normal queue.
type sessionMoveEnv struct {
	t      *testing.T
	env    *testcore.TestEnv
	tv     *testvars.TestVars
	poller *taskpoller.TaskPoller
}

func newSessionMoveEnv(t *testing.T, workflowTaskTimeout time.Duration) *sessionMoveEnv {
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableStickyActivityExecution, true))
	tv := env.Tv()
	_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(5 * time.Minute), WorkflowTaskTimeout: durationpb.New(workflowTaskTimeout),
	})
	require.NoError(t, err)
	return &sessionMoveEnv{t: t, env: env, tv: tv, poller: taskpoller.New(t, env.FrontendClient(), env.Namespace().String())}
}

func (e *sessionMoveEnv) stickyQueue(worker string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{Name: e.tv.TaskQueue().GetName() + "-sticky-" + worker, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: e.tv.TaskQueue().GetName()}
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
		request.SessionOptions = multiSessionWorkerOptions(e.stickyQueue(worker).GetName())
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

func (e *sessionMoveEnv) pollActivityTask(queue *taskqueuepb.TaskQueue, timeout time.Duration) (*workflowservice.PollActivityTaskQueueResponse, error) {
	var task *workflowservice.PollActivityTaskQueueResponse
	_, err := e.poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{TaskQueue: queue}).HandleTask(e.tv,
		func(t *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			task = t
			return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
		}, taskpoller.WithTimeout(timeout))
	return task, err
}

func (e *sessionMoveEnv) signal() {
	_, err := e.env.FrontendClient().SignalWorkflowExecution(e.t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: e.env.Namespace().String(), WorkflowExecution: e.tv.WorkflowExecution(), SignalName: "wake", Identity: "client",
	})
	require.NoError(e.t, err)
}

func (e *sessionMoveEnv) scheduleSessionActivity(id string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId: id, ActivityType: e.tv.ActivityType(), TaskQueue: e.tv.TaskQueue(),
			ScheduleToCloseTimeout: durationpb.New(2 * time.Minute), StartToCloseTimeout: durationpb.New(time.Minute),
			SessionOptions: &sessionpb.ActivitySessionOptions{Placement: &sessionpb.ActivitySessionOptions_WorkflowSession{WorkflowSession: &emptypb.Empty{}}},
		}},
	}
}

// bindToA starts the session on worker A from the normal queue and schedules one session activity, which is
// dispatched to A's sticky queue. A never polls its sticky queue, so Matching considers it unavailable.
func (e *sessionMoveEnv) bindToA(stickyScheduleToStart time.Duration) {
	err := e.pollWorkflowTask("A", e.tv.TaskQueue(), 30*time.Second, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{e.scheduleSessionActivity("session-activity")}, StickyAttributes: e.stickyAttributes("A", stickyScheduleToStart),
		}, nil
	})
	require.NoError(e.t, err)
}

func (e *sessionMoveEnv) completeOn(worker string, queue *taskqueuepb.TaskQueue, timeout time.Duration) {
	err := e.pollWorkflowTask(worker, queue, timeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		response := &workflowservice.RespondWorkflowTaskCompletedRequest{}
		if worker != "" {
			response.StickyAttributes = e.stickyAttributes(worker, time.Second)
		}
		return response, nil
	})
	require.NoError(e.t, err)
}

func (e *sessionMoveEnv) requireSessionActivityOn(queue *taskqueuepb.TaskQueue) {
	task, err := e.pollActivityTask(queue, 30*time.Second)
	require.NoError(e.t, err)
	require.Equal(e.t, "session-activity", task.GetActivityId())
}

func (e *sessionMoveEnv) requireNoActivityOn(queue *taskqueuepb.TaskQueue) {
	task, err := e.pollActivityTask(queue, 3*time.Second)
	require.Error(e.t, err)
	require.Nil(e.t, task)
}

/*
TestWorkflowSessionMovesWithWorkflowTask tests the main move path:
 1. Worker A starts the workflow, binds the session, and schedules a session activity on A's sticky queue.
 2. A dies, so the next workflow task falls back to the normal queue and worker B starts it.
 3. The session moves to B and the pending session activity is rescheduled onto B's sticky queue.
*/
func TestWorkflowSessionMovesWithWorkflowTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)

	e.signal()
	e.completeOn("B", e.tv.TaskQueue(), 30*time.Second)

	e.requireSessionActivityOn(e.stickyQueue("B"))
}

/*
TestWorkflowSessionStaysOnBusyWorker tests hard stickiness while the worker is alive:
 1. Worker A binds the session and polls its sticky queue once, so Matching sees it as alive.
 2. A workflow task waits in A's backlog past its schedule-to-start timeout while B polls the normal queue.
 3. The timeout re-offers the task to A instead of the normal queue: B gets nothing and A completes it.
*/
func TestWorkflowSessionStaysOnBusyWorker(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
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
	require.True(t, timedOut, "the busy worker's task should have timed out at least once without moving")
}

/*
TestWorkflowSessionRecoversWhenNewWorkerCrashesMidTask tests the window between a move and its completion:
 1. B polls its sticky queue once, the session moves from A to B, and B crashes while running the task.
 2. The retry goes to B's sticky queue with a schedule-to-start timer, because the move already set stickiness.
 3. Once Matching considers B dead, the retry falls back and worker C takes over the session.
*/
func TestWorkflowSessionRecoversWhenNewWorkerCrashesMidTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 2*time.Second)
	e.bindToA(time.Second)

	// B is alive to Matching before it takes over, so the retry lands in its backlog rather than falling back.
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

	e.completeOn("C", e.tv.TaskQueue(), time.Minute)
	e.requireSessionActivityOn(e.stickyQueue("C"))

	// The retry waited on B's sticky queue, timing out there, before it fell back to C. Without the binding's
	// sticky queue, the retry would have gone straight to the normal queue with no such timeout.
	startToCloseTimedOut, retryTimedOutOnB := false, false
	for _, event := range e.env.GetHistory(e.env.Namespace().String(), e.tv.WorkflowExecution()) {
		switch event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType() {
		case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
			startToCloseTimedOut = true
		case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
			retryTimedOutOnB = retryTimedOutOnB || startToCloseTimedOut
		default:
		}
	}
	require.True(t, retryTimedOutOnB, "the retry after B's crash should time out on B's sticky queue first")
}

/*
TestWorkflowSessionDroppedAndRebound tests a non-session worker taking over and a session worker returning:
 1. A binds the session and dies; worker Z without session options starts the next workflow task.
 2. The session is dropped and the pending session activity is rescheduled onto the normal queue.
 3. Session worker W then starts a workflow task, rebinding the session; the copy on the normal queue is now
    obsolete, and the activity is delivered on W's sticky queue.
*/
func TestWorkflowSessionDroppedAndRebound(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)

	e.signal()
	e.completeOn("", e.tv.TaskQueue(), 30*time.Second)

	e.signal()
	e.completeOn("W", e.tv.TaskQueue(), 30*time.Second)

	e.requireNoActivityOn(e.tv.TaskQueue())
	e.requireSessionActivityOn(e.stickyQueue("W"))
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
	e.bindToA(time.Minute)
	e.registerStickyPoller("A")

	updateCtx, cancelUpdate := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelUpdate()
	go func() {
		_, _ = e.env.FrontendClient().UpdateWorkflowExecution(updateCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: e.env.Namespace().String(), WorkflowExecution: e.tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: e.tv.UpdateID()},
				Input: &updatepb.Input{Name: "update"},
			},
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
		})
	}()

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

func multiSessionWorkerOptions(workerAddress string) *sessionpb.WorkerSessionOptions {
	return &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_MultiSession{
		MultiSession: &sessionpb.MultiSessionOptions{WorkerAddress: workerAddress},
	}}
}
