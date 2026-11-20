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
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
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

// instanceKey is the worker process's instance key, which Matching uses to look up its heartbeats.
func (e *sessionMoveEnv) instanceKey(worker string) string {
	return e.tv.WorkerInstanceKey() + "-" + worker
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
// it from startup would. Without it, Matching treats the worker as gone and session activities fall back.
func (e *sessionMoveEnv) registerStickyActivityPoller(worker string) {
	_, err := e.poller.PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: e.stickyQueue(worker), Identity: "worker-" + worker, SessionOptions: multiSessionWorkerOptions(e.stickyQueue(worker).GetName()),
		WorkerInstanceKey: e.instanceKey(worker),
	}).HandleTask(e.tv, func(*workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		require.FailNow(e.t, "no activity task expected while registering a sticky activity poller")
		return nil, nil
	}, taskpoller.WithTimeout(stickyPollTimeout))
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
	request := &workflowservice.PollActivityTaskQueueRequest{TaskQueue: queue, Identity: "worker-" + worker}
	if worker != "" {
		request.SessionOptions = multiSessionWorkerOptions(e.stickyQueue(worker).GetName())
		request.WorkerInstanceKey = e.instanceKey(worker)
	}
	var task *workflowservice.PollActivityTaskQueueResponse
	_, err := e.poller.PollActivityTask(request).HandleTask(e.tv,
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

// bindToA starts the session on worker A from the normal queue and schedules one session activity. A never polls
// its sticky queues, so Matching considers it unavailable and the activity falls back to the normal queue.
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

// completeOn completes a workflow task as worker, and checks the latest started event records the binding mutable
// state holds.
func (e *sessionMoveEnv) completeOn(worker string, queue *taskqueuepb.TaskQueue, timeout time.Duration) {
	err := e.pollWorkflowTask(worker, queue, timeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		response := &workflowservice.RespondWorkflowTaskCompletedRequest{}
		if worker != "" {
			response.StickyAttributes = e.stickyAttributes(worker, time.Second)
		}
		return response, nil
	})
	require.NoError(e.t, err)
	requireStartedEventRecordsBinding(e.t, e.env, e.tv.WorkflowExecution())
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
	e.registerStickyActivityPoller("B")
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

	e.registerStickyActivityPoller("C")
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
	e.registerStickyActivityPoller("W")
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

/*
TestWorkflowSessionMovesWithActivity tests an activity leading a session move:
 1. Worker A binds the session and schedules a session activity; A is dead, so the activity falls back to the
    normal queue.
 2. Session worker Y starts the activity from the normal queue, moving the session to Y.
 3. The workflow task that the activity's completion schedules is delivered on Y's sticky queue.
*/
func TestWorkflowSessionMovesWithActivity(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)

	e.registerStickyPoller("Y")
	e.completeActivityAs("Y", e.tv.TaskQueue())

	e.completeOn("Y", e.stickyQueue("Y"), 10*time.Second)
}

/*
TestWorkflowSessionDroppedByActivity tests a worker without session options starting a session activity:
 1. Worker A binds the session; its session activity falls back to the normal queue because A never polls its
    sticky activity queue.
 2. A then polls its sticky workflow queue, so it is alive for workflow tasks. This is a test device: a real worker
    polls both sticky queues. It makes the outcomes distinguishable, since a workflow still bound to a live A would
    get its next workflow task in A's backlog, while an unbound one gets it on the normal queue.
 3. A worker without session options starts the activity, which unbinds the workflow, so the workflow task its
    completion schedules is delivered on the normal queue.
*/
func TestWorkflowSessionDroppedByActivity(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)

	e.registerStickyPoller("A")
	e.completeActivityAs("", e.tv.TaskQueue())

	e.completeOn("", e.tv.TaskQueue(), 10*time.Second)
}

/*
TestWorkflowSessionMovesWithActivityRetry tests a retry leading a session move (no workflow task in between):
 1. Worker A binds the session, runs the session activity's first attempt, and fails it, so the activity backs off.
 2. A dies. When the retry timer fires, the retry is pushed to A's sticky queue, Matching reports A unavailable,
    and History falls back to the normal queue.
 3. Session worker Y starts the retry, moving the session to Y, and the next workflow task goes to Y's sticky queue.
*/
func TestWorkflowSessionMovesWithActivityRetry(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(time.Second)
	require.Equal(t, int32(1), e.failActivityAs("A", e.tv.TaskQueue()).GetAttempt())

	e.registerStickyPoller("Y")
	require.Equal(t, int32(2), e.completeActivityAs("Y", e.tv.TaskQueue()).GetAttempt())

	e.completeOn("Y", e.stickyQueue("Y"), 10*time.Second)
}

/*
TestWorkflowSessionActivityStaysOnLiveWorker tests the happy path of a session activity on a live bound worker:
 1. Worker A polls its sticky activity queue and binds the session, so the session activity is queued at A.
 2. Worker Y polling the normal queue never gets it, even while A has not picked it up yet.
 3. A starts it from its own sticky queue, which keeps the binding: the next workflow task goes to A's sticky queue.
*/
func TestWorkflowSessionActivityStaysOnLiveWorker(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.registerStickyActivityPoller("A")
	e.bindToA(30 * time.Second)

	e.requireNoActivityOn(e.tv.TaskQueue())
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
TestWorkflowSessionActivityMoveReschedulesAllPendingActivities tests that a move carries a batch of session work:
 1. Worker A binds the session and schedules 10 session activities; A is dead, so they fall back to the normal
    queue.
 2. Session worker Y starts one of them, moving the session to Y.
 3. The other 9 are rescheduled onto Y's sticky queue in the same move, and the copies left on the normal queue
    are obsolete.
*/
func TestWorkflowSessionActivityMoveReschedulesAllPendingActivities(t *testing.T) {
	t.Parallel()
	const activities = 10
	e := newSessionMoveEnv(t, 10*time.Second)
	var commands []*commandpb.Command
	for i := range activities {
		commands = append(commands, e.scheduleSessionActivity(fmt.Sprintf("session-activity-%d", i)))
	}
	e.bindToAWith(30*time.Second, commands...)

	e.registerStickyActivityPoller("Y")
	first := e.completeActivityAs("Y", e.tv.TaskQueue()).GetActivityId()

	rescheduled := map[string]bool{}
	for range activities - 1 {
		rescheduled[e.completeActivityAs("Y", e.stickyQueue("Y")).GetActivityId()] = true
	}
	require.Len(t, rescheduled, activities-1)
	require.NotContains(t, rescheduled, first)
	e.requireNoActivityOn(e.tv.TaskQueue())
}

/*
TestWorkflowSessionActivityMoveReschedulesPendingWorkflowTask tests that a pending workflow task follows an
activity-led move:
 1. Worker A binds the session and is alive for workflow tasks; its session activity falls back to the normal queue.
 2. A signal schedules a workflow task, which is queued in A's sticky backlog.
 3. Session worker Y starts the session activity, moving the session to Y; the pending workflow task is rescheduled
    onto Y's sticky queue, and the copy in A's backlog is obsolete.
*/
func TestWorkflowSessionActivityMoveReschedulesPendingWorkflowTask(t *testing.T) {
	t.Parallel()
	e := newSessionMoveEnv(t, 10*time.Second)
	e.bindToA(30 * time.Second)

	e.registerStickyPoller("A")
	e.signal()
	e.registerStickyPoller("Y")
	e.completeActivityAs("Y", e.tv.TaskQueue())

	e.completeOn("Y", e.stickyQueue("Y"), 10*time.Second)
	err := e.pollWorkflowTask("A", e.stickyQueue("A"), stickyPollTimeout, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		require.FailNow(t, "the copy in A's backlog must be obsolete after the move")
		return nil, nil
	})
	require.Error(t, err)
}

func multiSessionWorkerOptions(workerAddress string) *sessionpb.WorkerSessionOptions {
	return &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_MultiSession{
		MultiSession: &sessionpb.MultiSessionOptions{WorkerAddress: workerAddress},
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
			rebuild: func(e *sessionMoveEnv) string {
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
			},
		},
		{
			name: "reset after a move",
			bind: func(e *sessionMoveEnv) string {
				e.bindToA(time.Second)
				e.signal()
				e.completeOn("Y", e.tv.TaskQueue(), 30*time.Second)
				return "Y"
			},
			rebuild: resetToLastWorkflowTask,
		},
		{
			name: "reset after an unbind",
			bind: func(e *sessionMoveEnv) string {
				e.bindToA(time.Second)
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
	response, err := env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(), Execution: execution,
	})
	require.NoError(t, err)
	state := response.GetDatabaseMutableState().GetExecutionInfo().GetSessionState()
	var want *sessionpb.SessionInfo
	if workerQueue := state.GetBoundWorkerInfo().GetWorkerQueue(); workerQueue != "" {
		want = &sessionpb.SessionInfo{ExclusiveWorker: state.GetExclusiveWorker(), BoundWorkerInfo: &sessionpb.BoundWorkerInfo{WorkerQueue: workerQueue}}
	}
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

// resetWorkflowTo resets execution to workflowTaskFinishEventID and returns the reset run's ID.
func resetWorkflowTo(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution, workflowTaskFinishEventID int64) string {
	response, err := env.FrontendClient().ResetWorkflowExecution(t.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowExecution: execution, Reason: "test",
		WorkflowTaskFinishEventId: workflowTaskFinishEventID, RequestId: uuid.NewString(),
	})
	require.NoError(t, err)
	return response.GetRunId()
}
