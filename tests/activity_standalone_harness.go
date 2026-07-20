package tests

// Driver for standalone-activity (SAA) tests: starts an activity and drives it through a scripted
// sequence of events (a trace), realizing each event as the corresponding frontend RPC / poll /
// wall-clock wait. It does not make assertions about behavior.

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// saaInput is the activity input every driven activity is started with.
var saaInput = payloads.EncodeString("Input")

// --- driver --------------------------------------------------------------------------------

type saaHarness struct {
	env     *testcore.TestEnv
	ctx     context.Context
	cfg     model.Config
	counter int
	// activity-id prefix so that subtests don't collide.
	idBase        string
	startDelay    time.Duration
	retryInterval time.Duration // defaults to a short backoff
	// bounds a "must dispatch" poll; defaults to 10s.
	positivePollTimeout time.Duration
}

// saaWallClockSettle is slack added to a wall-clock event's clock when waiting for it to fire.
const saaWallClockSettle = 2 * time.Second

// saaPollTimeout bounds a poll: just above common.MinLongPollTimeout.
const saaPollTimeout = common.MinLongPollTimeout + time.Second

// saaHandle is a handle to one activity instance
type saaHandle struct {
	h          *saaHarness
	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

// driveTrace runs a trace (a sequence of events) for a fresh activity, realizing each event against
// the server, returning a handle to it at the reached state.
func (h *saaHarness) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	a := h.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event: a poll captures the dispatched token; a wall-clock
// event is realized by waiting for its configured time window; any other event is its RPC (which
// must succeed).
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
	h := a.h
	switch e.Kind {
	case model.Poll:
		timeout := 10 * time.Second
		if h.positivePollTimeout > 0 {
			timeout = h.positivePollTimeout
		}
		a.token = a.pollForTask(t, timeout).GetTaskToken()
	case model.BackoffElapses:
		// No time-skip in a real-server test: sleep out the backoff.
		time.Sleep(h.retryInterval + saaWallClockSettle) //nolint:forbidigo
	default:
		require.NoError(t, a.rpc(e))
	}
}

func (h *saaHarness) start(t require.TestingT) *saaHandle {
	h.counter++
	id := fmt.Sprintf("%s-%d", h.idBase, h.counter)
	resp, err := h.env.FrontendClient().StartActivityExecution(h.ctx, h.startRequest(id, id))
	require.NoError(t, err)
	return &saaHandle{h: h, activityID: id, taskQueue: id, runID: resp.RunId}
}

func (h *saaHarness) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	interval := 200 * time.Millisecond
	if h.retryInterval > 0 {
		interval = h.retryInterval
	}
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           h.env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        h.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               saaInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(interval),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(interval),
			MaximumAttempts:    h.cfg.MaxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if h.startDelay > 0 {
		req.StartDelay = durationpb.New(h.startDelay)
	}
	return req
}

// describe returns the DescribeActivityExecution response.
func (a *saaHandle) describe(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := a.h.env.FrontendClient().DescribeActivityExecution(a.h.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:          a.h.env.Namespace().String(),
		ActivityId:         a.activityID,
		RunId:              a.runID,
		IncludeOutcome:     true,
		IncludeLastFailure: true,
	})
	require.NoError(t, err)
	return resp
}

// rpc performs the RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.h.env.FrontendClient()
	ns := a.h.env.Namespace().String()
	switch e.Kind {
	case model.RespondCompleted:
		_, err := fc.RespondActivityTaskCompleted(a.h.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RespondFailed:
		_, err := fc.RespondActivityTaskFailed(a.h.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: saaFailure(e.Retryable),
		})
		return err
	case model.Pause:
		_, err := fc.PauseActivityExecution(a.h.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "worker", Reason: "drive",
		})
		return err
	default:
		return fmt.Errorf("saaHarness: unhandled event kind %v", e.Kind)
	}
}

func (a *saaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.h.ctx, timeout)
	defer cancel()
	resp, err := a.h.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.h.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	// Matching signals "waited, found nothing" with an empty response and a nil error, so a genuine
	// no-task result never surfaces as an error.
	if err != nil {
		// The poll did not complete cleanly.
		if deadline, ok := a.h.ctx.Deadline(); ok && time.Until(deadline) < common.MinLongPollTimeout {
			require.FailNowf(t, "saaHarness: test context budget exhausted before the poll could run",
				"time left: %.1fs (need >= %s); raise TEMPORAL_TEST_TIMEOUT or use `go test -timeout`. error: %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
		}
		require.FailNowf(t, "saaHarness: PollActivityTaskQueue did not complete cleanly",
			"the server rejected the poll or returned an unexpected error (a no-task result would be an empty response, not an error). error: %v", err)
	}
	// Empty response with a nil error means "waited, no task available".
	require.NotEmptyf(t, resp.GetActivityId(), "saaHarness: scripted Poll found no dispatched task within %s (still in backoff / start-delay, already started, or terminal)", timeout)
	return resp
}

func saaFailure(retryable bool) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: !retryable},
		},
	}
}

// --- traces --------------------------------------------------------------------------------
//
// A trace is an event sequence run once on one fresh activity.

type saaTrace struct {
	trace         []model.Event
	maxAttempts   int32         // RetryPolicy MaximumAttempts (0 = unlimited)
	startDelayed  bool          // activity created with a start_delay; the window length is derived (see startDelay)
	retryInterval time.Duration // RetryPolicy interval; how long the driver waits for BackoffElapses
}

// config derives the model Config from the trace.
func (tr saaTrace) config() model.Config {
	return model.Config{MaxAttempts: tr.maxAttempts}
}

// startDelay is the activity's start_delay duration: if the activity is start-delayed then the
// value used is long enough to stay open for the whole trace.
func (tr saaTrace) startDelay() time.Duration {
	if !tr.startDelayed {
		return 0
	}
	return saaLongStartDelay
}

// saaDelayWindow is the dispatch-backoff interval: long enough that a describe issued right after a
// failure still observes the state before the retry dispatches; short enough that waiting it out
// stays within the test budget.
const saaDelayWindow = 5 * time.Second

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace, so the
// activity stays SCHEDULED and pending dispatch.
const saaLongStartDelay = time.Hour

var (
	saaPoll               = model.Event{Kind: model.Poll}
	saaFailRetryably      = model.Event{Kind: model.RespondFailed, Retryable: true}
	saaBackoffDelayElapse = model.Event{Kind: model.BackoffElapses}
	saaComplete           = model.Event{Kind: model.RespondCompleted}
	saaPause              = model.Event{Kind: model.Pause}
)
