package tests

import (
	"context"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// CallbacksCircuitBreakerSuite covers the outbound queue's circuit breaker as it applies to
// completion callback deliveries.
type CallbacksCircuitBreakerSuite struct {
	parallelsuite.Suite[*CallbacksCircuitBreakerSuite]
}

func TestCallbacksCircuitBreakerSuite(t *testing.T) {
	parallelsuite.Run(t, &CallbacksCircuitBreakerSuite{})
}

// gobreaker's default ReadyToTrip, which the outbound queue's pool leaves in place, opens the
// breaker once consecutive failures exceed five.
const circuitBreakerFailureThreshold = 5

// newCircuitBreakerEnv builds an env with standalone activity callbacks enabled and the callback
// retry policy dialed down so that failures accumulate within a test's lifetime. The retry policy
// is a global setting, hence the dedicated cluster.
func (s *CallbacksCircuitBreakerSuite) newCircuitBreakerEnv(extra ...testcore.TestOption) *standaloneActivityEnv {
	opts := []testcore.TestOption{
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(activity.Enabled, true),
		testcore.WithDynamicConfig(activity.EnableCallbacks, true),
		// The callback targets are local HTTP servers, so plain HTTP has to be allowed.
		testcore.WithDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 10*time.Millisecond),
	}
	return &standaloneActivityEnv{TestEnv: testcore.NewEnv(s.T(), append(opts, extra...)...)}
}

// callbackTarget is an HTTP server accepting Nexus completions, failing each delivery with a
// retryable error while it is failing and succeeding once it is not. Its URL is the target of a
// completion callback, and its host is the destination the outbound queue keys the circuit breaker
// by - so two targets are two destinations.
type callbackTarget struct {
	url        string
	failing    atomic.Bool
	deliveries atomic.Int32
}

// newCallbackTarget starts a target, failing every delivery until [callbackTarget.stopFailing] is
// called if failing is set. The server shuts down when t cleans up.
func newCallbackTarget(t *testing.T, failing bool) *callbackTarget {
	target := &callbackTarget{}
	target.failing.Store(failing)

	srv := httptest.NewServer(nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
		Handler: target,
	}))
	t.Cleanup(srv.Close)
	target.url = srv.URL

	return target
}

func (h *callbackTarget) CompleteOperation(_ context.Context, _ *nexusrpc.CompletionRequest) error {
	h.deliveries.Add(1)
	if h.failing.Load() {
		// A retryable error, so the delivery counts against the destination's circuit breaker.
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
	}
	return nil
}

// stopFailing makes every subsequent delivery succeed.
func (h *callbackTarget) stopFailing() {
	h.failing.Store(false)
}

// startAndCompleteActivity starts a standalone activity with a single completion callback to
// target, then completes it, so the callback is scheduled for delivery right away.
func (s *CallbacksCircuitBreakerSuite) startAndCompleteActivity(
	env *standaloneActivityEnv,
	t *testing.T,
	target *callbackTarget,
) string {
	t.Helper()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        env.Tv().ActivityType(),
		Identity:            env.Tv().WorkerIdentity(),
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		RequestId:           env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: target.url}},
		}},
	})
	require.NoError(t, err)

	pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)

	_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Result:    defaultResult,
		Identity:  env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)

	return activityID
}

// awaitCallbackState waits for the activity's single completion callback to reach wantState,
// failing if it is reported as any of forbidden along the way, and returns the CallbackInfo it
// settled on.
func (s *CallbacksCircuitBreakerSuite) awaitCallbackState(
	env *standaloneActivityEnv,
	t *testing.T,
	activityID string,
	wantState enumspb.CallbackState,
	forbidden ...enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	t.Helper()

	// A forbidden sighting is sticky: recorded on the attempt and reported after the wait, since
	// reaching wantState afterwards does not make it acceptable. The condition runs on its own
	// goroutine, so it must not assert on the enclosing t.
	var (
		forbiddenSeen enumspb.CallbackState
		cbInfo        *callbackpb.CallbackInfo
	)
	await.Require(s.Context(), t, func(c *await.T) {
		descResp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(c, err)
		require.Len(c, descResp.GetCallbacks(), 1)

		cbInfo = descResp.GetCallbacks()[0].GetInfo()
		got := cbInfo.GetState()
		if slices.Contains(forbidden, got) {
			forbiddenSeen = got
			c.Fatalf("Callback has forbidden state %s", got)
		}
		require.Equal(c, wantState, got)
	}, 30*time.Second, 200*time.Millisecond)
	require.Equal(t, enumspb.CALLBACK_STATE_UNSPECIFIED, forbiddenSeen,
		"Callback reached forbidden state %s", forbiddenSeen)

	return cbInfo
}

// TestBlockedWhenCircuitBreakerOpens covers deliveries that keep failing against the same target:
// the breaker for its destination opens and Describe reports the callback as BLOCKED.
func (s *CallbacksCircuitBreakerSuite) TestBlockedWhenCircuitBreakerOpens() {
	env := s.newCircuitBreakerEnv()
	t := s.T()

	target := newCallbackTarget(t, true)
	activityID := s.startAndCompleteActivity(env, t, target)

	// Deliveries are retried until enough have failed to open the breaker, so the callback passes
	// through SCHEDULED and BACKING_OFF on the way to BLOCKED.
	cbInfo := s.awaitCallbackState(env, t, activityID, enumspb.CALLBACK_STATE_BLOCKED)

	require.Equal(t, "The circuit breaker is open.", cbInfo.GetBlockedReason())
	require.Greater(t, cbInfo.GetAttempt(), int32(circuitBreakerFailureThreshold),
		"the breaker should not open before the failure threshold is exceeded")
}

// TestBreakerIsPerDestination covers the isolation the per-destination key buys: one dead callback
// target must not hold back deliveries to a healthy one.
//
// The two activities are deliberately sequential. Attaching both callbacks to one activity does not
// test anything: the healthy delivery succeeds immediately, long before the failing one has
// accumulated enough failures for there to be an open breaker to be affected by.
func (s *CallbacksCircuitBreakerSuite) TestBreakerIsPerDestination() {
	env := s.newCircuitBreakerEnv()
	t := s.T()

	deadTarget := newCallbackTarget(t, true)
	blockedActivity := s.startAndCompleteActivity(env, t, deadTarget)
	s.awaitCallbackState(env, t, blockedActivity, enumspb.CALLBACK_STATE_BLOCKED)

	// With the breaker for the dead target now open, a delivery to a healthy one still goes
	// through - it is a destination of its own, keyed by its own host.
	healthyTarget := newCallbackTarget(t, false)
	healthyActivity := s.startAndCompleteActivity(env, t, healthyTarget)
	s.awaitCallbackState(env, t, healthyActivity,
		enumspb.CALLBACK_STATE_SUCCEEDED, enumspb.CALLBACK_STATE_BLOCKED)
	require.EqualValues(t, 1, healthyTarget.deliveries.Load())
}

// TestRecoversFromBlocked covers BLOCKED not being terminal: once the breaker's open period elapses
// it half-opens, and the target, healthy again by then, gets the delivery.
func (s *CallbacksCircuitBreakerSuite) TestRecoversFromBlocked() {
	env := s.newCircuitBreakerEnv(
		// Shorten the open period so the breaker half-opens within the test rather than after the
		// default minute. It still has to comfortably outlast the retry interval: while the breaker
		// is open the callback sits in SCHEDULED, but every delivery the breaker lets through on
		// half-opening puts it back into BACKING_OFF, where BLOCKED cannot be observed.
		testcore.WithDynamicConfig(dynamicconfig.OutboundQueueCircuitBreakerSettings,
			dynamicconfig.CircuitBreakerSettings{Timeout: 3 * time.Second}),
	)
	t := s.T()

	target := newCallbackTarget(t, true)
	activityID := s.startAndCompleteActivity(env, t, target)

	// Every delivery fails, so the breaker opens.
	blockedState := s.awaitCallbackState(env, t, activityID, enumspb.CALLBACK_STATE_BLOCKED)
	require.Equal(t, "The circuit breaker is open.", blockedState.GetBlockedReason())

	// The target recovers. The breaker half-opens, lets a delivery through, and it succeeds.
	target.stopFailing()
	successfulState := s.awaitCallbackState(env, t, activityID, enumspb.CALLBACK_STATE_SUCCEEDED)

	require.Greater(t, successfulState.GetAttempt(), blockedState.GetAttempt())
	require.Empty(t, successfulState.GetBlockedReason())
}
