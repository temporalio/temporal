package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// gobreaker's default ReadyToTrip, which the outbound queue's pool leaves in place, opens the
// breaker once consecutive failures exceed five.
const circuitBreakerFailureThreshold = 5

// CallbacksCircuitBreakerSuite covers the outbound queue's circuit breaker as it applies to
// completion callback deliveries.
//
// There are several layers of abstraction so that the test suite can ensure circuit breaker behavior
// is consistent across execution kinds as well as callback variants. (The implementations of those
// interfaces are in another file.)
type CallbacksCircuitBreakerSuite struct {
	parallelsuite.Suite[*CallbacksCircuitBreakerSuite]
}

func TestCallbacksCircuitBreakerSuite(t *testing.T) {
	parallelsuite.Run(t, &CallbacksCircuitBreakerSuite{})
}

// newCircuitBreakerEnv builds an env with standalone activity callbacks enabled and the callback
// retry policy dialed down so that failures accumulate within a test's lifetime. The retry policy
// is a global setting, hence the dedicated cluster.
func (s *CallbacksCircuitBreakerSuite) newCircuitBreakerEnv(extra ...testcore.TestOption) *NexusTestEnv {
	opts := []testcore.TestOption{
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		// Standalone Activities
		testcore.WithDynamicConfig(activity.Enabled, true),
		testcore.WithDynamicConfig(activity.EnableCallbacks, true),
		testcore.WithDynamicConfig(activity.EnabledCallbackKinds, []callbacks.Kind{callbacks.KindNexus}),
		// Standalone Nexus operations
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []callbacks.Kind{callbacks.KindNexus}),
		// All Callbacks and Retry policy
		testcore.WithDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 10*time.Millisecond),
		// Shorten the open period so the breaker half-opens within the test rather than after the
		// default minute. It still has to comfortably outlast the retry interval: while the breaker
		// is open the callback sits in SCHEDULED, but every delivery the breaker lets through on
		// half-opening puts it back into BACKING_OFF, where BLOCKED cannot be observed.
		testcore.WithDynamicConfig(dynamicconfig.OutboundQueueCircuitBreakerSettings,
			dynamicconfig.CircuitBreakerSettings{Timeout: 3 * time.Second}),
	}
	return &NexusTestEnv{
		TestEnv:             testcore.NewEnv(s.T(), opts...),
		useTemporalFailures: true,
	}
}

// callbackExecutionType defines a Temporal execution that supports completion callbacks.
type executionType struct {
	name      string
	execution callbackExecutionType
}

var executionTypes = []executionType{
	{name: "standalone Activity", execution: &standaloneActivityExecutionType{}},
	{name: "standalone Nexus operation", execution: &standaloneNexusOperationExecutionType{}},
}

type newTargetFn = func(t *testing.T, env *NexusTestEnv, failing bool) callbackTarget

// callbackVariant is one of the ways a completion callback can address a destination.
// Currently, the only callback variant is for Nexus callbacks. (NexusHandler callbacks are
// not yet implemented.)
type callbackVariant struct {
	name string
	// newTarget starts a destination of this variant. If [failing] is set, will
	// fail every request until [callbackDestination.stopFailing] is called.
	newTarget newTargetFn
}

var callbackVariants = []callbackVariant{
	{name: "Nexus", newTarget: newNexusCallbackTarget},
}

type circuitbreakerTestSuiteTest func(*CallbacksCircuitBreakerSuite, callbackExecutionType, newTargetFn)

// forEachTestCombination runs the given testcase across all permutations of execution types and callback variants.
func (s *CallbacksCircuitBreakerSuite) forEachTestCombination(fn circuitbreakerTestSuiteTest) {
	for _, executionType := range executionTypes {
		s.Run(executionType.name, func(s *CallbacksCircuitBreakerSuite) {
			for _, callbackVariant := range callbackVariants {
				s.Run(callbackVariant.name, func(s *CallbacksCircuitBreakerSuite) {
					fn(s, executionType.execution, callbackVariant.newTarget)
				})
			}
		})
	}
}

// TestBlockedWhenCircuitBreakerOpens covers deliveries that keep failing against the same
// destination: its breaker opens and Describe reports the callback as BLOCKED.
func (s *CallbacksCircuitBreakerSuite) TestBlockedWhenCircuitBreakerOpens() {
	env := s.newCircuitBreakerEnv()

	s.forEachTestCombination(func(s *CallbacksCircuitBreakerSuite, exec callbackExecutionType, newCallbackTargetFn newTargetFn) {
		t := s.T()

		// Create a callback target that will always fail, and run the execution type.
		alwaysFailingCallbackTarget := newCallbackTargetFn(t, env, true)
		executionID := exec.startAndCompleteEx(t, env, alwaysFailingCallbackTarget)

		// Deliveries are retried until enough have failed to open the breaker, so the callback passes
		// through SCHEDULED and BACKING_OFF on the way to BLOCKED.
		callbackInfo := exec.awaitCallbackState(t, executionID, env, enumspb.CALLBACK_STATE_BLOCKED, nil)

		require.Equal(t, "The circuit breaker is open.", callbackInfo.GetBlockedReason())
		require.Greater(t, callbackInfo.GetAttempt(), int32(circuitBreakerFailureThreshold),
			"the breaker should not open before the failure threshold is exceeded")
	})
}

// TestBreakerIsPerDestination covers the isolation the per-destination key buys: one dead
// destination must not hold back deliveries to a healthy one.
//
// The two executions are deliberately sequential. Attaching both callbacks to one execution does
// not test anything: the healthy delivery succeeds immediately, long before the failing one has
// accumulated enough failures for there to be an open breaker to be affected by.
func (s *CallbacksCircuitBreakerSuite) TestBreakerIsPerDestination() {
	env := s.newCircuitBreakerEnv()

	s.forEachTestCombination(func(s *CallbacksCircuitBreakerSuite, exec callbackExecutionType, newCallbackTargetFn newTargetFn) {
		t := s.T()

		// Create an execution targeting an always failing callback target. (e.g. a Nexus handler that
		// is unavailable.)
		alwaysFailingCallbackTarget := newCallbackTargetFn(t, env, true)
		failingExecutionID := exec.startAndCompleteEx(t, env, alwaysFailingCallbackTarget)
		exec.awaitCallbackState(t, failingExecutionID, env, enumspb.CALLBACK_STATE_BLOCKED, nil)

		// Start another execution with a callback targeting a different destination for the same
		// variant of callback. (e.g. a different Nexus handler.)
		alwaysSucceedCallbackTarget := newCallbackTargetFn(t, env, false)
		successfulExecutionID := exec.startAndCompleteEx(t, env, alwaysSucceedCallbackTarget)
		errorStates := []enumspb.CallbackState{enumspb.CALLBACK_STATE_BLOCKED, enumspb.CALLBACK_STATE_BACKING_OFF}
		successfulCallbackInfo := exec.awaitCallbackState(t, successfulExecutionID, env, enumspb.CALLBACK_STATE_SUCCEEDED, errorStates)
		require.EqualValues(t, 1, successfulCallbackInfo.GetAttempt())
	})
}

// TestRecoversFromBlocked covers BLOCKED not being terminal: once the breaker's open period elapses
// it half-opens, and the destination, healthy again by then, gets the delivery.
func (s *CallbacksCircuitBreakerSuite) TestRecoversFromBlocked() {
	env := s.newCircuitBreakerEnv()

	s.forEachTestCombination(func(s *CallbacksCircuitBreakerSuite, exec callbackExecutionType, newCallbackTargetFn newTargetFn) {
		t := s.T()

		// Have the callback target always start where each request fails with a retryable error.
		testCallbackTarget := newCallbackTargetFn(t, env, true)
		executionID := exec.startAndCompleteEx(t, env, testCallbackTarget)

		blockedCbi := exec.awaitCallbackState(t, executionID, env, enumspb.CALLBACK_STATE_BLOCKED, nil)

		// Simulate the destination recovering. The breaker half-opens, lets a delivery through, and it succeeds.
		testCallbackTarget.stopFailing()
		successfulCbi := exec.awaitCallbackState(t, executionID, env, enumspb.CALLBACK_STATE_SUCCEEDED, nil)

		// Confirm the updated CallbackInfo reflects the state change.
		require.Equal(t, blockedCbi.GetRequestId(), successfulCbi.GetRequestId())
		require.Less(t, blockedCbi.GetAttempt(), successfulCbi.GetAttempt())
		require.Empty(t, successfulCbi.GetBlockedReason())
	})
}
