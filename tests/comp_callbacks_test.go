package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// circuitBreakerFailureThreshold is how many failures are encountered before the circuit breaker
// opens, and starts skipping deliveries.
const circuitBreakerFailureThreshold = 5

// CompletionCallbacksSuite tests completion callback behavior.
//
// There are several layers of abstraction so that the test suite can run the same test against
// different callback types, or different forms of execution (Workflow, standalone Activity, etc.)
type CompletionCallbacksSuite struct {
	parallelsuite.Suite[*CompletionCallbacksSuite]
}

func TestCompletionCallbacksSuite(t *testing.T) {
	parallelsuite.Run(t, &CompletionCallbacksSuite{})
}

// newTestEnv builds a test environment with completion callbacks configured for all supported
// execution types, and the retry policy dialed down so failures can accumulate within the tests's
// lifetime. The retry policy is a global setting, hence the dedicated cluster.
func (s *CompletionCallbacksSuite) newTestEnv() *testcore.TestEnv {
	opts := []testcore.TestOption{
		testcore.WithDedicatedCluster(),
		// Workflows
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		// Standalone Activities
		testcore.WithDynamicConfig(activity.Enabled, true),
		testcore.WithDynamicConfig(activity.EnableCallbacks, true),
		// Standalone Nexus operations
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		// All Callbacks and Retry policy
		testcore.WithDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 20*time.Millisecond),
		// Circuit breaker. Timeout is how long the breaker stays open before half-opening
		// (and trying to send a request again). It defaults to 60s which is too long for a
		// unit test to observe. But 1s is too short, and tests couldn't detect the BLOCKED
		// or BACKING_OFF state. 3s seems to be the sweet spot.
		testcore.WithDynamicConfig(dynamicconfig.OutboundQueueCircuitBreakerSettings,
			dynamicconfig.CircuitBreakerSettings{MaxRequests: 1, Timeout: 3 * time.Second}),
	}
	return testcore.NewEnv(s.T(), opts...)
}

// newCompletionCallbackTargetFn is a function for constructing a new completionCallbackTarget.
// It's passed to testcases so they can construct new completion callback targets with a specific behavior.
type newCompletionCallbackTargetFn func(*testing.T, *testcore.TestEnv, completionCallbackBehavior) completionCallbackTarget

// completionCallbackTestcase defines the logic for a testcase that runs against an abstracted "execution type"
// and "completion callback target",
type completionCallbackTestcase func(*CompletionCallbacksSuite, executionWithCallbacks, newCompletionCallbackTargetFn)

// forEachTestCombination runs the given testcase across all permutations of execution types and callback variants.
func (s *CompletionCallbacksSuite) forEachTestCombination(testFn completionCallbackTestcase) {
	// Types of Temporal executions that support completion callbacks.
	// COMING SOON: Standalone Nexus operations (which don't support comp. callbacks yet)
	executionTypes := []struct {
		Name      string
		Execution executionWithCallbacks
	}{
		{"Workflow", &workflowExecutionType{}},
		{"StandaloneActivity", &standaloneActivityExecutionType{}},
	}

	// Types of callback targets.
	// COMING SOON: The NexusHandler-variant.
	callbackTargets := []struct {
		Name                string
		CBTargetConstructor newCompletionCallbackTargetFn
	}{
		{"Nexus", newNexusCompletionCallbackTarget},
	}

	// Run the testcase against all combinations of execution types and callback variants.
	// If this proves to be too slow, we can run a randomized subset or something.
	for _, executionType := range executionTypes {
		s.Run(executionType.Name, func(s *CompletionCallbacksSuite) {
			for _, callbackTarget := range callbackTargets {
				s.Run(callbackTarget.Name, func(s *CompletionCallbacksSuite) {
					cbTargetCtor := callbackTarget.CBTargetConstructor
					testFn(s, executionType.Execution, cbTargetCtor)
				})
			}
		})
	}
}

// TestCallbackBasics covers the 80% case of completion callbacks being successful or failing
// with a terminal error. (Retries are covered elsewhere.)
func (s *CompletionCallbacksSuite) TestCallbackBasics() {
	env := s.newTestEnv()

	s.forEachTestCombination(
		func(
			s *CompletionCallbacksSuite,
			exec executionWithCallbacks,
			newCompCallbackTargetFn newCompletionCallbackTargetFn,
		) {
			t := s.T()

			// Create a callback target that will be successful.
			successfulTarget := newCompCallbackTargetFn(t, env, completionCallbackBehaviorSuccess)
			cbSucceedesExecutionID, err := exec.startAndCompleteEx(t, env, successfulTarget.newCallback())
			require.NoError(t, err)

			invalidStates := []enumspb.CallbackState{
				enumspb.CALLBACK_STATE_BACKING_OFF,
				enumspb.CALLBACK_STATE_BLOCKED,
				enumspb.CALLBACK_STATE_FAILED,
			}
			successfulCallbackInfo := exec.awaitCallbackState(t, cbSucceedesExecutionID, env, enumspb.CALLBACK_STATE_SUCCEEDED, invalidStates)
			require.EqualValues(t, 1, successfulCallbackInfo.GetAttempt())
			require.Nil(t, successfulCallbackInfo.LastAttemptFailure)

			// Create a callback target that will always fail.
			failingTarget := newCompCallbackTargetFn(t, env, completionCallbackBehaviorNonRetryableFailure)
			cbFailsExecutionID, err := exec.startAndCompleteEx(t, env, failingTarget.newCallback())
			require.NoError(t, err)

			failedCallbackInfo := exec.awaitCallbackState(t, cbFailsExecutionID, env, enumspb.CALLBACK_STATE_FAILED, nil)
			require.EqualValues(t, 1, failedCallbackInfo.GetAttempt())
			gotTermFailure := failedCallbackInfo.LastAttemptFailure
			require.NotNil(t, gotTermFailure)
			require.Equal(t, "handler error (BAD_REQUEST): terminal failure", gotTermFailure.GetMessage())
		})
}

func (s *CompletionCallbacksSuite) TestUnsupportedVariants() {
	env := s.newTestEnv()

	s.forEachTestCombination(
		func(
			s *CompletionCallbacksSuite,
			exec executionWithCallbacks,
			newCompCallbackTargetFn newCompletionCallbackTargetFn,
		) {
			t := s.T()

			// Try to run start an execution with an invalid completion callback variant. Confirm
			// it fails with the given error.
			cases := []struct {
				Name     string
				Callback *commonpb.Callback
				WantErr  string
			}{
				{
					"NoVariant",
					&commonpb.Callback{Variant: nil},
					"unknown callback variant: <nil>",
				},
				{
					"NexusHandler",
					&commonpb.Callback{
						Variant: &commonpb.Callback_NexusHandler_{
							NexusHandler: &commonpb.Callback_NexusHandler{
								// Should be rejected based on type, not its contents.
							},
						},
					},
					"nexusHandler callbacks are not enabled for this execution type",
				},
			}

			for _, tc := range cases {
				t.Run(tc.Name, func(t *testing.T) {
					_, gotErr := exec.startAndCompleteEx(t, env, tc.Callback)
					require.ErrorContains(t, gotErr, tc.WantErr)
				})
			}
		})
}

// TestBlockedWhenCircuitBreakerOpens covers deliveries that keep failing against the same
// destination: its breaker opens and Describe reports the callback as BLOCKED.
func (s *CompletionCallbacksSuite) TestBlockedWhenCircuitBreakerOpens() {
	env := s.newTestEnv()

	s.forEachTestCombination(
		func(
			s *CompletionCallbacksSuite,
			exec executionWithCallbacks,
			newCompCallbackTargetFn newCompletionCallbackTargetFn,
		) {
			t := s.T()

			// Create a callback target that will always fail with a retryable error.
			alwaysFailingCallbackTarget := newCompCallbackTargetFn(t, env, completionCallbackBehaviorRetryableFailure)
			executionID, err := exec.startAndCompleteEx(t, env, alwaysFailingCallbackTarget.newCallback())
			require.NoError(t, err)

			// Deliveries are retried until enough have failed to open the breaker, so the callback passes
			// through SCHEDULED and BACKING_OFF on the way to BLOCKED.
			callbackInfo := exec.awaitCallbackState(t, executionID, env, enumspb.CALLBACK_STATE_BLOCKED, nil)
			require.Equal(t, "The circuit breaker is open.", callbackInfo.GetBlockedReason())

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
func (s *CompletionCallbacksSuite) TestBreakerIsPerDestination() {
	env := s.newTestEnv()

	s.forEachTestCombination(
		func(
			s *CompletionCallbacksSuite,
			exec executionWithCallbacks,
			newCompCallbackTargetFn newCompletionCallbackTargetFn,
		) {
			t := s.T()

			// Create an execution trying to deliver a callback to an unavailable target.
			alwaysFailingCallbackTarget := newCompCallbackTargetFn(t, env, completionCallbackBehaviorRetryableFailure)
			failingExecutionID, err := exec.startAndCompleteEx(t, env, alwaysFailingCallbackTarget.newCallback())
			require.NoError(t, err)

			exec.awaitCallbackState(t, failingExecutionID, env, enumspb.CALLBACK_STATE_BLOCKED, nil)

			// Start another execution with a callback targeting a _different_ destination for the same
			// variant of callback. (e.g. a different Nexus handler.)
			alwaysSucceedCallbackTarget := newCompCallbackTargetFn(t, env, completionCallbackBehaviorSuccess)
			successfulExecutionID, err := exec.startAndCompleteEx(t, env, alwaysSucceedCallbackTarget.newCallback())
			require.NoError(t, err)

			errorStates := []enumspb.CallbackState{enumspb.CALLBACK_STATE_BLOCKED, enumspb.CALLBACK_STATE_BACKING_OFF}
			successfulCallbackInfo := exec.awaitCallbackState(t, successfulExecutionID, env, enumspb.CALLBACK_STATE_SUCCEEDED, errorStates)
			require.EqualValues(t, 1, successfulCallbackInfo.GetAttempt())
		})
}

// TestRecoversFromBlocked covers BLOCKED not being terminal: once the breaker's open period elapses
// it half-opens, and the destination, healthy again by then, gets the delivery.
func (s *CompletionCallbacksSuite) TestRecoversFromBlocked() {
	env := s.newTestEnv()

	s.forEachTestCombination(
		func(
			s *CompletionCallbacksSuite,
			exec executionWithCallbacks,
			newCompCallbackTargetFn newCompletionCallbackTargetFn,
		) {
			t := s.T()

			// Have the callback target always start where each request fails with a retryable error.
			testCallbackTarget := newCompCallbackTargetFn(t, env, completionCallbackBehaviorRetryableFailure)
			executionID, err := exec.startAndCompleteEx(t, env, testCallbackTarget.newCallback())
			require.NoError(t, err)

			blockedCbi := exec.awaitCallbackState(t, executionID, env, enumspb.CALLBACK_STATE_BLOCKED, nil)

			// Simulate the destination recovering. The breaker half-opens, lets a delivery through, and it succeeds.
			testCallbackTarget.changeBehavior(completionCallbackBehaviorSuccess)
			successfulCbi := exec.awaitCallbackState(t, executionID, env, enumspb.CALLBACK_STATE_SUCCEEDED, nil)

			// Confirm the updated CallbackInfo reflects the state change.
			require.Equal(t, blockedCbi.GetRequestId(), successfulCbi.GetRequestId())
			require.Less(t, blockedCbi.GetAttempt(), successfulCbi.GetAttempt())
			require.Empty(t, successfulCbi.GetBlockedReason())
		})
}
