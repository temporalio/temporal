package callback_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/queues"
)

// invocationTasksSuite extends callbackSuite to test InvocationTask execution.
// These tests mirror HSM's TestProcessInvocationTaskNexus_Outcomes pattern.
type invocationTasksSuite struct {
	callbackSuite
}

func TestInvocationTasksSuite(t *testing.T) {
	suite.Run(t, &invocationTasksSuite{})
}

func (s *invocationTasksSuite) SetupTest() {
	s.callbackSuite.SetupTest()
}

// TestInvocationTask_SuccessfulNexusCallback tests a successful HTTP callback invocation.
// Mirrors HSM test: TestProcessInvocationTaskNexus_Outcomes/success
// TODO (seankane): This test is functionally a duplicate of TestExecuteInvocationTaskNexus_Outcomes/success
// in executors_test.go until CHASM testing infrastructure matures to support verifying task generation
// in integration tests. Once that's available this test will test the full CHASM stack integration.
func (s *invocationTasksSuite) TestInvocationTask_SuccessfulNexusCallback() {
	// Setup HTTP caller that returns 200 OK
	s.httpCallerProvider = func(nid queues.NamespaceIDAndDestination) callback.HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}
	}
	s.setupDefaultDependencies()

	// Create callback in SCHEDULED state
	s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)

	// Create mutable context and get callback through it (like scheduler pattern)
	ctx := s.newMutableContext()
	callbackField := s.workflow.Callbacks["cb-1"]
	cb, err := callbackField.Get(ctx)
	s.NoError(err)

	// Set up contexts for reading
	readCtx := s.newContext()

	// Set up engine expectations to use the real callback from the tree
	s.ExpectReadComponent(readCtx, cb)
	s.ExpectUpdateComponent(ctx, cb)

	// Create engine context for side effect task execution
	engineCtx := s.newEngineContext()

	// Create ComponentRef
	ref := chasm.NewComponentRef[*callback.Callback](chasm.EntityKey{
		NamespaceID: s.tv.NamespaceID().String(),
		BusinessID:  s.tv.WorkflowID(),
		EntityID:    s.tv.RunID(),
	})

	// Execute InvocationTask with engine context
	err = s.invocationExecutor.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)
	s.NoError(err)

	// Close transaction to commit tasks to node backend
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to SUCCEEDED
	s.Equal(callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
}

// TestInvocationTask_FailureTriggersBackoff tests that a retryable HTTP error triggers backoff.
// Mirrors HSM test: TestProcessInvocationTaskNexus_Outcomes/retryable-error
// TODO (seankane): This test is functionally a duplicate of TestExecuteInvocationTaskNexus_Outcomes/retryable-http-error
// in executors_test.go until CHASM testing infrastructure matures to support verifying task generation
// in integration tests. Once that's available this test will test the full CHASM stack integration.
func (s *invocationTasksSuite) TestInvocationTask_FailureTriggersBackoff() {
	// Setup default dependencies first
	s.setupDefaultDependencies()

	// Then override HTTP caller to return 500 (retryable error)
	s.httpCallerProvider = func(nid queues.NamespaceIDAndDestination) callback.HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			errorBody := `{"message": "internal server error"}`
			return &http.Response{
				StatusCode: 500,
				Body:       io.NopCloser(strings.NewReader(errorBody)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
			}, nil
		}
	}
	// Recreate executor with the new HTTP caller
	s.invocationExecutor = callback.NewInvocationTaskExecutor(callback.InvocationTaskExecutorOptions{
		Config: &callback.Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		NamespaceRegistry:  s.namespaceRegistry,
		MetricsHandler:     s.metricsHandler,
		Logger:             s.logger,
		HTTPCallerProvider: s.httpCallerProvider,
		HTTPTraceProvider:  &noopHTTPClientTraceProvider{},
		HistoryClient:      nil,
		ChasmEngine:        s.mockEngine,
	})

	// Create callback in SCHEDULED state
	s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)

	// Create mutable context and get callback through it (like scheduler pattern)
	ctx := s.newMutableContext()
	callbackField := s.workflow.Callbacks["cb-1"]
	cb, err := callbackField.Get(ctx)
	s.NoError(err)

	// Set up contexts for reading
	readCtx := s.newContext()

	// Set up engine expectations to use the real callback from the tree
	s.ExpectReadComponent(readCtx, cb)
	s.ExpectUpdateComponent(ctx, cb)

	// Create ComponentRef
	ref := chasm.NewComponentRef[*callback.Callback](chasm.EntityKey{
		NamespaceID: s.tv.NamespaceID().String(),
		BusinessID:  s.tv.WorkflowID(),
		EntityID:    s.tv.RunID(),
	})

	// Create engine context for side effect task execution
	engineCtx := s.newEngineContext()

	// Execute InvocationTask with engine context
	err = s.invocationExecutor.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)
	// For retryable errors, expect an error but not an UnprocessableTaskError
	s.Error(err)
	s.NotErrorIs(err, &queues.UnprocessableTaskError{})

	// Close transaction to commit tasks to node backend
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to BACKING_OFF
	s.Equal(callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
	s.Equal(int32(1), cb.Attempt)
	s.NotNil(cb.LastAttemptFailure)
	s.NotNil(cb.NextAttemptScheduleTime)
	s.NotNil(cb.LastAttemptCompleteTime)

	// TODO: Verify BackoffTask was generated. Currently cannot verify task generation when
	// using the mock engine pattern for side-effect tasks (InvocationTask). The mock engine's
	// UpdateComponent doesn't properly integrate with CHASM's transaction system, so tasks
	// added during state transitions aren't committed to the node backend. This is a known
	// limitation - see scheduler's invoker_execute_task_test.go which also uses mock engine
	// and only verifies state changes, not task generation. To verify task generation, we
	// would need to refactor to call the executor directly (like generator_tasks_test.go),
	// but InvocationTask is a side-effect task that requires ComponentRef/mock engine.
	// s.True(s.hasBackoffTask(cb.NextAttemptScheduleTime.AsTime()))
}

// TestInvocationTask_NonRetryableFailure tests that a non-retryable error causes permanent failure.
// Mirrors HSM test: TestProcessInvocationTaskNexus_Outcomes/non-retryable-error
// TODO (seankane): This test is functionally a duplicate of TestExecuteInvocationTaskNexus_Outcomes/non-retryable-http-error
// in executors_test.go until CHASM testing infrastructure matures to support verifying task generation
// in integration tests. Once that's available this test will test the full CHASM stack integration.
func (s *invocationTasksSuite) TestInvocationTask_NonRetryableFailure() {
	// Setup default dependencies first
	s.setupDefaultDependencies()

	// Then override HTTP caller to return 400 (non-retryable error)
	s.httpCallerProvider = func(nid queues.NamespaceIDAndDestination) callback.HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			errorBody := `{"message": "bad request"}`
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader(errorBody)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
			}, nil
		}
	}
	// Recreate executor with the new HTTP caller
	s.invocationExecutor = callback.NewInvocationTaskExecutor(callback.InvocationTaskExecutorOptions{
		Config: &callback.Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		NamespaceRegistry:  s.namespaceRegistry,
		MetricsHandler:     s.metricsHandler,
		Logger:             s.logger,
		HTTPCallerProvider: s.httpCallerProvider,
		HTTPTraceProvider:  &noopHTTPClientTraceProvider{},
		HistoryClient:      nil,
		ChasmEngine:        s.mockEngine,
	})

	// Create callback in SCHEDULED state
	s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)

	// Create mutable context and get callback through it (like scheduler pattern)
	ctx := s.newMutableContext()
	callbackField := s.workflow.Callbacks["cb-1"]
	cb, err := callbackField.Get(ctx)
	s.NoError(err)

	// Set up contexts for reading
	readCtx := s.newContext()

	// Set up engine expectations to use the real callback from the tree
	s.ExpectReadComponent(readCtx, cb)
	s.ExpectUpdateComponent(ctx, cb)

	// Create engine context for side effect task execution
	engineCtx := s.newEngineContext()

	// Create ComponentRef
	ref := chasm.NewComponentRef[*callback.Callback](chasm.EntityKey{
		NamespaceID: s.tv.NamespaceID().String(),
		BusinessID:  s.tv.WorkflowID(),
		EntityID:    s.tv.RunID(),
	})

	// Execute InvocationTask with engine context
	err = s.invocationExecutor.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)
	s.NoError(err)

	// Close transaction to commit tasks to node backend
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to FAILED (not BACKING_OFF)
	s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
	s.NotNil(cb.LastAttemptFailure)
	s.NotNil(cb.LastAttemptCompleteTime)
}

// TestInvocationTask_ValidateStaleTask tests that validation rejects tasks with old attempt numbers.
func (s *invocationTasksSuite) TestInvocationTask_ValidateStaleTask() {
	// Create callback with attempt=5
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)
	cb.Attempt = 5

	ctx := s.newContext()

	// Try to validate task with old attempt number (3)
	valid, err := s.invocationExecutor.Validate(ctx, cb, chasm.TaskAttributes{}, &callbackspb.InvocationTask{
		Attempt: 3,
	})
	s.NoError(err)
	s.False(valid, "stale task should be rejected")
}

// TestInvocationTask_ValidateWrongState tests that validation rejects tasks when callback is in wrong state.
func (s *invocationTasksSuite) TestInvocationTask_ValidateWrongState() {
	// Create callback in SUCCEEDED state (terminal)
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SUCCEEDED)
	cb.Attempt = 1

	ctx := s.newContext()

	// Try to validate InvocationTask (expects SCHEDULED state)
	valid, err := s.invocationExecutor.Validate(ctx, cb, chasm.TaskAttributes{}, &callbackspb.InvocationTask{
		Attempt: 1,
	})
	s.NoError(err)
	s.False(valid, "task should be rejected when callback is not in SCHEDULED state")
}
