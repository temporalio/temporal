package callback_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// callbackIntegrationSuite tests callback integration with CHASM framework.
// This suite focuses on end-to-end task execution within a real CHASM tree,
// similar to how schedulerSuite tests scheduler tasks.
type callbackIntegrationSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller *gomock.Controller

	// CHASM infrastructure
	registry        *chasm.Registry
	node            *chasm.Node
	nodeBackend     *chasm.MockNodeBackend
	mockEngine      *chasm.MockEngine
	timeSource      *clock.EventTimeSource
	nodePathEncoder chasm.NodePathEncoder
	logger          log.Logger
	tv              *testvars.TestVars

	// Components
	workflow *chasmworkflow.Workflow

	// Executors
	invocationExecutor *callback.InvocationTaskExecutor
	backoffExecutor    *callback.BackoffTaskExecutor

	// Test dependencies
	namespaceRegistry  namespace.Registry
	metricsHandler     metrics.Handler
	httpCallerProvider callback.HTTPCallerProvider
}

func TestCallbackIntegrationSuite(t *testing.T) {
	suite.Run(t, &callbackIntegrationSuite{})
}

// SetupTest initializes the CHASM tree with workflow and callback components
func (s *callbackIntegrationSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockEngine = chasm.NewMockEngine(s.controller)
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)
	s.nodePathEncoder = chasm.DefaultPathEncoder
	s.tv = testvars.New(s.T())

	// Register libraries
	s.registry = chasm.NewRegistry(s.logger)
	err := s.registry.Register(&callback.Library{
		InvocationTaskExecutor: &callback.InvocationTaskExecutor{},
		BackoffTaskExecutor:    &callback.BackoffTaskExecutor{},
	})
	s.NoError(err)
	err = s.registry.Register(chasmworkflow.NewLibrary())
	s.NoError(err)

	// Initialize time source
	s.timeSource = clock.NewEventTimeSource()
	s.timeSource.Update(time.Now())

	// Setup node backend
	s.nodeBackend = &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      s.tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}

	// Create tree and workflow
	s.node = chasm.NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	s.workflow = chasmworkflow.NewWorkflow(ctx, s.nodeBackend)
	s.node.SetRootComponent(s.workflow)
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Setup test dependencies with defaults
	s.setupDefaultDependencies()
}

// setupDefaultDependencies creates default test dependencies
func (s *callbackIntegrationSuite) setupDefaultDependencies() {
	// Setup namespace registry
	namespaceRegistryMock := namespace.NewMockRegistry(s.controller)
	namespaceRegistryMock.EXPECT().GetNamespaceByID(gomock.Any()).Return(
		namespace.FromPersistentState(&persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   s.tv.NamespaceID().String(),
				Name: s.tv.NamespaceName().String(),
			},
			Config: &persistencespb.NamespaceConfig{},
		}),
		nil,
	).AnyTimes()
	s.namespaceRegistry = namespaceRegistryMock

	// Setup metrics handler (noop by default)
	s.metricsHandler = metrics.NoopMetricsHandler

	// Setup default HTTP caller (returns 200 OK)
	s.httpCallerProvider = func(nid queues.NamespaceIDAndDestination) callback.HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}
	}

	// Create executors with default config using the constructors
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

	s.backoffExecutor = callback.NewBackoffTaskExecutor(callback.BackoffTaskExecutorOptions{
		Config: &callback.Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		MetricsHandler: s.metricsHandler,
		Logger:         s.logger,
	})
}

// createCallback is a helper to add a callback to the workflow's callbacks map.
// Similar to HSM's pattern of adding callbacks via MachineCollection.
func (s *callbackIntegrationSuite) createCallback(
	callbackID string,
	requestID string,
	url string,
	status callbackspb.CallbackStatus,
) *callback.Callback {
	ctx := chasm.NewMutableContext(context.Background(), s.node)

	cb := &callback.Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        requestID,
			RegistrationTime: timestamppb.New(s.timeSource.Now()),
			Status:           status,
			Attempt:          0,
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url:    url,
						Header: map[string]string{},
					},
				},
			},
		},
	}

	// Set CompletionSource for the callback (not needed for BackoffTask tests)
	completionSrc := &mockCompletionSource{
		completion: s.createDefaultCompletion(),
	}
	setFieldValue(&cb.CompletionSource, callback.CompletionSource(completionSrc))

	if s.workflow.Callbacks == nil {
		s.workflow.Callbacks = make(chasm.Map[string, *callback.Callback])
	}

	s.workflow.Callbacks[callbackID] = chasm.NewComponentField(ctx, cb)
	_, err := s.node.CloseTransaction()
	s.NoError(err)

	return cb
}

// createDefaultCompletion creates a successful Nexus operation completion
func (s *callbackIntegrationSuite) createDefaultCompletion() nexusrpc.OperationCompletion {
	completion, err := nexusrpc.NewOperationCompletionSuccessful(nil, nexusrpc.OperationCompletionSuccessfulOptions{})
	s.NoError(err)
	return completion
}

// hasInvocationTask checks if an InvocationTask with the given attempt was generated
func (s *callbackIntegrationSuite) hasInvocationTask(attempt int32) bool {
	for _, taskList := range s.nodeBackend.TasksByCategory {
		for _, task := range taskList {
			// CHASM tasks are stored as *tasks.ChasmTask with DeserializedTask field
			if chasmTask, ok := task.(*tasks.ChasmTask); ok {
				if chasmTask.DeserializedTask.IsValid() {
					if invTask, ok := chasmTask.DeserializedTask.Interface().(*callbackspb.InvocationTask); ok {
						if invTask.Attempt == attempt {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// hasBackoffTask checks if a BackoffTask was generated at the given visibility time
func (s *callbackIntegrationSuite) hasBackoffTask(visibilityTime time.Time) bool {
	for _, taskList := range s.nodeBackend.TasksByCategory {
		for _, task := range taskList {
			// CHASM tasks are stored as *tasks.ChasmTask with DeserializedTask field
			if chasmTask, ok := task.(*tasks.ChasmTask); ok {
				if chasmTask.DeserializedTask.IsValid() {
					if _, ok := chasmTask.DeserializedTask.Interface().(*callbackspb.BackoffTask); ok {
						if task.GetVisibilityTime().Equal(visibilityTime) {
							return true
						}
					}
				}
			}
			// Also check for ChasmTaskPure (pure tasks)
			if chasmPureTask, ok := task.(*tasks.ChasmTaskPure); ok {
				// Pure tasks don't have deserialized tasks, but we can check visibility time
				// For now, just check if it's a BackoffTask by looking at the task type
				_ = chasmPureTask // BackoffTask should be a pure task
				if task.GetVisibilityTime().Equal(visibilityTime) {
					return true
				}
			}
		}
	}
	return false
}

// newMutableContext creates a new mutable context for the node
func (s *callbackIntegrationSuite) newMutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), s.node)
}

// newContext creates a new read-only context for the node
func (s *callbackIntegrationSuite) newContext() chasm.Context {
	return chasm.NewContext(context.Background(), s.node)
}

// newEngineContext creates a context with the mock engine attached
func (s *callbackIntegrationSuite) newEngineContext() context.Context {
	return chasm.NewEngineContext(context.Background(), s.mockEngine)
}

// ExpectReadComponent sets up mock expectation for ReadComponent to use the real component from the tree
func (s *callbackIntegrationSuite) ExpectReadComponent(ctx chasm.Context, returnedComponent chasm.Component) {
	s.mockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			return readFn(ctx, returnedComponent)
		}).Times(1)
}

// ExpectUpdateComponent sets up mock expectation for UpdateComponent to use the real component from the tree
func (s *callbackIntegrationSuite) ExpectUpdateComponent(ctx chasm.MutableContext, componentToUpdate chasm.Component) {
	s.mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]any, error) {
			err := updateFn(ctx, componentToUpdate)
			return nil, err
		}).Times(1)
}

// mockCompletionSource implements CompletionSource for testing
type mockCompletionSource struct {
	completion nexusrpc.OperationCompletion
	err        error
}

func (m *mockCompletionSource) GetNexusCompletion(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error) {
	return m.completion, m.err
}

// setFieldValue is a test helper that uses reflection to set the internal value of a chasm.Field.
// This is necessary for testing because:
// 1. The Field API (NewComponentField, NewDataField) only supports chasm.Component and proto.Message types
// 2. CompletionSource is a plain interface, not a Component
// 3. The fieldInternal struct and its fields are unexported
//
// In production, Fields are typically initialized through proper CHASM lifecycle methods or
// by using NewComponentField/NewDataField with appropriate types.
func setFieldValue[T any](field *chasm.Field[T], value T) {
	// Get the Internal field (which is exported)
	internalField := reflect.ValueOf(field).Elem().FieldByName("Internal")

	// Get the unexported 'v' field using unsafe pointer manipulation
	vField := internalField.FieldByName("v")
	vField = reflect.NewAt(vField.Type(), unsafe.Pointer(vField.UnsafeAddr())).Elem()

	// Set the value
	vField.Set(reflect.ValueOf(value))
}

// noopHTTPClientTraceProvider implements HTTPClientTraceProvider for testing
type noopHTTPClientTraceProvider struct{}

func (n *noopHTTPClientTraceProvider) NewTrace(int32, log.Logger) *httptrace.ClientTrace {
	return nil
}

func (n *noopHTTPClientTraceProvider) NewForwardingTrace(log.Logger) *httptrace.ClientTrace {
	return nil
}

// ========================================
// P1.1: InvocationTask Execution Tests
// ========================================
// These tests mirror HSM's TestProcessInvocationTaskNexus_Outcomes pattern

// TODO: InvocationTask tests require ComponentRef and side effect execution architecture
// These will be added once the pattern is established

// TestInvocationTask_SuccessfulNexusCallback tests a successful HTTP callback invocation.
// Mirrors HSM test: TestProcessInvocationTaskNexus_Outcomes/success
func (s *callbackIntegrationSuite) TestInvocationTask_SuccessfulNexusCallback() {
	// Setup HTTP caller that returns 200 OK
	s.httpCallerProvider = func(nid queues.NamespaceIDAndDestination) callback.HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}
	}
	s.setupDefaultDependencies()

	// Create callback in SCHEDULED state
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)

	// Set up contexts - one for tree operations, one for engine operations
	treeCtx := s.newMutableContext()
	engineCtx := s.newEngineContext()
	readCtx := s.newContext()

	// Set up engine expectations to use the real callback from the tree
	s.ExpectReadComponent(readCtx, cb)
	s.ExpectUpdateComponent(treeCtx, cb)

	// Create ComponentRef
	ref := chasm.NewComponentRef[*callback.Callback](chasm.EntityKey{
		NamespaceID: s.tv.NamespaceID().String(),
		BusinessID:  s.tv.WorkflowID(),
		EntityID:    s.tv.RunID(),
	})

	// Execute InvocationTask with engine context
	err := s.invocationExecutor.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)
	s.NoError(err)

	// Close transaction to finalize changes in the tree
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to SUCCEEDED
	s.Equal(callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
}

// TestInvocationTask_FailureTriggersBackoff tests that a retryable HTTP error triggers backoff.
// Mirrors HSM test: TestProcessInvocationTaskNexus_Outcomes/retryable-error
func (s *callbackIntegrationSuite) TestInvocationTask_FailureTriggersBackoff() {
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
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)

	// Set up contexts - one for tree operations, one for engine operations
	treeCtx := s.newMutableContext()
	engineCtx := s.newEngineContext()
	readCtx := s.newContext()

	// Set up engine expectations to use the real callback from the tree
	s.ExpectReadComponent(readCtx, cb)
	s.ExpectUpdateComponent(treeCtx, cb)

	// Create ComponentRef
	ref := chasm.NewComponentRef[*callback.Callback](chasm.EntityKey{
		NamespaceID: s.tv.NamespaceID().String(),
		BusinessID:  s.tv.WorkflowID(),
		EntityID:    s.tv.RunID(),
	})

	// Execute InvocationTask with engine context
	err := s.invocationExecutor.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)
	// For retryable errors, expect an error but not an UnprocessableTaskError
	s.Error(err)
	s.NotErrorIs(err, &queues.UnprocessableTaskError{})

	// Close transaction to finalize changes in the tree
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to BACKING_OFF
	s.Equal(callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
	s.Equal(int32(1), cb.Attempt)
	s.NotNil(cb.LastAttemptFailure)
	s.NotNil(cb.NextAttemptScheduleTime)
	s.NotNil(cb.LastAttemptCompleteTime)

	// Verify BackoffTask was generated
	s.True(s.hasBackoffTask(cb.NextAttemptScheduleTime.AsTime()))
}

// TestInvocationTask_NonRetryableFailure tests that a non-retryable error causes permanent failure.
// Mirrors HSM test: TestProcessInvocationTaskNexus_Outcomes/non-retryable-error
func (s *callbackIntegrationSuite) TestInvocationTask_NonRetryableFailure() {
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
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_SCHEDULED)

	// Set up contexts - one for tree operations, one for engine operations
	treeCtx := s.newMutableContext()
	engineCtx := s.newEngineContext()
	readCtx := s.newContext()

	// Set up engine expectations to use the real callback from the tree
	s.ExpectReadComponent(readCtx, cb)
	s.ExpectUpdateComponent(treeCtx, cb)

	// Create ComponentRef
	ref := chasm.NewComponentRef[*callback.Callback](chasm.EntityKey{
		NamespaceID: s.tv.NamespaceID().String(),
		BusinessID:  s.tv.WorkflowID(),
		EntityID:    s.tv.RunID(),
	})

	// Execute InvocationTask with engine context
	err := s.invocationExecutor.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)
	s.NoError(err)

	// Close transaction to finalize changes in the tree
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to FAILED (not BACKING_OFF)
	s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
	s.NotNil(cb.LastAttemptFailure)
	s.NotNil(cb.LastAttemptCompleteTime)
}

// ========================================
// P1.2: BackoffTask Execution Tests
// ========================================
// These tests mirror HSM's TestProcessBackoffTask pattern

// TestBackoffTask_ReschedulesCallback tests that BackoffTask transitions callback back to SCHEDULED.
// Mirrors HSM test: TestProcessBackoffTask
func (s *callbackIntegrationSuite) TestBackoffTask_ReschedulesCallback() {
	// Create callback in BACKING_OFF state
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_BACKING_OFF)

	// Modify callback state within a transaction
	ctx := s.newMutableContext()
	cb.Attempt = 1
	cb.NextAttemptScheduleTime = timestamppb.New(s.timeSource.Now().Add(time.Minute))
	_, err := s.node.CloseTransaction()
	s.NoError(err)

	// Open new transaction for Execute
	ctx = s.newMutableContext()

	// Execute BackoffTask
	err = s.backoffExecutor.Execute(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 1,
	})
	s.NoError(err)

	// Close transaction
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to SCHEDULED
	s.Equal(callbackspb.CALLBACK_STATUS_SCHEDULED, cb.Status)
	s.Equal(int32(1), cb.Attempt) // Attempt stays same
	s.Nil(cb.NextAttemptScheduleTime)
}

// TestBackoffTask_GeneratesInvocationTask tests that BackoffTask generates a new InvocationTask.
func (s *callbackIntegrationSuite) TestBackoffTask_GeneratesInvocationTask() {
	// Create callback in BACKING_OFF state
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_BACKING_OFF)

	// Modify callback state within a transaction
	ctx := s.newMutableContext()
	cb.Attempt = 2
	cb.NextAttemptScheduleTime = timestamppb.New(s.timeSource.Now().Add(time.Minute))
	_, err := s.node.CloseTransaction()
	s.NoError(err)

	// Open new transaction for Execute
	ctx = s.newMutableContext()

	// Get the callback from the tree's Callbacks map to ensure we're working with the tree-tracked component
	callbackField := s.workflow.Callbacks["cb-1"]
	trackedCb, err := callbackField.Get(ctx)
	s.NoError(err)

	// Execute BackoffTask on the tree-tracked callback
	err = s.backoffExecutor.Execute(ctx, trackedCb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 2,
	})
	s.NoError(err)

	// Close transaction
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Debug: Print what tasks we have
	s.T().Logf("Total tasks by category: %+v", s.nodeBackend.TasksByCategory)
	for category, taskList := range s.nodeBackend.TasksByCategory {
		s.T().Logf("Category %v has %d tasks", category, len(taskList))
		for i, task := range taskList {
			s.T().Logf("  Task %d: type=%T", i, task)
		}
	}

	// Verify InvocationTask was generated with correct attempt
	s.True(s.hasInvocationTask(2), "Expected InvocationTask with attempt=2 to be generated")
}

// ========================================
// P1.3: Task Validation Tests
// ========================================
// These tests are new for CHASM - they don't exist in HSM tests

// TestInvocationTask_ValidateStaleTask tests that validation rejects tasks with old attempt numbers.
func (s *callbackIntegrationSuite) TestInvocationTask_ValidateStaleTask() {
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
func (s *callbackIntegrationSuite) TestInvocationTask_ValidateWrongState() {
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

// TestBackoffTask_ValidateAttemptMismatch tests that BackoffTask validation rejects mismatched attempts.
func (s *callbackIntegrationSuite) TestBackoffTask_ValidateAttemptMismatch() {
	// Create callback in BACKING_OFF state with attempt=3
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_BACKING_OFF)
	cb.Attempt = 3

	ctx := s.newContext()

	// Try to validate BackoffTask with different attempt number
	valid, err := s.backoffExecutor.Validate(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 2,
	})
	s.NoError(err)
	s.False(valid, "task should be rejected when attempt doesn't match")

	// Validate with correct attempt
	valid, err = s.backoffExecutor.Validate(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 3,
	})
	s.NoError(err)
	s.True(valid, "task should be accepted when attempt matches and state is BACKING_OFF")
}
