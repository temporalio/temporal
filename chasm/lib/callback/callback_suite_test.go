package callback_test

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"reflect"
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
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// callbackSuite sets up a suite that has a basic CHASM tree ready
// for use with the Callback library, integrated within a Workflow component.
type callbackSuite struct {
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

// SetupTest initializes the CHASM tree with workflow and callback components
func (s *callbackSuite) SetupTest() {
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
func (s *callbackSuite) setupDefaultDependencies() {
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
func (s *callbackSuite) createCallback(
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
func (s *callbackSuite) createDefaultCompletion() nexusrpc.OperationCompletion {
	completion, err := nexusrpc.NewOperationCompletionSuccessful(nil, nexusrpc.OperationCompletionSuccessfulOptions{})
	s.NoError(err)
	return completion
}

// NOTE: hasInvocationTask and hasBackoffTask are currently unused because task generation
// verification doesn't work in CHASM integration tests. See TODO comments in the tests that
// previously used these methods. Keeping these methods commented out for future use when CHASM
// provides proper unit testing hooks for task generation.

// hasInvocationTask checks if an InvocationTask with the given attempt was generated
// func (s *callbackSuite) hasInvocationTask(attempt int32) bool {
// 	for _, taskList := range s.nodeBackend.TasksByCategory {
// 		for _, task := range taskList {
// 			// CHASM tasks are stored as *tasks.ChasmTask with DeserializedTask field
// 			if chasmTask, ok := task.(*tasks.ChasmTask); ok {
// 				if chasmTask.DeserializedTask.IsValid() {
// 					if invTask, ok := chasmTask.DeserializedTask.Interface().(*callbackspb.InvocationTask); ok {
// 						if invTask.Attempt == attempt {
// 							return true
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return false
// }

// hasBackoffTask checks if a BackoffTask was generated at the given visibility time
// func (s *callbackSuite) hasBackoffTask(visibilityTime time.Time) bool {
// 	for _, taskList := range s.nodeBackend.TasksByCategory {
// 		for _, task := range taskList {
// 			// CHASM tasks are stored as *tasks.ChasmTask with DeserializedTask field
// 			if chasmTask, ok := task.(*tasks.ChasmTask); ok {
// 				if chasmTask.DeserializedTask.IsValid() {
// 					if _, ok := chasmTask.DeserializedTask.Interface().(*callbackspb.BackoffTask); ok {
// 						if task.GetVisibilityTime().Equal(visibilityTime) {
// 							return true
// 						}
// 					}
// 				}
// 			}
// 			// Also check for ChasmTaskPure (pure tasks)
// 			if chasmPureTask, ok := task.(*tasks.ChasmTaskPure); ok {
// 				// Pure tasks don't have deserialized tasks, but we can check visibility time
// 				// For now, just check if it's a BackoffTask by looking at the task type
// 				_ = chasmPureTask // BackoffTask should be a pure task
// 				if task.GetVisibilityTime().Equal(visibilityTime) {
// 					return true
// 				}
// 			}
// 		}
// 	}
// 	return false
// }

// newMutableContext creates a new mutable context for the node
func (s *callbackSuite) newMutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), s.node)
}

// newContext creates a new read-only context for the node
func (s *callbackSuite) newContext() chasm.Context {
	return chasm.NewContext(context.Background(), s.node)
}

// newEngineContext creates a context with the mock engine attached
func (s *callbackSuite) newEngineContext() context.Context {
	return chasm.NewEngineContext(context.Background(), s.mockEngine)
}

// ExpectReadComponent sets up mock expectation for ReadComponent to use the real component from the tree
func (s *callbackSuite) ExpectReadComponent(ctx chasm.Context, returnedComponent chasm.Component) {
	s.mockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			return readFn(ctx, returnedComponent)
		}).Times(1)
}

// ExpectUpdateComponent sets up mock expectation for UpdateComponent to use the real callback from the tree
func (s *callbackSuite) ExpectUpdateComponent(ctx chasm.MutableContext, componentToUpdate chasm.Component) {
	s.mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
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
