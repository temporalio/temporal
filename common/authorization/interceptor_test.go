package authorization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	testNamespace    string = "test-namespace"
	targetNamespace  string = "target-namespace"
	anotherNamespace string = "another-namespace"
)

var (
	ctx                           = context.Background()
	describeNamespaceRequest      = &workflowservice.DescribeNamespaceRequest{Namespace: testNamespace}
	describeNamespaceTarget       = &CallTarget{Namespace: testNamespace, Request: describeNamespaceRequest, APIName: "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"}
	describeNamespaceInfo         = &grpc.UnaryServerInfo{FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"}
	startWorkflowExecutionRequest = &workflowservice.StartWorkflowExecutionRequest{Namespace: testNamespace}
	startWorkflowExecutionTarget  = &CallTarget{Namespace: testNamespace, Request: startWorkflowExecutionRequest, APIName: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"}
	startWorkflowExecutionInfo    = &grpc.UnaryServerInfo{FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"}

	respondWorkflowTaskCompletedInfo = &grpc.UnaryServerInfo{FullMethod: api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted"}
)

type (
	authorizerInterceptorSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockAuthorizer     *MockAuthorizer
		mockMetricsHandler *metrics.MockHandler
		interceptor        *Interceptor
		handler            grpc.UnaryHandler
		mockClaimMapper    *MockClaimMapper
	}

	mockNamespaceChecker namespace.Name
)

func TestAuthorizerInterceptorSuite(t *testing.T) {
	s := new(authorizerInterceptorSuite)
	suite.Run(t, s)
}

func (s *authorizerInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockAuthorizer = NewMockAuthorizer(s.controller)
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(testNamespace),
	).Return(s.mockMetricsHandler).AnyTimes()
	s.mockMetricsHandler.EXPECT().Timer(metrics.ServiceAuthorizationLatency.Name()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	s.mockClaimMapper = NewMockClaimMapper(s.controller)
	s.interceptor = NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false), // exposeAuthorizerErrors
		dynamicconfig.GetBoolPropertyFn(false), // enableCrossNamespaceCommands
	)
	s.handler = func(ctx context.Context, req interface{}) (interface{}, error) { return true, nil }
}

func (s *authorizerInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *authorizerInterceptorSuite) TestIsAuthorized() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestIsAuthorizedWithNamespace() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, startWorkflowExecutionTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor.Intercept(ctx, startWorkflowExecutionRequest, startWorkflowExecutionInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestIsUnauthorized() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, nil)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrUnauthorizedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := s.interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestIsUnknown() {
	request := &workflowservice.DescribeNamespaceRequest{Namespace: "unknown-namespace"}
	target := &CallTarget{Namespace: "unknown-namespace", Request: request, APIName: "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"}
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, target).Return(Result{Decision: DecisionDeny}, nil)
	handler := metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceUnknownTag(), // note: should use unknown tag since unknown-namespace is not registered
	).Return(handler).AnyTimes()
	handler.EXPECT().Counter(metrics.ServiceErrUnauthorizedCounter.Name()).Return(metrics.NoopCounterMetricFunc)
	handler.EXPECT().Timer(metrics.ServiceAuthorizationLatency.Name()).Return(metrics.NoopTimerMetricFunc)

	res, err := s.interceptor.Intercept(ctx, request, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestAuthorizationFailed() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, errUnauthorized)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrAuthorizeFailedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := s.interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestAuthorizationFailedExposed() {
	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(true),  // exposeAuthorizerErrors
		dynamicconfig.GetBoolPropertyFn(false), // enableCrossNamespaceCommands
	)

	authErr := serviceerror.NewInternal("intentional test failure")
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, authErr)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrAuthorizeFailedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.ErrorIs(err, authErr)
}

func (s *authorizerInterceptorSuite) TestNoopClaimMapperWithoutTLS() {
	admin := &Claims{System: RoleAdmin}
	s.mockAuthorizer.EXPECT().Authorize(gomock.Any(), admin, describeNamespaceTarget).
		DoAndReturn(func(ctx context.Context, caller *Claims, target *CallTarget) (Result, error) {
			// check that claims are present in ctx also
			s.Equal(admin, ctx.Value(MappedClaims))

			return Result{Decision: DecisionAllow}, nil
		})

	interceptor := NewInterceptor(
		NewNoopClaimMapper(),
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false), // exposeAuthorizerErrors
		dynamicconfig.GetBoolPropertyFn(false), // enableCrossNamespaceCommands
	)
	_, err := interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestAlternateHeaders() {
	interceptor := NewInterceptor(
		s.mockClaimMapper,
		NewNoopAuthorizer(),
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"custom-header",
		"custom-extra-header",
		dynamicconfig.GetBoolPropertyFn(false), // exposeAuthorizerErrors
		dynamicconfig.GetBoolPropertyFn(false), // enableCrossNamespaceCommands
	)

	cases := []struct {
		md       metadata.MD
		authInfo *AuthInfo
	}{
		{
			metadata.Pairs(
				"custom-header", "the-token",
				"custom-extra-header", "more stuff",
			),
			&AuthInfo{
				AuthToken: "the-token",
				ExtraData: "more stuff",
			},
		},
		{
			metadata.Pairs(
				"custom-header", "the-token",
				"authorization-extras", "this gets ignored",
			),
			&AuthInfo{
				AuthToken: "the-token",
			},
		},
		{
			metadata.Pairs(
				"custom-extra-header", "missing main header, this gets ignored",
			),
			nil,
		},
	}
	for _, testCase := range cases {
		if testCase.authInfo != nil {
			s.mockClaimMapper.EXPECT().GetClaims(testCase.authInfo).Return(&Claims{System: RoleAdmin}, nil)
		}
		inCtx := metadata.NewIncomingContext(ctx, testCase.md)
		_, err := interceptor.Intercept(inCtx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
		s.NoError(err)
	}
}

func (n mockNamespaceChecker) Exists(name namespace.Name) error {
	if name == namespace.Name(n) {
		return nil
	}
	return errors.New("doesn't exist")
}

// multiNamespaceChecker is a mock that recognizes multiple namespaces
type multiNamespaceChecker []string

func (m multiNamespaceChecker) Exists(name namespace.Name) error {
	for _, ns := range m {
		if ns == string(name) {
			return nil
		}
	}
	return errors.New("doesn't exist")
}

// Helper to create a cross-namespace command
func makeCrossNamespaceCommand(commandType enumspb.CommandType, targetNs string) *commandpb.Command {
	switch commandType {
	case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		return &commandpb.Command{
			CommandType: commandType,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
				SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
					Namespace: targetNs,
				},
			},
		}
	case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		return &commandpb.Command{
			CommandType: commandType,
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
				StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace: targetNs,
				},
			},
		}
	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		return &commandpb.Command{
			CommandType: commandType,
			Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{
				RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					Namespace: targetNs,
				},
			},
		}
	default:
		return nil
	}
}

// Helper to create interceptor with cross-namespace commands enabled
func (s *authorizerInterceptorSuite) newCrossNamespaceInterceptor(namespaces ...string) *Interceptor {
	return NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		multiNamespaceChecker(namespaces),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false), // exposeAuthorizerErrors
		dynamicconfig.GetBoolPropertyFn(true),  // enableCrossNamespaceCommands
	)
}

func (s *authorizerInterceptorSuite) TestCrossNamespaceCommands_Authorized() {
	testCases := []struct {
		name        string
		commandType enumspb.CommandType
		expectedAPI string
	}{
		{
			name:        "SignalExternalWorkflow",
			commandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			expectedAPI: "SignalWorkflowExecution",
		},
		{
			name:        "StartChildWorkflow",
			commandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
			expectedAPI: "StartWorkflowExecution",
		},
		{
			name:        "CancelExternalWorkflow",
			commandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
			expectedAPI: "RequestCancelWorkflowExecution",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			request := &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: testNamespace,
				Commands:  []*commandpb.Command{makeCrossNamespaceCommand(tc.commandType, targetNamespace)},
			}

			sourceTarget := &CallTarget{
				Namespace: testNamespace,
				Request:   request,
				APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			}
			crossNsTarget := &CallTarget{
				Namespace: targetNamespace,
				Request:   request,
				APIName:   api.WorkflowServicePrefix + tc.expectedAPI,
			}

			interceptor := s.newCrossNamespaceInterceptor(testNamespace, targetNamespace)

			s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
				Return(Result{Decision: DecisionAllow}, nil)
			s.mockMetricsHandler.EXPECT().WithTags(
				metrics.OperationTag(metrics.AuthorizationScope),
				metrics.NamespaceTag(targetNamespace),
			).Return(s.mockMetricsHandler)
			s.mockAuthorizer.EXPECT().Authorize(ctx, nil, crossNsTarget).
				Return(Result{Decision: DecisionAllow}, nil)

			res, err := interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
			s.True(res.(bool))
			s.NoError(err)
		})
	}
}

func (s *authorizerInterceptorSuite) TestCrossNamespaceCommand_Unauthorized() {
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands:  []*commandpb.Command{makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, targetNamespace)},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}
	crossNsTarget := &CallTarget{
		Namespace: targetNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "SignalWorkflowExecution",
	}

	interceptor := s.newCrossNamespaceInterceptor(testNamespace, targetNamespace)

	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(targetNamespace),
	).Return(s.mockMetricsHandler)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, crossNsTarget).
		Return(Result{Decision: DecisionDeny}, nil)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrUnauthorizedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestNoExtraAuthCheck() {
	testCases := []struct {
		name        string
		targetNs    string
		description string
	}{
		{
			name:        "SameNamespace",
			targetNs:    testNamespace, // Same as source
			description: "command targeting same namespace should not trigger extra auth",
		},
		{
			name:        "EmptyNamespace",
			targetNs:    "", // Empty defaults to source
			description: "command with empty namespace should not trigger extra auth",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			request := &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: testNamespace,
				Commands:  []*commandpb.Command{makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, tc.targetNs)},
			}

			sourceTarget := &CallTarget{
				Namespace: testNamespace,
				Request:   request,
				APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			}

			// Only expect authorization for source namespace
			s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
				Return(Result{Decision: DecisionAllow}, nil)

			res, err := s.interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
			s.True(res.(bool))
			s.NoError(err)
		})
	}
}

func (s *authorizerInterceptorSuite) TestCrossNamespaceCommand_DisabledFeature() {
	// When cross-namespace commands are disabled, no extra auth check should happen
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands:  []*commandpb.Command{makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, targetNamespace)},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}

	// Interceptor with cross-namespace commands DISABLED (uses default s.interceptor which has it disabled)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestMultipleCommands_AuthDeduplication() {
	// Test that authorization is deduplicated per namespace+API combination
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, targetNamespace),
			makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION, targetNamespace),
			makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION, targetNamespace),
			// Duplicate signal to same namespace - should not trigger extra auth
			makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, targetNamespace),
		},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}

	interceptor := s.newCrossNamespaceInterceptor(testNamespace, targetNamespace)

	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	// Expect 3 auth checks (one per unique API type), not 4
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(targetNamespace),
	).Return(s.mockMetricsHandler).Times(3)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, &CallTarget{
		Namespace: targetNamespace, Request: request, APIName: api.WorkflowServicePrefix + "SignalWorkflowExecution",
	}).Return(Result{Decision: DecisionAllow}, nil)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, &CallTarget{
		Namespace: targetNamespace, Request: request, APIName: api.WorkflowServicePrefix + "StartWorkflowExecution",
	}).Return(Result{Decision: DecisionAllow}, nil)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, &CallTarget{
		Namespace: targetNamespace, Request: request, APIName: api.WorkflowServicePrefix + "RequestCancelWorkflowExecution",
	}).Return(Result{Decision: DecisionAllow}, nil)

	res, err := interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestMultipleTargetNamespaces() {
	// Test commands targeting different namespaces
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, targetNamespace),
			makeCrossNamespaceCommand(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION, anotherNamespace),
		},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}

	interceptor := s.newCrossNamespaceInterceptor(testNamespace, targetNamespace, anotherNamespace)

	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(targetNamespace),
	).Return(s.mockMetricsHandler)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, &CallTarget{
		Namespace: targetNamespace, Request: request, APIName: api.WorkflowServicePrefix + "SignalWorkflowExecution",
	}).Return(Result{Decision: DecisionAllow}, nil)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(anotherNamespace),
	).Return(s.mockMetricsHandler)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, &CallTarget{
		Namespace: anotherNamespace, Request: request, APIName: api.WorkflowServicePrefix + "StartWorkflowExecution",
	}).Return(Result{Decision: DecisionAllow}, nil)

	res, err := interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}
