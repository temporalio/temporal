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
	testNamespace       string = "test-namespace"
	targetNamespace     string = "target-namespace"
	anotherNamespace    string = "another-namespace"
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
		dynamicconfig.GetBoolPropertyFn(false),
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
		dynamicconfig.GetBoolPropertyFn(true),
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
		dynamicconfig.GetBoolPropertyFn(false),
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
		dynamicconfig.GetBoolPropertyFn(false),
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

func (s *authorizerInterceptorSuite) TestCrossNamespaceSignalExternalWorkflow_Authorized() {
	// Create request with cross-namespace signal command
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
					SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
						Namespace: targetNamespace,
					},
				},
			},
		},
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

	// Setup interceptor with multi-namespace checker
	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		multiNamespaceChecker{testNamespace, targetNamespace},
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)

	// Expect authorization for source namespace
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	// Expect authorization for target namespace
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(targetNamespace),
	).Return(s.mockMetricsHandler)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, crossNsTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestCrossNamespaceSignalExternalWorkflow_Unauthorized() {
	// Create request with cross-namespace signal command
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
					SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
						Namespace: targetNamespace,
					},
				},
			},
		},
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

	// Setup interceptor with multi-namespace checker
	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		multiNamespaceChecker{testNamespace, targetNamespace},
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)

	// Expect authorization for source namespace (allowed)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	// Expect authorization for target namespace (denied)
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

func (s *authorizerInterceptorSuite) TestCrossNamespaceStartChildWorkflow_Authorized() {
	// Create request with cross-namespace start child workflow command
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace: targetNamespace,
					},
				},
			},
		},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}
	crossNsTarget := &CallTarget{
		Namespace: targetNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "StartWorkflowExecution",
	}

	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		multiNamespaceChecker{testNamespace, targetNamespace},
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)

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
}

func (s *authorizerInterceptorSuite) TestCrossNamespaceCancelExternalWorkflow_Authorized() {
	// Create request with cross-namespace cancel external workflow command
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{
					RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
						Namespace: targetNamespace,
					},
				},
			},
		},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}
	crossNsTarget := &CallTarget{
		Namespace: targetNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RequestCancelWorkflowExecution",
	}

	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		multiNamespaceChecker{testNamespace, targetNamespace},
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)

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
}

func (s *authorizerInterceptorSuite) TestSameNamespaceCommand_NoExtraAuthCheck() {
	// Create request with command targeting the same namespace
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
					SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
						Namespace: testNamespace, // Same as source namespace
					},
				},
			},
		},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}

	// Only expect authorization for source namespace, not for target (since they're the same)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestEmptyNamespaceCommand_NoExtraAuthCheck() {
	// Create request with command with empty namespace (defaults to source)
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
					SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
						Namespace: "", // Empty namespace
					},
				},
			},
		},
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
}

func (s *authorizerInterceptorSuite) TestMultipleCrossNamespaceCommands_Authorized() {
	// Create request with multiple cross-namespace commands
	request := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
					SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
						Namespace: targetNamespace,
					},
				},
			},
			{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace: anotherNamespace,
					},
				},
			},
		},
	}

	sourceTarget := &CallTarget{
		Namespace: testNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
	}
	signalTarget := &CallTarget{
		Namespace: targetNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "SignalWorkflowExecution",
	}
	childTarget := &CallTarget{
		Namespace: anotherNamespace,
		Request:   request,
		APIName:   api.WorkflowServicePrefix + "StartWorkflowExecution",
	}

	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		multiNamespaceChecker{testNamespace, targetNamespace, anotherNamespace},
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)

	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, sourceTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(targetNamespace),
	).Return(s.mockMetricsHandler)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, signalTarget).
		Return(Result{Decision: DecisionAllow}, nil)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(anotherNamespace),
	).Return(s.mockMetricsHandler)
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, childTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := interceptor.Intercept(ctx, request, respondWorkflowTaskCompletedInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}
