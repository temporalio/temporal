package parentclosepolicy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type parentClosePolicyWorkflowSuite struct {
	*require.Assertions
	suite.Suite
	testsuite.WorkflowTestSuite

	hostInfo          membership.HostInfo
	controller        *gomock.Controller
	mockClientBean    *client.MockBean
	mockHistoryClient *historyservicemock.MockHistoryServiceClient
	mockRemoteClient  *workflowservicemock.MockWorkflowServiceClient

	processor *Processor
}

func TestParentClosePolicyWorkflowSuite(t *testing.T) {
	s := new(parentClosePolicyWorkflowSuite)
	suite.Run(t, s)
}

func (s *parentClosePolicyWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.hostInfo = membership.NewHostInfoFromAddress("localhost")
	s.controller = gomock.NewController(s.T())
	s.mockClientBean = client.NewMockBean(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockRemoteClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)

	s.mockClientBean.EXPECT().GetHistoryClient().Return(s.mockHistoryClient).AnyTimes()
	s.mockClientBean.EXPECT().GetRemoteFrontendClient(gomock.Any()).Return(nil, s.mockRemoteClient, nil).AnyTimes()

	s.processor = &Processor{
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewNoopLogger(),
		cfg: Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1000),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1000),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(4),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(4),
			NumParentClosePolicySystemWorkflows:    dynamicconfig.GetIntPropertyFn(10),
		},
		clientBean: s.mockClientBean,
		hostInfo:   s.hostInfo,
	}
}

func (s *parentClosePolicyWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_SameCluster() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 3",
				RunID:       "childworkflow runID 3",
				Policy:      enumspb.PARENT_CLOSE_POLICY_ABANDON,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&historyservice.TerminateWorkflowExecutionResponse{}, nil).Times(1)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNotFound("")).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_RemoteCluster() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 1"}).Times(1)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 2"}).Times(1)
	s.mockRemoteClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *workflowservice.SignalWithStartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
			s.Equal(primitives.SystemLocalNamespace, request.Namespace)
			s.Equal(processorWFTypeName, request.WorkflowType.Name)
			s.Equal(processorTaskQueueName, request.TaskQueue.Name)
			s.Equal(workflowIDReusePolicy, request.WorkflowIdReusePolicy)
			s.Equal(processorChannelName, request.SignalName)
			return &workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil
		},
	).Times(2)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorWorkflow_NoRequest() {
	env := s.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	env.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})

	activityStarted := false
	env.SetOnActivityStartedListener(func(*activity.Info, context.Context, converter.EncodedValues) {
		activityStarted = true
	})

	env.ExecuteWorkflow(processorWFTypeName)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	s.False(activityStarted, "no request was signaled, so the processor activity must not be scheduled")
}

// TestProcessorActivity_NoParentExecution covers the backward compatible behavior: a request
// without a parent execution must not be scoped to children, otherwise terminate and cancel
// would fail with a mismatch error.
func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_NoParentExecution() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.TerminateWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.False(request.ChildWorkflowOnly)
			s.Nil(request.ExternalWorkflowExecution)
			s.Equal(tests.ChildNamespaceID.String(), request.NamespaceId)
			s.Equal(tests.ChildNamespace.String(), request.TerminateRequest.Namespace)
			s.Equal("child workflowID 1", request.TerminateRequest.WorkflowExecution.WorkflowId)
			s.Equal("childworkflow runID 1", request.TerminateRequest.FirstExecutionRunId)
			s.Equal(processorWFTypeName, request.TerminateRequest.Identity)
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		},
	).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_ChildWorkflowOnly() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "parent workflowID",
		RunId:      "parent runID",
	}
	request := Request{
		ParentExecution: parentExecution,
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			},
		},
	}

	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.RequestCancelWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			s.True(request.ChildWorkflowOnly)
			s.Equal(parentExecution.WorkflowId, request.ExternalWorkflowExecution.WorkflowId)
			s.Equal(parentExecution.RunId, request.ExternalWorkflowExecution.RunId)
			s.Equal(tests.ChildNamespaceID.String(), request.NamespaceId)
			s.Equal(tests.ChildNamespace.String(), request.CancelRequest.Namespace)
			s.Equal("child workflowID 1", request.CancelRequest.WorkflowExecution.WorkflowId)
			s.Equal("childworkflow runID 1", request.CancelRequest.FirstExecutionRunId)
			s.Equal(processorWFTypeName, request.CancelRequest.Identity)
			return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
		},
	).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_NamespaceNotFound() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
		},
	}

	// A namespace that is already gone must not fail the rest of the batch.
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNamespaceNotFound(tests.ChildNamespace.String())).Times(1)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&historyservice.TerminateWorkflowExecutionResponse{}, nil).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_UnexpectedErrorFailsActivity() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
		},
	}

	// The activity bails out on the first unexpected error so that the whole batch is retried.
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInternal("intentional test failure")).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.Error(err)
	s.ErrorContains(err, "intentional test failure")
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_RemoteCluster_OneSignalPerCluster() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 2",
				RunID:       "childworkflow runID 2",
				Policy:      enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 1"}).Times(1)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 1"}).Times(1)
	// Both executions belong to the same active cluster, so they are forwarded in one signal.
	s.mockRemoteClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *workflowservice.SignalWithStartWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
			var forwarded Request
			s.NoError(converter.GetDefaultDataConverter().FromPayloads(request.SignalInput, &forwarded))
			s.Len(forwarded.Executions, 2)
			s.Equal("child workflowID 1", forwarded.Executions[0].WorkflowID)
			s.Equal(enumspb.PARENT_CLOSE_POLICY_TERMINATE, forwarded.Executions[0].Policy)
			s.Equal("child workflowID 2", forwarded.Executions[1].WorkflowID)
			s.Equal(enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL, forwarded.Executions[1].Policy)
			return &workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil
		},
	).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_RemoteCluster_SignalFailure() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: "parent workflowID",
			RunId:      "parent runID",
		},
		Executions: []RequestDetail{
			{
				Namespace:   tests.ChildNamespace.String(),
				NamespaceID: tests.ChildNamespaceID.String(),
				WorkflowID:  "child workflowID 1",
				RunID:       "childworkflow runID 1",
				Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
			},
		},
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NamespaceNotActive{ActiveCluster: "remote cluster 1"}).Times(1)
	s.mockRemoteClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInternal("intentional test failure")).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.Error(err)
	s.ErrorContains(err, "intentional test failure")
}
