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
	"go.temporal.io/sdk/testsuite"
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
