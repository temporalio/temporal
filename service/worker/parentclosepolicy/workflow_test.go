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

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_UnexpectedError() {
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
		Return(nil, serviceerror.NewInternal("random error")).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.Error(err)
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorActivity_NoParentExecution() {
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(getWorkerOptions(s.processor))
	env.RegisterActivity(ProcessorActivity)

	request := Request{
		ParentExecution: &commonpb.WorkflowExecution{},
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
			// backward compatibility: without a parent execution, child-workflow-only
			// verification must be disabled or the request would fail with a mismatch error
			s.False(request.ChildWorkflowOnly)
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		},
	).Times(1)

	_, err := env.ExecuteActivity(ProcessorActivity, request)
	s.NoError(err)
}

// registerProcessor registers ProcessorWorkflow and ProcessorActivity the same
// way Processor.Start does: by their custom names, with the worker options that
// carry the processor in the background activity context. This exercises the
// full workflow -> activity wiring.
func (s *parentClosePolicyWorkflowSuite) registerProcessor(env *testsuite.TestWorkflowEnvironment) {
	workerOptions := getWorkerOptions(s.processor)
	// The test environment re-applies worker option defaults for every activity
	// task handler it creates. The first application installs a Tuner derived from
	// these sizes, and the next one panics because both are then set. Clear the
	// sizes to keep multi-activity workflow tests working.
	workerOptions.MaxConcurrentActivityExecutionSize = 0
	workerOptions.MaxConcurrentWorkflowTaskExecutionSize = 0
	env.SetWorkerOptions(workerOptions)
	env.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	env.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorWorkflow_ProcessesSignaledRequests() {
	env := s.NewTestWorkflowEnvironment()
	s.registerProcessor(env)

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

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *historyservice.TerminateWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.Equal(tests.ChildNamespaceID.String(), request.NamespaceId)
			s.Equal("child workflowID 1", request.TerminateRequest.WorkflowExecution.WorkflowId)
			s.Equal("childworkflow runID 1", request.TerminateRequest.FirstExecutionRunId)
			s.True(request.ChildWorkflowOnly)
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		},
	).Times(1)

	// zero delay delivers the signal before the workflow runs, simulating signal-with-start
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(processorChannelName, request)
	}, 0)

	env.ExecuteWorkflow(processorWFTypeName)
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorWorkflow_DrainsAllPendingRequests() {
	env := s.NewTestWorkflowEnvironment()
	s.registerProcessor(env)

	newRequest := func(workflowID string) Request {
		return Request{
			ParentExecution: &commonpb.WorkflowExecution{
				WorkflowId: "parent workflowID",
				RunId:      "parent runID",
			},
			Executions: []RequestDetail{
				{
					Namespace:   tests.ChildNamespace.String(),
					NamespaceID: tests.ChildNamespaceID.String(),
					WorkflowID:  workflowID,
					RunID:       "childworkflow runID",
					Policy:      enumspb.PARENT_CLOSE_POLICY_TERMINATE,
				},
			},
		}
	}

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&historyservice.TerminateWorkflowExecutionResponse{}, nil).Times(2)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(processorChannelName, newRequest("child workflowID 1"))
		env.SignalWorkflow(processorChannelName, newRequest("child workflowID 2"))
	}, 0)

	env.ExecuteWorkflow(processorWFTypeName)
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *parentClosePolicyWorkflowSuite) TestProcessorWorkflow_CompletesDespiteActivityFailure() {
	env := s.NewTestWorkflowEnvironment()
	s.registerProcessor(env)

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

	// first attempt fails, retry succeeds per the activity retry policy
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInternal("transient error")).Times(1)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&historyservice.TerminateWorkflowExecutionResponse{}, nil).Times(1)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(processorChannelName, request)
	}, 0)

	env.ExecuteWorkflow(processorWFTypeName)
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}
