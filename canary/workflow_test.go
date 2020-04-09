package canary

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/temporal/mocks"
	"go.temporal.io/temporal/testsuite"
	"go.temporal.io/temporal/worker"
)

type (
	workflowTestSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		env *testsuite.TestWorkflowEnvironment
	}

	mockCadenceClient struct {
		client          *mocks.Client
		namespaceClient *mocks.NamespaceClient
	}
)

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(workflowTestSuite))
}

func (s *workflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()

	registerHistoryArchival(s.env)
	registerBatch(s.env)
	registerCancellation(s.env)
	registerConcurrentExec(s.env)
	registerCron(s.env)
	registerEcho(s.env)
	registerLocalActivity(s.env)
	registerQuery(s.env)
	registerReset(s.env)
	registerRetry(s.env)
	registerSanity(s.env)
	registerSearchAttributes(s.env)
	registerSignal(s.env)
	registerTimeout(s.env)
	registerVisibility(s.env)
}

func (s *workflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

func (s *workflowTestSuite) TestConcurrentExecWorkflow() {
	s.env.SetWorkerOptions(newTestWorkerOptions(newMockActivityContext(newMockCadenceClient())))
	s.env.ExecuteWorkflow(wfTypeConcurrentExec, time.Now().UnixNano(), "")
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *workflowTestSuite) TestQueryWorkflow() {
	// DOTO implement this test once client test framework
	// SetOnActivityStartedListener is guaranteed to
	// be executed and finished before activity started
}

func (s *workflowTestSuite) TestTimeout() {
	// DOTO implement this test once client test framework
	// can handle timeout error correctly
}

func (s *workflowTestSuite) TestSignalWorkflow() {
	mockClient := newMockCadenceClient()
	mockClient.client.On("SignalWorkflow",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		arg3 := args.Get(3).(string)
		arg4 := args.Get(4).(string)
		s.env.SignalWorkflow(arg3, arg4)
	}).Return(nil)
	s.env.SetWorkerOptions(newTestWorkerOptions(newMockActivityContext(mockClient)))
	s.env.ExecuteWorkflow(wfTypeSignal, time.Now().UnixNano(), "")
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *workflowTestSuite) TestEchoWorkflow() {
	s.env.SetWorkerOptions(newTestWorkerOptions(newMockActivityContext(newMockCadenceClient())))
	s.env.ExecuteWorkflow(wfTypeEcho, time.Now().UnixNano(), "")
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *workflowTestSuite) TestVisibilityWorkflow() {
	// uncomment below when race condition on s.env.SetOnActivityStartedListener is resolved

	// mockClient := newMockCadenceClient()
	// // setup the mock for visibility apis after the activity is invoked, because
	// // we need the workflow id and run id to construct a response for the mock
	// s.env.SetOnActivityStartedListener(func(activityInfo *temporal.ActivityInfo, ctx context.Context, args temporal.EncodedValues) {
	// 	if activityInfo.ActivityType.Name != activityTypeVisibility {
	// 		return
	// 	}
	// 	resp := newMockOpenWorkflowResponse(activityInfo.WorkflowExecution.ID, activityInfo.WorkflowExecution.RunID)
	// 	mockClient.client.On("ListOpenWorkflow", mock.Anything, mock.Anything).Return(resp, nil)
	// 	history := &eventpb.History{Events: []*eventpb.HistoryEvent{{}}}
	// 	mockClient.client.On("GetWorkflowHistory", mock.Anything, mock.Anything, mock.Anything).Return(history, nil)
	// })
	// s.env.SetWorkerOptions(newTestWorkerOptions(newMockActivityContext(mockClient)))
	// s.env.ExecuteWorkflow(wfTypeVisibility, time.Now().UnixNano(), &ServiceConfig{})
	// s.True(s.env.IsWorkflowCompleted())
	// s.NoError(s.env.GetWorkflowError())
}

func (s *workflowTestSuite) TestSanityWorkflow() {
	oldWFList := sanityChildWFList
	sanityChildWFList = []string{wfTypeEcho}
	s.env.ExecuteWorkflow(wfTypeSanity, time.Now().UnixNano(), "")
	sanityChildWFList = oldWFList
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *workflowTestSuite) TestLocalActivityWorkflow() {
	s.env.OnActivity(getConditionData).Return(int32(20), nil).Once()
	s.env.ExecuteWorkflow(localActivityWorkfow)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result string
	err := s.env.GetWorkflowResult(&result)
	s.NoError(err)
	s.Equal("data%2 == 0 and data%5 == 0", result)
}

func newTestWorkerOptions(ctx *activityContext) worker.Options {
	return worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), ctxKeyActivityRuntime, ctx),
	}
}

func newMockActivityContext(client mockCadenceClient) *activityContext {
	return &activityContext{
		cadence: cadenceClient{
			Client:          client.client,
			NamespaceClient: client.namespaceClient,
		},
	}
}

func newMockCadenceClient() mockCadenceClient {
	return mockCadenceClient{
		client:          new(mocks.Client),
		namespaceClient: new(mocks.NamespaceClient),
	}
}
