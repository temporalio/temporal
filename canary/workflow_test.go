// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package canary

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/mocks"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
)

type (
	workflowTestSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		env *testsuite.TestWorkflowEnvironment
	}

	mockCadenceClient struct {
		client       *mocks.Client
		domainClient *mocks.DomainClient
	}
)

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(workflowTestSuite))
}

func (s *workflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
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
	// s.env.SetOnActivityStartedListener(func(activityInfo *cadence.ActivityInfo, ctx context.Context, args cadence.EncodedValues) {
	// 	if activityInfo.ActivityType.Name != activityTypeVisibility {
	// 		return
	// 	}
	// 	resp := newMockOpenWorkflowResponse(activityInfo.WorkflowExecution.ID, activityInfo.WorkflowExecution.RunID)
	// 	mockClient.client.On("ListOpenWorkflow", mock.Anything, mock.Anything).Return(resp, nil)
	// 	history := &shared.History{Events: []*shared.HistoryEvent{{}}}
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

func newMockOpenWorkflowResponse(wfID string, runID string) *shared.ListOpenWorkflowExecutionsResponse {
	return &shared.ListOpenWorkflowExecutionsResponse{
		Executions: []*shared.WorkflowExecutionInfo{
			{Execution: &shared.WorkflowExecution{WorkflowId: &wfID, RunId: &runID}},
		},
	}
}

func newTestWorkerOptions(ctx *activityContext) worker.Options {
	return worker.Options{
		MetricsScope:              tally.NoopScope,
		BackgroundActivityContext: context.WithValue(context.Background(), ctxKeyActivityRuntime, ctx),
	}
}

func newMockActivityContext(client mockCadenceClient) *activityContext {
	return &activityContext{
		cadence: cadenceClient{
			Client:       client.client,
			DomainClient: client.domainClient,
		},
	}
}

func newMockCadenceClient() mockCadenceClient {
	return mockCadenceClient{
		client:       new(mocks.Client),
		domainClient: new(mocks.DomainClient),
	}
}
