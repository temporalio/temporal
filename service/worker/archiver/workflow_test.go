// Copyright (c) 2017 Uber Technologies, Inc.
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

package archiver

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

var (
	workflowTestMetricsClient *mmocks.Client
	workflowTestMetricsScope  *mmocks.Scope
	workflowTestLogger        log.Logger
	workflowTestHandler       *MockHandler
	workflowTestPump          *PumpMock
	workflowTestConfig        *Config
)

type workflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func (s *workflowSuite) SetupSuite() {
	workflow.Register(archiveHistoryWorkflowTest)
}

func TestWorkflowSuite(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	workflowTestMetricsClient = &mmocks.Client{}
	workflowTestMetricsScope = &mmocks.Scope{}
	workflowTestLogger = loggerimpl.NewLogger(zap.NewNop())
	workflowTestHandler = &MockHandler{}
	workflowTestPump = &PumpMock{}
	workflowTestConfig = &Config{
		ArchiverConcurrency:           dynamicconfig.GetIntPropertyFn(0),
		ArchivalsPerIteration:         dynamicconfig.GetIntPropertyFn(0),
		TimeLimitPerArchivalIteration: dynamicconfig.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}
}

func (s *workflowSuite) TearDownTest() {
	workflowTestMetricsClient.AssertExpectations(s.T())
	workflowTestMetricsScope.AssertExpectations(s.T())
	workflowTestHandler.AssertExpectations(s.T())
	workflowTestPump.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchiveHistoryWorkflow_Fail_HashesDoNotEqual() {
	workflowTestMetricsClient.On("Scope", metrics.HistoryArchivalWorkflowScope, mock.Anything).Return(workflowTestMetricsScope).Once()
	workflowTestMetricsScope.On("IncCounter", metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetricsScope.On("StartTimer", metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).Once()
	workflowTestMetricsScope.On("StartTimer", metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NewTestStopwatch()).Once()
	workflowTestMetricsScope.On("AddCounter", metrics.ArchiverNumPumpedRequestsCount, int64(3)).Once()
	workflowTestMetricsScope.On("AddCounter", metrics.ArchiverNumHandledRequestsCount, int64(3)).Once()
	workflowTestMetricsScope.On("IncCounter", metrics.ArchiverPumpedNotEqualHandledCount).Once()
	workflowTestHandler.On("Start").Once()
	workflowTestHandler.On("Finished").Return([]uint64{9, 7, 0}).Once()
	workflowTestPump.On("Run").Return(PumpResult{
		PumpedHashes: []uint64{8, 7, 0},
	}).Once()

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archiveHistoryWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	_, ok := env.GetWorkflowError().(*workflow.ContinueAsNewError)
	s.True(ok, "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchiveHistoryWorkflow_Exit_TimeoutWithoutSignals() {
	workflowTestMetricsClient.On("Scope", metrics.HistoryArchivalWorkflowScope, mock.Anything).Return(workflowTestMetricsScope).Once()
	workflowTestMetricsScope.On("IncCounter", metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetricsScope.On("StartTimer", metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).Once()
	workflowTestMetricsScope.On("StartTimer", metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NewTestStopwatch()).Once()
	workflowTestMetricsScope.On("AddCounter", metrics.ArchiverNumPumpedRequestsCount, int64(0)).Once()
	workflowTestMetricsScope.On("AddCounter", metrics.ArchiverNumHandledRequestsCount, int64(0)).Once()
	workflowTestMetricsScope.On("IncCounter", metrics.ArchiverWorkflowStoppingCount).Once()
	workflowTestHandler.On("Start").Once()
	workflowTestHandler.On("Finished").Return([]uint64{}).Once()
	workflowTestPump.On("Run").Return(PumpResult{
		PumpedHashes:          []uint64{},
		TimeoutWithoutSignals: true,
	}).Once()

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archiveHistoryWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchiveHistoryWorkflow_Success() {
	workflowTestMetricsClient.On("Scope", metrics.HistoryArchivalWorkflowScope, mock.Anything).Return(workflowTestMetricsScope).Once()
	workflowTestMetricsScope.On("IncCounter", metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetricsScope.On("StartTimer", metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).Once()
	workflowTestMetricsScope.On("StartTimer", metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NewTestStopwatch()).Once()
	workflowTestMetricsScope.On("AddCounter", metrics.ArchiverNumPumpedRequestsCount, int64(5)).Once()
	workflowTestMetricsScope.On("AddCounter", metrics.ArchiverNumHandledRequestsCount, int64(5)).Once()
	workflowTestHandler.On("Start").Once()
	workflowTestHandler.On("Finished").Return([]uint64{1, 2, 3, 4, 5}).Once()
	workflowTestPump.On("Run").Return(PumpResult{
		PumpedHashes: []uint64{1, 2, 3, 4, 5},
	}).Once()

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archiveHistoryWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	_, ok := env.GetWorkflowError().(*workflow.ContinueAsNewError)
	s.True(ok, "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestReplayArchiveHistoryWorkflow() {
	logger, _ := zap.NewDevelopment()
	globalLogger = workflowTestLogger
	globalMetricsClient = metrics.NewClient(tally.NewTestScope("replay", nil), metrics.Worker)
	globalConfig = &Config{
		ArchiverConcurrency:           dynamicconfig.GetIntPropertyFn(50),
		ArchivalsPerIteration:         dynamicconfig.GetIntPropertyFn(1000),
		TimeLimitPerArchivalIteration: dynamicconfig.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}
	err := worker.ReplayWorkflowHistoryFromJSONFile(logger, "testdata/archive_history_workflow_history_v1.json")
	s.NoError(err)
}

func archiveHistoryWorkflowTest(ctx workflow.Context) error {
	return archivalWorkflowHelper(
		ctx,
		workflowTestLogger,
		workflowTestMetricsClient,
		workflowTestConfig,
		workflowTestHandler,
		workflowTestPump,
		nil,
		GetHistoryRequestReceiver(),
		NewHistoryRequestProcessor(workflowTestLogger, workflowTestMetricsScope),
	)
}
