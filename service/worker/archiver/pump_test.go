// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var (
	pumpTestMetrics *metrics.MockClient
	pumpTestLogger  *log.MockLogger
)

type pumpSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
}

func TestPumpSuite(t *testing.T) {
	suite.Run(t, new(pumpSuite))
}

func (s *pumpSuite) registerWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflow(carryoverSatisfiesLimitWorkflow)
	env.RegisterWorkflow(pumpWorkflow)
	env.RegisterWorkflow(signalChClosePumpWorkflow)
	env.RegisterWorkflow(signalAndCarryoverPumpWorkflow)
}

func (s *pumpSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	pumpTestMetrics = metrics.NewMockClient(s.controller)
	pumpTestMetrics.EXPECT().StartTimer(gomock.Any(), gomock.Any()).Return(metrics.NoopStopwatch)
	pumpTestLogger = log.NewMockLogger(s.controller)
}

func (s *pumpSuite) TearDownTest() {
}

func (s *pumpSuite) TestPumpRun_CarryoverLargerThanLimit() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(1))

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(carryoverSatisfiesLimitWorkflow, 10, 11)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *pumpSuite) TestPumpRun_CarryoverExactlyMatchesLimit() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(0))

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(carryoverSatisfiesLimitWorkflow, 10, 10)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *pumpSuite) TestPumpRun_TimeoutWithoutSignals() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(0))
	pumpTestMetrics.EXPECT().IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpTimeoutCount)
	pumpTestMetrics.EXPECT().IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpTimeoutWithoutSignalsCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(pumpWorkflow, 10, 0)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *pumpSuite) TestPumpRun_TimeoutWithSignals() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(0))
	pumpTestMetrics.EXPECT().IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpTimeoutCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(pumpWorkflow, 10, 5)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *pumpSuite) TestPumpRun_SignalsGottenSatisfyLimit() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(0))
	pumpTestMetrics.EXPECT().IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpSignalThresholdCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(pumpWorkflow, 10, 10)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *pumpSuite) TestPumpRun_SignalsAndCarryover() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(0))
	pumpTestMetrics.EXPECT().IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpSignalThresholdCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(signalAndCarryoverPumpWorkflow, 10, 5, 5)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *pumpSuite) TestPumpRun_SignalChannelClosedUnexpectedly() {
	pumpTestMetrics.EXPECT().UpdateGauge(metrics.ArchiverPumpScope, metrics.ArchiverBacklogSizeGauge, float64(0))
	pumpTestMetrics.EXPECT().IncCounter(metrics.ArchiverPumpScope, metrics.ArchiverPumpSignalChannelClosedCount)
	pumpTestLogger.EXPECT().Error(gomock.Any(), gomock.Any())

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(signalChClosePumpWorkflow, 10, 5)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func carryoverSatisfiesLimitWorkflow(ctx workflow.Context, requestLimit int, carryoverSize int) error {
	unhandledCarryoverSize := carryoverSize - requestLimit
	carryover, carryoverHashes := randomCarryover(carryoverSize)
	requestCh := workflow.NewBufferedChannel(ctx, requestLimit)
	pump := NewPump(ctx, pumpTestLogger, pumpTestMetrics, carryover, time.Nanosecond, requestLimit, requestCh, nil)
	actual := pump.Run()
	expected := PumpResult{
		PumpedHashes:          carryoverHashes[:len(carryoverHashes)-unhandledCarryoverSize],
		UnhandledCarryover:    carryover[len(carryover)-unhandledCarryoverSize:],
		TimeoutWithoutSignals: false,
	}
	if !pumpResultsEqual(expected, actual) {
		return errors.New("did not get expected pump result")
	}
	if !channelContainsExpected(ctx, requestCh, carryover[:len(carryover)-unhandledCarryoverSize]) {
		return errors.New("request channel was not populated with expected values")
	}
	return nil
}

func pumpWorkflow(ctx workflow.Context, requestLimit int, numRequests int) error {
	signalCh := workflow.NewBufferedChannel(ctx, requestLimit)
	signalsSent, signalHashes := sendRequestsToChannel(ctx, signalCh, numRequests)
	requestCh := workflow.NewBufferedChannel(ctx, requestLimit)
	pump := NewPump(ctx, pumpTestLogger, pumpTestMetrics, nil, time.Nanosecond, requestLimit, requestCh, signalCh)
	actual := pump.Run()
	expected := PumpResult{
		PumpedHashes:          signalHashes,
		UnhandledCarryover:    nil,
		TimeoutWithoutSignals: numRequests == 0,
	}
	if !pumpResultsEqual(expected, actual) {
		return errors.New("did not get expected pump result")
	}
	if !channelContainsExpected(ctx, requestCh, signalsSent) {
		return errors.New("request channel was not populated with expected values")
	}
	return nil
}

func signalChClosePumpWorkflow(ctx workflow.Context, requestLimit int, numRequests int) error {
	signalCh := workflow.NewBufferedChannel(ctx, requestLimit)
	signalsSent, signalHashes := sendRequestsToChannelBlocking(ctx, signalCh, numRequests)
	signalCh.Close()
	requestCh := workflow.NewBufferedChannel(ctx, requestLimit)
	pump := NewPump(ctx, pumpTestLogger, pumpTestMetrics, nil, time.Nanosecond, requestLimit, requestCh, signalCh)
	actual := pump.Run()
	expected := PumpResult{
		PumpedHashes:          signalHashes,
		UnhandledCarryover:    nil,
		TimeoutWithoutSignals: numRequests == 0,
	}
	if !pumpResultsEqual(expected, actual) {
		return errors.New("did not get expected pump result")
	}
	if !channelContainsExpected(ctx, requestCh, signalsSent) {
		return errors.New("request channel was not populated with expected values")
	}
	return nil
}

func signalAndCarryoverPumpWorkflow(ctx workflow.Context, requestLimit int, carryoverSize, numSignals int) error {
	signalCh := workflow.NewBufferedChannel(ctx, requestLimit)
	signalsSent, signalHashes := sendRequestsToChannel(ctx, signalCh, numSignals)
	carryover, carryoverHashes := randomCarryover(carryoverSize)
	requestCh := workflow.NewBufferedChannel(ctx, requestLimit)
	pump := NewPump(ctx, pumpTestLogger, pumpTestMetrics, carryover, time.Nanosecond, requestLimit, requestCh, signalCh)
	actual := pump.Run()
	expected := PumpResult{
		PumpedHashes:          append(carryoverHashes, signalHashes...),
		UnhandledCarryover:    nil,
		TimeoutWithoutSignals: false,
	}
	if !pumpResultsEqual(expected, actual) {
		return errors.New("did not get expected pump result")
	}
	if !channelContainsExpected(ctx, requestCh, append(carryover, signalsSent...)) {
		return errors.New("request channel was not populated with expected values")
	}
	return nil
}

func sendRequestsToChannel(ctx workflow.Context, ch workflow.Channel, numRequests int) ([]ArchiveRequest, []uint64) {
	requests := make([]ArchiveRequest, numRequests)
	hashes := make([]uint64, numRequests)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for i := 0; i < numRequests; i++ {
			requests[i], hashes[i] = randomArchiveRequest()
			ch.Send(ctx, requests[i])
		}
	})
	return requests, hashes
}

func sendRequestsToChannelBlocking(ctx workflow.Context, ch workflow.Channel, numRequests int) ([]ArchiveRequest, []uint64) {
	requests := make([]ArchiveRequest, numRequests)
	hashes := make([]uint64, numRequests)
	for i := 0; i < numRequests; i++ {
		requests[i], hashes[i] = randomArchiveRequest()
		ch.Send(ctx, requests[i])
	}
	return requests, hashes
}

func channelContainsExpected(ctx workflow.Context, ch workflow.Channel, expected []ArchiveRequest) bool {
	for i := 0; i < len(expected); i++ {
		var actual ArchiveRequest
		if !ch.Receive(ctx, &actual) {
			return false
		}
		if hash(expected[i]) != hash(actual) {
			return false
		}
	}
	return !ch.Receive(ctx, nil)
}

func randomCarryover(count int) ([]ArchiveRequest, []uint64) {
	carryover := make([]ArchiveRequest, count)
	hashes := make([]uint64, count)
	for i := 0; i < count; i++ {
		carryover[i], hashes[i] = randomArchiveRequest()
	}
	return carryover, hashes
}

func pumpResultsEqual(expected PumpResult, actual PumpResult) bool {
	return expected.TimeoutWithoutSignals == actual.TimeoutWithoutSignals &&
		requestsEqual(expected.UnhandledCarryover, actual.UnhandledCarryover) &&
		hashesEqual(expected.PumpedHashes, actual.PumpedHashes)
}

func requestsEqual(expected []ArchiveRequest, actual []ArchiveRequest) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i := 0; i < len(expected); i++ {
		if hash(expected[i]) != hash(actual[i]) {
			return false
		}
	}
	return true
}
