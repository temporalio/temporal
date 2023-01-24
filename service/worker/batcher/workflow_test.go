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

package batcher

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/testsuite"
)

type batcherSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
	env        *testsuite.TestWorkflowEnvironment
}

func TestBatcherSuite(t *testing.T) {
	suite.Run(t, new(batcherSuite))
}

func (s *batcherSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.WorkflowTestSuite.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(BatchWorkflow)
}

func (s *batcherSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *batcherSuite) TestBatchWorkflow_MissingParams() {
	s.env.ExecuteWorkflow(BatchWorkflow, BatchParams{})
	err := s.env.GetWorkflowError()
	s.Require().Error(err)
	s.Contains(err.Error(), "must provide required parameters")
}

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Query() {
	var ac *activities
	s.env.OnActivity(ac.BatchActivity, mock.Anything, mock.Anything).Return(HeartBeatDetails{
		SuccessCount: 42,
		ErrorCount:   27,
	}, nil)
	s.env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
		memo, ok := args.Get(0).(map[string]interface{})
		s.Require().True(ok)
		s.Equal(map[string]interface{}{
			"batch_operation_stats": BatchOperationStats{
				NumSuccess: 42,
				NumFailure: 27,
			},
		}, memo)
	}).Once()
	s.env.ExecuteWorkflow(BatchWorkflow, BatchParams{
		BatchType: BatchTypeTerminate,
		Reason:    "test-reason",
		Namespace: "test-namespace",
		Query:     "test-query",
	})
	err := s.env.GetWorkflowError()
	s.Require().NoError(err)
}

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Executions() {
	var ac *activities
	s.env.OnActivity(ac.BatchActivity, mock.Anything, mock.Anything).Return(HeartBeatDetails{
		SuccessCount: 42,
		ErrorCount:   27,
	}, nil)
	s.env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
		memo, ok := args.Get(0).(map[string]interface{})
		s.Require().True(ok)
		s.Equal(map[string]interface{}{
			"batch_operation_stats": BatchOperationStats{
				NumSuccess: 42,
				NumFailure: 27,
			},
		}, memo)
	}).Once()
	s.env.ExecuteWorkflow(BatchWorkflow, BatchParams{
		BatchType: BatchTypeTerminate,
		Reason:    "test-reason",
		Namespace: "test-namespace",
		Executions: []*commonpb.WorkflowExecution{
			{
				WorkflowId: uuid.New(),
				RunId:      uuid.New(),
			},
		},
	})
	err := s.env.GetWorkflowError()
	s.Require().NoError(err)
}
