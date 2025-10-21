package batcher

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.uber.org/mock/gomock"
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
	s.env.RegisterWorkflow(BatchWorkflowProtobuf)
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

func (s *batcherSuite) TestBatchWorkflow_MissingParams_Protobuf() {
	s.env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{})
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

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Query_Protobuf() {
	var ac *activities
	s.env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).Return(HeartBeatDetails{
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
	s.env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
		Request: &workflowservice.StartBatchOperationRequest{
			JobId: uuid.New(),
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{},
			},
			Namespace:       "test-namespace",
			Reason:          "test-reason",
			VisibilityQuery: "test-query",
		},
		BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE,
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

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Executions_Protobuf() {
	var ac *activities
	s.env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).Return(HeartBeatDetails{
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
	s.env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
		Request: &workflowservice.StartBatchOperationRequest{
			JobId: uuid.New(),
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{},
			},
			Executions: []*commonpb.WorkflowExecution{
				{
					WorkflowId: uuid.New(),
					RunId:      uuid.New(),
				},
			},
			Reason:    "test-reason",
			Namespace: "test-namespace",
		},
		BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE,
	})
	err := s.env.GetWorkflowError()
	s.Require().NoError(err)
}
