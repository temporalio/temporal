package batcher

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type batcherSuite struct {
	parallelsuite.Suite[*batcherSuite]
}

func TestBatcherSuite(t *testing.T) {
	parallelsuite.Run(t, new(batcherSuite))
}

func (s *batcherSuite) newTestWorkflowEnvironment() *testsuite.TestWorkflowEnvironment {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(BatchWorkflowProtobuf)
	return env
}

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Query_Protobuf() {
	env := s.newTestWorkflowEnvironment()
	defer env.AssertExpectations(s.T())

	var ac *activities
	env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).Return(HeartBeatDetails{
		SuccessCount: 42,
		ErrorCount:   27,
	}, nil)
	env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
		memo, ok := args.Get(0).(map[string]any)
		s.True(ok)
		s.Equal(map[string]any{
			"batch_operation_stats": BatchOperationStats{
				NumSuccess: 42,
				NumFailure: 27,
			},
		}, memo)
	}).Once()
	env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
		Request: &workflowservice.StartBatchOperationRequest{
			JobId: uuid.NewString(),
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{},
			},
			Namespace:       "test-namespace",
			Reason:          "test-reason",
			VisibilityQuery: "test-query",
		},
		BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE,
	})
	err := env.GetWorkflowError()
	s.NoError(err)
}

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Executions_Protobuf() {
	env := s.newTestWorkflowEnvironment()
	defer env.AssertExpectations(s.T())

	var ac *activities
	env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).Return(HeartBeatDetails{
		SuccessCount: 42,
		ErrorCount:   27,
	}, nil)
	env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
		memo, ok := args.Get(0).(map[string]any)
		s.True(ok)
		s.Equal(map[string]any{
			"batch_operation_stats": BatchOperationStats{
				NumSuccess: 42,
				NumFailure: 27,
			},
		}, memo)
	}).Once()
	env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
		Request: &workflowservice.StartBatchOperationRequest{
			JobId: uuid.NewString(),
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{},
			},
			Executions: []*commonpb.WorkflowExecution{
				{
					WorkflowId: uuid.NewString(),
					RunId:      uuid.NewString(),
				},
			},
			Reason:    "test-reason",
			Namespace: "test-namespace",
		},
		BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE,
	})
	err := env.GetWorkflowError()
	s.NoError(err)
}
