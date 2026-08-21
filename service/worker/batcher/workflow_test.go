package batcher

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
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
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(BatchWorkflowProtobuf)
}

func (s *batcherSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *batcherSuite) TestBatchWorkflow_ValidParams_Query_Protobuf() {
	var ac *activities
	s.env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).Return(HeartBeatDetails{
		SuccessCount: 42,
		ErrorCount:   27,
	}, nil)
	s.env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
		memo, ok := args.Get(0).(map[string]any)
		s.Require().True(ok)
		s.Equal(map[string]any{
			"batch_operation_stats": BatchOperationStats{
				NumSuccess: 42,
				NumFailure: 27,
			},
		}, memo)
	}).Once()
	s.env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
		Request: &workflowservice.StartBatchOperationRequest{
			JobId: uuid.NewString(),
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{},
			},
			Namespace:       "test-namespace",
			Reason:          "test-reason",
			VisibilityQuery: "test-query",
		},
		BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
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
		memo, ok := args.Get(0).(map[string]any)
		s.Require().True(ok)
		s.Equal(map[string]any{
			"batch_operation_stats": BatchOperationStats{
				NumSuccess: 42,
				NumFailure: 27,
			},
		}, memo)
	}).Once()
	s.env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
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
		BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
	})
	err := s.env.GetWorkflowError()
	s.Require().NoError(err)
}

func (s *batcherSuite) TestSetDefaultParams() {
	for _, tc := range []struct {
		name                     string
		input                    *batchspb.BatchOperationInput
		expectedAttempts         int64
		expectedHeartbeatTimeout time.Duration
	}{
		{
			name:                     "unset takes the defaults",
			input:                    &batchspb.BatchOperationInput{},
			expectedAttempts:         defaultAttemptsOnRetryableError,
			expectedHeartbeatTimeout: defaultActivityHeartBeatTimeout,
		},
		{
			// A single retry is a valid request: the caller wants a task that
			// keeps failing to be given up on quickly, not retried 50 times.
			name:                     "one attempt is honored",
			input:                    &batchspb.BatchOperationInput{AttemptsOnRetryableError: 1},
			expectedAttempts:         1,
			expectedHeartbeatTimeout: defaultActivityHeartBeatTimeout,
		},
		{
			name:                     "explicit attempts are honored",
			input:                    &batchspb.BatchOperationInput{AttemptsOnRetryableError: 7},
			expectedAttempts:         7,
			expectedHeartbeatTimeout: defaultActivityHeartBeatTimeout,
		},
		{
			name:                     "negative attempts take the default",
			input:                    &batchspb.BatchOperationInput{AttemptsOnRetryableError: -1},
			expectedAttempts:         defaultAttemptsOnRetryableError,
			expectedHeartbeatTimeout: defaultActivityHeartBeatTimeout,
		},
		{
			name: "explicit heartbeat timeout is honored",
			input: &batchspb.BatchOperationInput{
				AttemptsOnRetryableError: 3,
				ActivityHeartbeatTimeout: durationpb.New(time.Minute),
			},
			expectedAttempts:         3,
			expectedHeartbeatTimeout: time.Minute,
		},
		{
			name: "non-positive heartbeat timeout takes the default",
			input: &batchspb.BatchOperationInput{
				AttemptsOnRetryableError: 3,
				ActivityHeartbeatTimeout: durationpb.New(0),
			},
			expectedAttempts:         3,
			expectedHeartbeatTimeout: defaultActivityHeartBeatTimeout,
		},
	} {
		s.Run(tc.name, func() {
			params := setDefaultParams(tc.input)
			s.Equal(tc.expectedAttempts, params.GetAttemptsOnRetryableError())
			s.Equal(tc.expectedHeartbeatTimeout, params.GetActivityHeartbeatTimeout().AsDuration())
		})
	}
}
