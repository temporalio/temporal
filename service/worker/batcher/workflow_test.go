package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
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

func TestBatchWorkflowActivityOptionsAreIndependent(t *testing.T) {
	const workflowCount = 32

	expectedTimeouts := make([]time.Duration, workflowCount)
	actualTimeouts := make([]time.Duration, workflowCount)
	errs := make([]error, workflowCount)
	var wg sync.WaitGroup
	for i := range workflowCount {
		expectedTimeout := time.Duration(i+10) * time.Second
		var heartbeatTimeout *durationpb.Duration
		if i == 0 {
			expectedTimeout = defaultActivityHeartBeatTimeout
		} else {
			heartbeatTimeout = durationpb.New(expectedTimeout)
		}
		expectedTimeouts[i] = expectedTimeout

		wg.Go(func() {
			var workflowTestSuite testsuite.WorkflowTestSuite
			env := workflowTestSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(BatchWorkflowProtobuf)
			var ac *activities
			env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).
				Return(func(ctx context.Context, _ *batchspb.BatchOperationInput) (HeartBeatDetails, error) {
					actualTimeouts[i] = activity.GetInfo(ctx).HeartbeatTimeout
					return HeartBeatDetails{}, nil
				}).Once()
			env.OnUpsertMemo(mock.Anything).Return(nil).Once()
			env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
				ActivityHeartbeatTimeout: heartbeatTimeout,
			})
			errs[i] = env.GetWorkflowError()
			env.AssertExpectations(t)
		})
	}
	wg.Wait()

	for i := range workflowCount {
		require.NoError(t, errs[i])
		require.Equal(t, expectedTimeouts[i], actualTimeouts[i])
	}
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
