package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
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

// TestBatchActivityOptions_IndependentPerCall verifies the activity options are
// built per call and share no mutable state, so concurrent batch workflows on a
// worker cannot race on them or pick up each other's heartbeat timeout.
func (s *batcherSuite) TestBatchActivityOptions_IndependentPerCall() {
	first := batchActivityOptions(time.Second)
	second := batchActivityOptions(time.Hour)

	s.Equal(time.Second, first.HeartbeatTimeout)
	s.Equal(time.Hour, second.HeartbeatTimeout)
	s.Equal(5*time.Minute, first.ScheduleToStartTimeout)
	s.Equal(infiniteDuration, first.StartToCloseTimeout)

	s.Require().NotNil(first.RetryPolicy)
	s.Require().NotNil(second.RetryPolicy)
	s.NotSame(first.RetryPolicy, second.RetryPolicy,
		"each call must get its own retry policy, not a shared pointer")
}

// TestBatchWorkflow_HeartbeatTimeoutFromParams verifies the activity is scheduled
// with the heartbeat timeout from its own params, defaulted when unset.
func (s *batcherSuite) TestBatchWorkflow_HeartbeatTimeoutFromParams() {
	for _, tc := range []struct {
		name             string
		heartbeatTimeout *durationpb.Duration
		expected         time.Duration
	}{
		{
			name:             "unset uses the default",
			heartbeatTimeout: nil,
			expected:         defaultActivityHeartBeatTimeout,
		},
		{
			name:             "explicit value is used",
			heartbeatTimeout: durationpb.New(42 * time.Second),
			expected:         42 * time.Second,
		},
	} {
		s.Run(tc.name, func() {
			env := s.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(BatchWorkflowProtobuf)

			var ac *activities
			var gotHeartbeatTimeout time.Duration
			env.OnActivity(ac.BatchActivityWithProtobuf, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					ctx, ok := args.Get(0).(context.Context)
					s.Require().True(ok)
					gotHeartbeatTimeout = activity.GetInfo(ctx).HeartbeatTimeout
				}).
				Return(HeartBeatDetails{}, nil)
			env.OnUpsertMemo(mock.Anything).Return(nil).Once()

			env.ExecuteWorkflow(BatchWorkflowProtobuf, &batchspb.BatchOperationInput{
				ActivityHeartbeatTimeout: tc.heartbeatTimeout,
				Request: &workflowservice.StartBatchOperationRequest{
					JobId: uuid.NewString(),
					Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
						TerminationOperation: &batchpb.BatchOperationTermination{},
					},
					VisibilityQuery: "WorkflowType = 'test'",
					Reason:          "test-reason",
					Namespace:       "test-namespace",
				},
				BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
			})

			s.Require().NoError(env.GetWorkflowError())
			s.Equal(tc.expected, gotHeartbeatTimeout)
		})
	}
}

// TestBatchActivityOptions_ConcurrentCallsDoNotInterfere guards the failure mode
// the per-call construction exists for: batch workflows run concurrently on a
// worker, so building the options through shared mutable state both races (under
// -race) and lets one execution observe another's heartbeat timeout.
func (s *batcherSuite) TestBatchActivityOptions_ConcurrentCallsDoNotInterfere() {
	const numCalls = 50

	got := make([]time.Duration, numCalls)
	var wg sync.WaitGroup
	for i := range numCalls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A distinct timeout per goroutine, so a shared value would surface
			// as some other goroutine's timeout here.
			got[i] = batchActivityOptions(time.Duration(i+1) * time.Second).HeartbeatTimeout
		}()
	}
	wg.Wait()

	for i, timeout := range got {
		s.Equal(time.Duration(i+1)*time.Second, timeout)
	}
}
