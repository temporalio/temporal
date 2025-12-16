package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TODO(fred) add tests for retries when we implement search attributes

var (
	defaultInput            = payloads.EncodeString("Input")
	defaultHeartbeatDetails = payloads.EncodeString("Heartbeat Details")
	defaultResult           = payloads.EncodeString("Done")
	defaultFailure          = &failurepb.Failure{
		Message: "Failed Activity",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "Test",
			NonRetryable: true,
		}},
	}
)

type standaloneActivityTestSuite struct {
	testcore.FunctionalTestBase
	tv *testvars.TestVars
}

func TestStandaloneActivityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(standaloneActivityTestSuite))
}

func (s *standaloneActivityTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite()
	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	s.OverrideDynamicConfig(
		activity.Enabled,
		true,
	)
}

func (s *standaloneActivityTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.tv = testvars.New(s.T())
}

func (s *standaloneActivityTestSuite) TestIDReusePolicy_RejectDuplicate() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId
	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Result:    defaultResult,
		Identity:  "new-worker",
	})
	require.NoError(t, err)

	s.validateCompletion(ctx, t, activityID, runID, "new-worker")

	_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout: durationpb.New(1 * time.Minute),
		IdReusePolicy:       enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
	})
	require.Error(t, err)
}

func (s *standaloneActivityTestSuite) TestIDReusePolicy_AllowDuplicateFailedOnly() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Failure:   defaultFailure,
		Identity:  "new-worker",
	})
	require.NoError(t, err)

	s.validateFailure(ctx, t, activityID, runID, nil, "new-worker")

	_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout: durationpb.New(1 * time.Minute),
		IdReusePolicy:       enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	})
	require.NoError(t, err)
}

func (s *standaloneActivityTestSuite) TestIDConflictPolicy_FailsIfExists() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	s.startAndValidateActivity(ctx, t, activityID, taskQueue)

	// By default, unspecified conflict policy should be set to ACTIVITY_ID_CONFLICT_POLICY_FAIL, so no need to set explicitly
	_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout: durationpb.New(1 * time.Minute),
	})
	require.Error(t, err)
}

// TODO(fred): add test for BusinessIDConflictPolicyUseExisting after rebasing on main

func (s *standaloneActivityTestSuite) TestActivityCompleted() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Result:    defaultResult,
		Identity:  "new-worker",
	})
	require.NoError(t, err)

	s.validateCompletion(ctx, t, activityID, runID, "new-worker")
}

func (s *standaloneActivityTestSuite) TestActivityCompletedByID() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskCompletedById(ctx, &workflowservice.RespondActivityTaskCompletedByIdRequest{
		Namespace:  s.Namespace().String(),
		RunId:      runID,
		ActivityId: activityID,
		Result:     defaultResult,
		Identity:   s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)

	s.validateCompletion(ctx, t, activityID, runID, s.tv.WorkerIdentity())
}

func (s *standaloneActivityTestSuite) TestActivityFailed() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Failure:   defaultFailure,
		Identity:  "new-worker",
	})
	require.NoError(t, err)

	s.validateFailure(ctx, t, activityID, runID, nil, "new-worker")
}

func (s *standaloneActivityTestSuite) TestActivityFailedWithLastHeartbeat() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		Namespace:            s.Namespace().String(),
		TaskToken:            pollTaskResp.TaskToken,
		Failure:              defaultFailure,
		LastHeartbeatDetails: defaultHeartbeatDetails,
		Identity:             s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)

	s.validateFailure(ctx, t, activityID, runID, defaultHeartbeatDetails, s.tv.WorkerIdentity())
}

func (s *standaloneActivityTestSuite) TestActivityFailedByID() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskFailedById(ctx, &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  s.Namespace().String(),
		RunId:      runID,
		ActivityId: activityID,
		Failure:    defaultFailure,
		Identity:   s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)

	s.validateFailure(ctx, t, activityID, runID, nil, s.tv.WorkerIdentity())
}

func (s *standaloneActivityTestSuite) TestActivityCancelled() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: s.tv.ActivityID(),
		RunId:      runID,
		Identity:   "cancelling-worker",
		RequestId:  s.tv.RequestID(),
		Reason:     "Test Cancellation",
	})
	require.NoError(t, err)

	// TODO: we should get the cancel request from heart beat once we implement it

	details := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			payload.EncodeString("Canceled Details"),
		},
	}

	_, err = s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Details:   details,
		Identity:  "new-worker",
	})
	require.NoError(t, err)

	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})
	require.NoError(t, err)

	info := activityResp.GetInfo()
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus(),
		"expected Canceled but is %s", info.GetStatus())
	require.Equal(t, "Test Cancellation", info.GetCanceledReason())
	protorequire.ProtoEqual(t, details, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetDetails())
}

func (s *standaloneActivityTestSuite) TestActivityCancelledByID() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: s.tv.ActivityID(),
		RunId:      runID,
		Identity:   "cancelling-worker",
		RequestId:  s.tv.RequestID(),
		Reason:     "Test Cancellation",
	})
	require.NoError(t, err)

	// TODO: we should get the cancel request from heart beat once we implement it

	details := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			payload.EncodeString("Canceled Details"),
		},
	}

	_, err = s.FrontendClient().RespondActivityTaskCanceledById(ctx, &workflowservice.RespondActivityTaskCanceledByIdRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      runID,
		Details:    details,
		Identity:   "new-worker",
	})
	require.NoError(t, err)

	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})
	require.NoError(t, err)

	info := activityResp.GetInfo()
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus(),
		"expected Canceled but is %s", info.GetStatus())
	require.Equal(t, "Test Cancellation", info.GetCanceledReason())
	protorequire.ProtoEqual(t, details, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetDetails())
}

func (s *standaloneActivityTestSuite) TestActivityCancelled_FailsIfNeverRequested() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	details := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			payload.EncodeString("Canceled Details"),
		},
	}

	_, err := s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Details:   details,
		Identity:  "new-worker",
	})
	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
}

func (s *standaloneActivityTestSuite) TestActivityCancelled_DuplicateRequestIDSucceeds() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	for i := 0; i < 2; i++ {
		_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: s.tv.ActivityID(),
			RunId:      runID,
			Identity:   "cancelling-worker",
			RequestId:  "cancel-request-id",
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)
	}

	// TODO: we should get the cancel request from heartbeat once we implement it

	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})
	require.NoError(t, err)

	info := activityResp.GetInfo()
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus(),
		"expected Running but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, info.GetRunState(),
		"expected CancelRequested but is %s", info.GetRunState())
	require.Equal(t, "Test Cancellation", info.GetCanceledReason())
}

func (s *standaloneActivityTestSuite) TestActivityCancelled_DifferentRequestIDFails() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: s.tv.ActivityID(),
		RunId:      runID,
		Identity:   "cancelling-worker",
		RequestId:  "cancel-request-id",
		Reason:     "Test Cancellation",
	})
	require.NoError(t, err)

	_, err = s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: s.tv.ActivityID(),
		RunId:      runID,
		Identity:   "cancelling-worker",
		RequestId:  "different-cancel-request-id",
		Reason:     "Test Cancellation",
	})
	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
}

func (s *standaloneActivityTestSuite) TestActivityFinishes_AfterCancelRequested() {
	testCases := []struct {
		name             string
		taskCompletionFn func(context.Context, *testing.T, []byte, string, string) error
		expectedStatus   enumspb.ActivityExecutionStatus
	}{
		{
			name: "finish with completion",
			taskCompletionFn: func(ctx context.Context, t *testing.T, taskToken []byte, activityID string, runID string) error {
				_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
					Result:    defaultResult,
				})

				return err
			},
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		},
		{
			name: "finish with failure",
			taskCompletionFn: func(ctx context.Context, t *testing.T, taskToken []byte, activityID string, runID string) error {
				_, err := s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
					Failure:   defaultFailure,
				})

				return err
			},
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
		},
		{
			name: "finish with termination",
			taskCompletionFn: func(ctx context.Context, t *testing.T, taskToken []byte, activityID string, runID string) error {
				_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Reason:     "Test Termination",
				})

				return err
			},
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			t := s.T()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			activityID := s.tv.Any().String()
			taskQueue := s.tv.TaskQueue().String()

			startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
			runID := startResp.RunId

			pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
				Identity:   "cancelling-worker",
				RequestId:  s.tv.RequestID(),
				Reason:     "Test Cancellation",
			})
			require.NoError(t, err)

			err = tc.taskCompletionFn(ctx, t, pollTaskResp.GetTaskToken(), activityID, runID)
			require.NoError(t, err)

			activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
			require.NoError(t, err)

			info := activityResp.GetInfo()
			require.Equal(t, tc.expectedStatus, info.GetStatus())
		})
	}
}

func (s *standaloneActivityTestSuite) TestRequestCancellation_FailsValidation() {
	testCases := []struct {
		name   string
		reqID  string
		reason string
	}{
		{
			name:   "request ID too long",
			reqID:  string(make([]byte, 1001)), // dynamic config default is 1000
			reason: "",
		},
		{
			name:   "reason too long",
			reqID:  "",
			reason: string(make([]byte, 1001)), // dynamic config default is 1000
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			t := s.T()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: s.tv.ActivityID(),
				RunId:      "run-id",
				Identity:   "cancelling-worker",
				RequestId:  tc.reqID,
				Reason:     tc.reason,
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
		})
	}
}

// TODO running into "unable to change workflow state from Created to Completed, status Failed from the chasm engine"
// This should be re-enabled after its addressed from the chasm engine and we implement the search attributes interface.
func (s *standaloneActivityTestSuite) TestActivityImmediatelyCancelled_WhenInScheduledState() {
	s.T().Skip("Temporarily disabled")

	/*	t := s.T()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		activityID := s.tv.ActivityID()
		taskQueue := s.tv.TaskQueue().String()

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: s.tv.ActivityID(),
			RunId:      runID,
			Identity:   "cancelling-worker",
			RequestId:  s.tv.RequestID(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      s.Namespace().String(),
			ActivityId:     activityID,
			RunId:          runID,
			IncludeInput:   true,
			IncludeOutcome: true,
		})
		require.NoError(t, err)

		info := activityResp.GetInfo()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus()) */
}

func (s *standaloneActivityTestSuite) TestActivityTerminated() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      runID,
		Reason:     "Test Termination",
		Identity:   "terminator",
	})
	require.NoError(t, err)

	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, info.GetStatus(),
		"expected Terminated but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState(),
		"expected Unspecified but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, s.tv.WorkerIdentity(), info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, info.GetLastFailure())

	expectedFailure := &failurepb.Failure{
		Message:     "Test Termination",
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
	}
	protorequire.ProtoEqual(t, expectedFailure, activityResp.GetOutcome().GetFailure())
}

func (s *standaloneActivityTestSuite) TestCompletedActivity_CannotTerminate() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue().String()

	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

	_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Result:    defaultResult,
		Identity:  "new-worker",
	})
	require.NoError(t, err)

	_, err = s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      runID,
		Reason:     "Test Termination",
		Identity:   "worker",
	})
	require.Error(t, err)
}

func (s *standaloneActivityTestSuite) TestRetryWithoutScheduleToCloseTimeout() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	// Start activity without ScheduleToCloseTimeout
	_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           s.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(1 * time.Minute),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Millisecond),
			MaximumAttempts: 2,
		},
	})
	require.NoError(t, err)

	// Attempt 1: fail retryably
	pollResp1, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, pollResp1.Attempt)
	_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp1.TaskToken,
		Failure: &failurepb.Failure{
			Message: "retryable failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
			},
		},
	})
	require.NoError(t, err)

	// Attempt 2 should be scheduled
	pollResp2, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, pollResp2.Attempt)
}

func (s *standaloneActivityTestSuite) Test_ScheduleToCloseTimeout_WithRetry() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	// Start an activity
	startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		// It's not possible to guarantee (e.g. via NextRetryDelay or RetryPolicy) that a retry
		// will start with a delay <1s because of the use of TimerProcessorMaxTimeShift in the
		// timer queue. Therefore we allow 1s for the ActivityDispatchTask to be executed, and
		// time out the activity 1s into Attempt 2.
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
	})
	require.NoError(t, err)

	// Fail attempt 1, causing the attempt counter to increment.
	pollTaskResp, err := s.pollActivityTaskQueue(ctx, taskQueue)
	require.NoError(t, err)
	_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Failure: &failurepb.Failure{
			Message: "Retryable failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable:   false,
				NextRetryDelay: durationpb.New(1 * time.Second),
			}},
		},
	})
	require.NoError(t, err)
	_, err = s.pollActivityTaskQueue(ctx, taskQueue)
	require.NoError(t, err)

	// Wait for schedule-to-close timeout.
	pollActivityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, pollActivityResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected ScheduleToCloseTimeout but is %s", pollActivityResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

// TestStartToCloseTimeout tests that a start-to-close timeout is recorded after the activity is
// started. It also verifies that DescribeActivityExecution can be used to long-poll for a TimedOut
// state change caused by execution of a timer task.
func (s *standaloneActivityTestSuite) TestStartToCloseTimeout() {
	t := s.T()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()

	startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		Input: payloads.EncodeString("test-activity-input"),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue.Name,
		},
		StartToCloseTimeout: durationpb.New(1 * time.Second),
		// This test is expecting activity failure on start-to-close timeout.
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumAttempts: 1,
		},
		RequestId: "test-request-id",
	})
	require.NoError(t, err)

	// First poll: activity has not started yet
	describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	require.NotNil(t, describeResp.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp.GetInfo().GetStatus(),
		"expected Running but is %s", describeResp.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, describeResp.GetInfo().GetRunState(),
		"expected Scheduled but is %s", describeResp.GetInfo().GetRunState())

	// Worker poll to start the activity
	pollTaskResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue.Name,
		},
		Identity: s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	require.NotNil(t, pollTaskResp)
	require.NotEmpty(t, pollTaskResp.TaskToken)

	// Second poll: activity has started
	describeResp, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:     s.Namespace().String(),
		ActivityId:    activityID,
		RunId:         startResp.RunId,
		LongPollToken: describeResp.LongPollToken,
	})
	require.NoError(t, err)
	require.NotNil(t, describeResp)
	require.NotNil(t, describeResp.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp.GetInfo().GetStatus(),
		"expected Running but is %s", describeResp.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, describeResp.GetInfo().GetRunState(),
		"expected Started but is %s", describeResp.GetInfo().GetRunState())

	// Third poll: activity has timed out
	describeResp, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResp.RunId,
		IncludeOutcome: true,
		LongPollToken:  describeResp.LongPollToken,
	})

	require.NoError(t, err)
	require.NotNil(t, describeResp)
	require.NotNil(t, describeResp.GetInfo())

	// The activity has timed out due to StartToClose. This is an attempt failure, therefore the
	// failure should be in ActivityExecutionInfo.LastFailure as well as set as the outcome failure.
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, describeResp.GetInfo().GetStatus(),
		"expected TimedOut but is %s", describeResp.GetInfo().GetStatus())
	failure := describeResp.GetInfo().GetLastFailure()
	require.NotNil(t, failure)
	timeoutFailure := failure.GetTimeoutFailureInfo()
	require.NotNil(t, timeoutFailure)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutFailure.GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", timeoutFailure.GetTimeoutType())

	require.NotNil(t, describeResp.GetOutcome().GetFailure())
	protorequire.ProtoEqual(t, failure, describeResp.GetOutcome().GetFailure())
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, describeResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", describeResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_NoWait() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()

	startResp, err := s.startActivity(ctx, activityID, taskQueue.Name)
	require.NoError(t, err)

	t.Run("MinimalResponse", func(t *testing.T) {
		describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			// Omit RunID to verify that latest run will be used
			IncludeInput:   false,
			IncludeOutcome: false,
		})
		require.NoError(t, err)
		require.NotNil(t, describeResp.LongPollToken)
		require.Equal(t, startResp.RunId, describeResp.RunId)
		require.Nil(t, describeResp.Input)
		require.Nil(t, describeResp.GetOutcome().GetResult())
		require.Nil(t, describeResp.GetOutcome().GetFailure())
	})

	t.Run("FullResponse", func(t *testing.T) {
		describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      s.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeInput:   true,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.NotNil(t, describeResp.LongPollToken)
		require.NotNil(t, describeResp.Info)
		s.assertActivityExecutionInfo(
			t,
			describeResp.Info,
			activityID,
			startResp.RunId,
			enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		)
		protorequire.ProtoEqual(t, defaultInput, describeResp.Input)

		// Activity is scheduled but not completed, so no outcome yet
		require.Nil(t, describeResp.GetOutcome().GetResult())
		require.Nil(t, describeResp.GetOutcome().GetFailure())
	})
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_WaitAnyStateChange() {
	// Long poll for any state change. PollActivityTaskQueue is used to cause a state change.
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()

	startResp, err := s.startActivity(ctx, activityID, taskQueue.Name)
	require.NoError(t, err)

	// First poll lacks token and therefore responds immediately, returning a token
	firstDescribeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		// RunId:        startResp.RunId, // RunID is now required by validation [?]
		IncludeInput: true,
	})
	require.NoError(t, err)
	require.NotNil(t, firstDescribeResp.LongPollToken)
	require.NotNil(t, firstDescribeResp.Info)
	require.Equal(t, firstDescribeResp.RunId, startResp.RunId)
	s.assertActivityExecutionInfo(
		t,
		firstDescribeResp.Info,
		activityID,
		startResp.RunId,
		enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
		enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
	)

	taskQueuePollErr := make(chan error, 1)
	activityPollDone := make(chan struct{})
	var describeResp *workflowservice.DescribeActivityExecutionResponse
	var describeErr error

	go func() {
		defer close(activityPollDone)
		// Second poll uses token and therefore waits for a state transition
		describeResp, describeErr = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     s.Namespace().String(),
			ActivityId:    activityID,
			RunId:         startResp.RunId,
			IncludeInput:  true,
			LongPollToken: firstDescribeResp.LongPollToken,
		})
	}()

	// TODO(dan): race here: subscription might not be established yet

	// Worker picks up activity task, triggering transition (via RecordActivityTaskStarted)
	go func() {
		_, err := s.pollActivityTaskQueue(ctx, taskQueue.Name)
		taskQueuePollErr <- err
	}()

	select {
	case <-activityPollDone:
		require.NoError(t, describeErr)
		require.NotNil(t, describeResp)
		require.NotNil(t, describeResp.Info)
		s.assertActivityExecutionInfo(
			t,
			describeResp.Info,
			activityID,
			startResp.RunId,
			enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			enumspb.PENDING_ACTIVITY_STATE_STARTED,
		)
		protorequire.ProtoEqual(t, defaultInput, describeResp.Input)

	case <-ctx.Done():
		t.Fatal("DescribeActivityExecution timed out")
	}

	err = <-taskQueuePollErr
	require.NoError(t, err)
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution() {
	testCases := []struct {
		name                   string
		expectedStatus         enumspb.ActivityExecutionStatus
		taskCompletionFn       func(context.Context, []byte) error
		completionValidationFn func(*testing.T, *workflowservice.PollActivityExecutionResponse)
	}{
		{
			name:           "successful completion",
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			taskCompletionFn: func(ctx context.Context, taskToken []byte) error {
				_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
					Result:    defaultResult,
				})

				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				protorequire.ProtoEqual(t, defaultResult, response.GetOutcome().GetResult())
			},
		},
		{
			name:           "failure completion",
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
			taskCompletionFn: func(ctx context.Context, taskToken []byte) error {
				_, err := s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
					Failure:   defaultFailure,
				})

				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				protorequire.ProtoEqual(t, defaultFailure, response.GetOutcome().GetFailure())
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			t := s.T()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			activityID := s.tv.Any().String()
			taskQueue := s.tv.TaskQueue().String()

			startResp, err := s.startActivity(ctx, activityID, taskQueue)
			require.NoError(t, err)
			pollTaskResp, err := s.pollActivityTaskQueue(ctx, taskQueue)
			require.NoError(t, err)
			err = tc.taskCompletionFn(ctx, pollTaskResp.TaskToken)
			require.NoError(t, err)
			pollActivityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.NotNil(t, pollActivityResp)
			tc.completionValidationFn(t, pollActivityResp)
		})
	}
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_NotFound() {
	t := s.T()
	ctx := testcore.NewContext()

	existingActivityID := s.tv.ActivityID()
	tq := s.tv.TaskQueue()
	startResp, err := s.startActivity(ctx, existingActivityID, tq.Name)
	require.NoError(t, err)
	existingRunID := startResp.RunId
	require.NotEmpty(t, existingRunID)
	existingNamespace := s.Namespace().String()

	var notFoundErr *serviceerror.NotFound
	var namespaceNotFoundErr *serviceerror.NamespaceNotFound

	testCases := []struct {
		name           string
		request        *workflowservice.PollActivityExecutionRequest
		expectedErr    error
		expectedErrMsg string
	}{
		{
			name: "NonExistentNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "non-existent-namespace",
				ActivityId: existingActivityID,
				RunId:      existingRunID,
			},
			expectedErr:    namespaceNotFoundErr,
			expectedErrMsg: "Namespace non-existent-namespace is not found.",
		},
		{
			name: "NonExistentActivityID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "non-existent-activity",
				RunId:      existingRunID,
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: "activity execution not found",
		},
		{
			name: "NonExistentRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "11111111-2222-3333-4444-555555555555",
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: "activity execution not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.FrontendClient().PollActivityExecution(ctx, tc.request)
			require.ErrorAs(t, err, &tc.expectedErr) //nolint:testifylint
			require.Equal(t, tc.expectedErrMsg, tc.expectedErr.Error())
		})
	}
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_InvalidArgument() {
	t := s.T()
	ctx := testcore.NewContext()

	existingNamespace := s.Namespace().String()
	validRunID := "11111111-2222-3333-4444-555555555555"

	testCases := []struct {
		name        string
		request     *workflowservice.PollActivityExecutionRequest
		expectedErr string
	}{
		{
			name: "EmptyNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "",
				ActivityId: "activity-id",
				RunId:      validRunID,
			},
			expectedErr: "Namespace is empty",
		},
		{
			name: "EmptyActivityID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "",
				RunId:      validRunID,
			},
			expectedErr: "activity ID is required",
		},
		{
			name: "ActivityIDTooLong",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: string(make([]byte, 2000)),
				RunId:      validRunID,
			},
			expectedErr: "activity ID exceeds length limit",
		},
		{
			name: "InvalidRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "activity-id",
				RunId:      "invalid-uuid",
			},
			expectedErr: "invalid run id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.FrontendClient().PollActivityExecution(ctx, tc.request)
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, tc.expectedErr)
		})
	}
}

// TODO(dan): add tests that DescribeActivityExecution can wait for deletion, termination, cancellation etc

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_DeadlineExceeded() {
	t := s.T()
	ctx := testcore.NewContext()

	// Start an activity and get initial long-poll state token
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()
	startResp, err := s.startActivity(ctx, activityID, taskQueue.Name)
	require.NoError(t, err)
	describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)

	// The DescribeActivityExecution calls below use a long-poll token and will necessarily time out,
	// because the activity undergoes no further state transitions.

	// The timeout imposed by the server is essentially
	// Min(CallerTimeout - LongPollBuffer, LongPollTimeout)

	// Case 1: Caller sets a deadline which has room for the buffer. History returns empty success
	// result with at least buffer remaining before the caller deadline.
	t.Run("CallerDeadlineNotExceeded", func(t *testing.T) {
		// CallerTimeout - LongPollBuffer is far in the future
		s.OverrideDynamicConfig(activity.LongPollBuffer, 1*time.Second)
		ctx, cancel := context.WithTimeout(ctx, 9999*time.Millisecond)
		defer cancel()

		// DescribeActivityExecution will return when this long poll timeout expires.
		s.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)

		describeResp, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     s.Namespace().String(),
			ActivityId:    activityID,
			RunId:         startResp.RunId,
			LongPollToken: describeResp.LongPollToken,
		})
		// The server uses an empty non-error response to indicate to the caller that it should resubmit
		// its long-poll.
		require.NoError(t, err)
		require.Empty(t, describeResp.GetInfo())
	})

	// Case 2: caller does not set a deadline. In practice this is equivalent to them setting a 30s
	// deadline since that is what Histry receives. In this case History times out the wait at
	// LongPollTimeout and the caller gets an empty response.
	t.Run("NoCallerDeadline", func(t *testing.T) {
		// The caller sets no deadline. However, the ctx received by the history service handler
		// will have a 30s deadline that was applied by one of the upstream server layers, so we
		// still must use a buffer < 30s.
		ctx := context.Background()
		s.OverrideDynamicConfig(activity.LongPollBuffer, 29*time.Second)
		// DescribeActivityExecution will return when this long poll timeout expires.
		s.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)

		_, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     s.Namespace().String(),
			ActivityId:    activityID,
			RunId:         startResp.RunId,
			LongPollToken: describeResp.LongPollToken,
		})
		require.NoError(t, err)
		require.Empty(t, describeResp.GetInfo())
	})

	// Case 3: caller sets a deadline that is < the buffer. In this case DescribeActivityExecution
	// will return an empty result immediately, and there is a race between caller receiving that
	// and caller's client timing out the request. Therefore we do not test this.
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_NotFound() {
	t := s.T()
	ctx := testcore.NewContext()

	existingActivityID := s.tv.ActivityID()
	tq := s.tv.TaskQueue()
	startResp, err := s.startActivity(ctx, existingActivityID, tq.Name)
	require.NoError(t, err)
	existingRunID := startResp.RunId
	require.NotEmpty(t, existingRunID)
	existingNamespace := s.Namespace().String()

	var notFoundErr *serviceerror.NotFound
	var namespaceNotFoundErr *serviceerror.NamespaceNotFound

	testCases := []struct {
		name           string
		request        *workflowservice.DescribeActivityExecutionRequest
		expectedErr    error
		expectedErrMsg string
	}{
		{
			name: "NonExistentNamespace",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  "non-existent-namespace",
				ActivityId: existingActivityID,
				RunId:      existingRunID,
			},
			expectedErr:    namespaceNotFoundErr,
			expectedErrMsg: "Namespace non-existent-namespace is not found.",
		},
		{
			name: "NonExistentActivityID",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "non-existent-activity",
				RunId:      existingRunID,
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: "activity execution not found",
		},
		{
			name: "NonExistentRunID",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "11111111-2222-3333-4444-555555555555",
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: "activity execution not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.FrontendClient().DescribeActivityExecution(ctx, tc.request)
			require.ErrorAs(t, err, &tc.expectedErr) //nolint:testifylint
			require.Equal(t, tc.expectedErrMsg, tc.expectedErr.Error())
		})
	}

	t.Run("LongPollNonExistentActivity", func(t *testing.T) {
		// Poll to get a token
		validPollResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: existingActivityID,
			RunId:      existingRunID,
		})
		require.NoError(t, err)

		// Use the token with a non-existent activity
		_, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     existingNamespace,
			ActivityId:    "non-existent-activity",
			RunId:         existingRunID,
			LongPollToken: validPollResp.LongPollToken,
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, "activity execution not found", notFoundErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_InvalidArgument() {

	t := s.T()
	ctx := testcore.NewContext()

	existingActivityID := s.tv.ActivityID()
	tq := s.tv.TaskQueue()
	startResp, err := s.startActivity(ctx, existingActivityID, tq.Name)
	require.NoError(t, err)
	existingRunID := startResp.RunId
	require.NotEmpty(t, existingRunID)
	existingNamespace := s.Namespace().String()

	validActivityID := "activity-id"
	validRunID := "11111111-2222-3333-4444-555555555555"

	testCases := []struct {
		name        string
		request     *workflowservice.DescribeActivityExecutionRequest
		expectedErr string
	}{
		{
			name: "EmptyNamespace",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  "",
				ActivityId: validActivityID,
				RunId:      validRunID,
			},
			expectedErr: "Namespace is empty",
		},
		{
			name: "EmptyActivityID",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "",
				RunId:      validRunID,
			},
			expectedErr: "activity ID is required",
		},
		{
			name: "ActivityIDTooLong",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: string(make([]byte, 2000)),
				RunId:      validRunID,
			},
			expectedErr: "activity ID exceeds length limit",
		},
		{
			name: "InvalidRunID",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: validActivityID,
				RunId:      "invalid-uuid",
			},
			expectedErr: "invalid run id",
		},
		{
			name: "RunIdNotRequiredWhenWaitPolicyAbsent",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "",
			},
			expectedErr: "",
		},
		{
			name: "RunIdNotRequiredWhenLongPollTokenAbsent",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "",
			},
			expectedErr: "",
		},
		{
			name: "RunIdRequiredWhenLongPollTokenPresent",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     existingNamespace,
				ActivityId:    validActivityID,
				RunId:         "",
				LongPollToken: []byte("doesn't-matter"),
			},
			expectedErr: "run id is required",
		},
		{
			name: "MalformedLongPollToken",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     existingNamespace,
				ActivityId:    existingActivityID,
				RunId:         existingRunID,
				LongPollToken: []byte("invalid-token"),
			},
			expectedErr: "invalid long poll token",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.FrontendClient().DescribeActivityExecution(ctx, tc.request)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, tc.expectedErr)
		})
	}

	t.Run("LongPollTokenFromWrongExecution", func(t *testing.T) {
		validPollResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: existingActivityID,
			RunId:      existingRunID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, validPollResp.LongPollToken)

		activityID2 := s.tv.Any().String()
		startResp2, err := s.startActivity(ctx, activityID2, tq.Name)
		require.NoError(t, err)
		require.NotEmpty(t, startResp2.GetRunId())

		_, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     existingNamespace,
			ActivityId:    activityID2,
			RunId:         startResp2.GetRunId(),
			LongPollToken: validPollResp.LongPollToken,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "long poll token does not match execution", invalidArgErr.Message)
	})

	// TODO(dan): add test for long poll token from non-existent execution
}

func (s *standaloneActivityTestSuite) TestHeartbeat() {
	t := s.T()
	ctx := testcore.NewContext()
	heartbeatDetails := payloads.EncodeString("Heartbeat Details")

	t.Run("InvalidArgument", func(t *testing.T) {
		testCases := []struct {
			name        string
			taskToken   []byte
			expectedErr string
		}{
			{
				name:        "EmptyTaskToken",
				taskToken:   nil,
				expectedErr: "Task token not set on request",
			},
			{
				name:        "MalformedTaskToken",
				taskToken:   []byte("invalid-token-data"),
				expectedErr: "Error deserializing task token",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: s.Namespace().String(),
					TaskToken: tc.taskToken,
					Details:   heartbeatDetails,
				})
				require.Error(t, err)
				statusErr := serviceerror.ToStatus(err)
				require.NotNil(t, statusErr)
				require.Equal(t, codes.InvalidArgument, statusErr.Code())
				require.Contains(t, statusErr.Message(), tc.expectedErr)
			})
		}
	})

	t.Run("StaleToken", func(t *testing.T) {
		// Start an activity and get a valid task token, then complete it
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Heartbeat with stale token (activity already completed)
		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to heartbeat with old token.
		// Use NextRetryDelay=1s to ensure the retry dispatch happens within test timeout.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              s.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           s.tv.ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and get task token for attempt 1
		attempt1Resp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		// Fail the task with NextRetryDelay to control retry timing
		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to get attempt 2 (ensures retry has happened)
		attempt2Resp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Heartbeat with the attempt 2 token
		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Try to heartbeat with the old attempt 1 token - should fail with NotFound
		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")
	})

	t.Run("ResponseIncludesCancelRequested", func(t *testing.T) {
		// Start activity, worker accepts task, request cancellation, worker heartbeats.
		// Verify: heartbeat response has cancel_requested=true.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           s.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        s.tv.ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		})
		require.NoError(t, err)
		runID := startResp.RunId

		// Worker accepts task
		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Heartbeat before cancellation - cancel_requested should be false
		hbResp, err := s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)
		require.False(t, hbResp.CancelRequested)

		// Request cancellation
		_, err = s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			RequestId:  s.tv.RequestID(),
			Reason:     "test cancellation",
		})
		require.NoError(t, err)

		// Heartbeat after cancellation - cancel_requested should be true
		hbResp, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)
		require.True(t, hbResp.CancelRequested)
	})

	t.Run("HeartbeatDetailsAvailableOnRetry", func(t *testing.T) {
		// Start activity (with retries), worker accepts, heartbeats with details,
		// worker fails the task. Worker accepts retry attempt.
		// Verify: retry task contains previous heartbeat details.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              s.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           s.tv.ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Millisecond),
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// First attempt: worker accepts task
		pollResp1, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp1.Attempt)
		require.Nil(t, pollResp1.HeartbeatDetails) // No heartbeat details on first attempt

		// Worker heartbeats with details
		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Worker fails with retryable error
		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: false,
					},
				},
			},
		})
		require.NoError(t, err)

		// Second attempt: worker accepts retry task
		pollResp2, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.Attempt)

		// Verify: heartbeat details from first attempt are available
		protorequire.ProtoEqual(t, heartbeatDetails, pollResp2.HeartbeatDetails)
	})

	t.Run("ActivityTimesOutWithoutHeartbeat", func(t *testing.T) {
		// Start activity (no retries), worker accepts task, time passes beyond
		// heartbeat timeout, worker never heartbeats.
		// Verify: activity status is TIMED_OUT.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           s.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        s.tv.ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			HeartbeatTimeout:    durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // No retries
			},
		})
		require.NoError(t, err)

		// Worker accepts task (starts the activity)
		pollTaskResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		// Long poll for completion (heartbeat timeout will fire)
		pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.TIMEOUT_TYPE_HEARTBEAT, pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
			"expected timeout type=Heartbeat but is %s", pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
	})

	t.Run("ActivityRetriesOnHeartbeatTimeout", func(t *testing.T) {
		// Start activity (with retries), worker accepts task, time passes beyond
		// heartbeat timeout, worker never heartbeats.
		// Verify: activity returns to SCHEDULED (or new task available).
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              s.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           s.tv.ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			HeartbeatTimeout:       durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Millisecond),
				MaximumAttempts: 2,
			},
		})
		require.NoError(t, err)

		// Attempt 1: worker accepts task
		pollTaskResp1, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollTaskResp1.Attempt)

		// Don't heartbeat - let it timeout and retry
		// Second attempt: worker accepts retry task
		pollTaskResp2, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollTaskResp2.Attempt)
	})

	t.Run("HeartbeatKeepsActivityAlive", func(t *testing.T) {
		// Start activity, worker accepts, worker heartbeats within timeout,
		// more time passes, worker heartbeats again, worker completes.
		// Verify: activity status is COMPLETED.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           s.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        s.tv.ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			HeartbeatTimeout:    durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // No retries - timeout would be terminal
			},
		})
		require.NoError(t, err)

		// Worker accepts task
		pollTaskResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		// Heartbeat before timeout
		time.Sleep(600 * time.Millisecond) //nolint:forbidigo
		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Wait again, then heartbeat again
		time.Sleep(600 * time.Millisecond) //nolint:forbidigo
		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Complete the activity
		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Verify activity completed successfully (didn't timeout)
		pollResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      s.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, pollResp.GetInfo().GetStatus(),
			"expected status=Completed but is %s", pollResp.GetInfo().GetStatus())
		protorequire.ProtoEqual(t, defaultResult, pollResp.GetOutcome().GetResult())
	})
}

func (s *standaloneActivityTestSuite) assertActivityExecutionInfo(
	t *testing.T,
	info *activitypb.ActivityExecutionInfo,
	activityID string,
	runID string,
	runStatus enumspb.ActivityExecutionStatus,
	runState enumspb.PendingActivityState,
) {
	t.Helper()
	require.Equal(t, activityID, info.ActivityId)
	require.Equal(t, runID, info.RunId)
	require.NotNil(t, info.ActivityType)
	require.Equal(t, s.tv.ActivityType(), info.ActivityType)
	require.Equal(t, runStatus, info.Status)
	require.Equal(t, runState, info.RunState)

	// TODO(dan): This test to be finalized when full API surface area implemented.
	if info.ScheduleTime != nil && info.ExpirationTime != nil {
		require.Less(t, info.ScheduleTime, info.ExpirationTime)
	}
	// info.Attempt
	// info.MaximumAttempts
	// info.Priority.PriorityKey
	// info.LastStartedTime
	// info.LastWorkerIdentity
	// info.HeartbeatDetails
	// info.LastHeartbeatTime
	// info.LastFailure
	// info.CurrentRetryInterval
	// info.LastAttemptCompleteTime
	// info.NextAttemptScheduleTime
	// info.LastDeploymentVersion
}

func (s *standaloneActivityTestSuite) pollActivityTaskQueue(ctx context.Context, taskQueue string) (*workflowservice.PollActivityTaskQueueResponse, error) {
	return s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test-identity",
	})
}

func (s *standaloneActivityTestSuite) startAndValidateActivity(
	ctx context.Context,
	t *testing.T,
	activityID string,
	taskQueue string,
) *workflowservice.StartActivityExecutionResponse {
	startResponse, err := s.startActivity(ctx, activityID, taskQueue)

	require.NoError(t, err)
	require.NotNil(t, startResponse.GetRunId())
	require.True(t, startResponse.Started)

	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResponse.RunId,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, startResponse.RunId, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus(),
		"expected Running but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState(),
		"expected Scheduled but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Nil(t, activityResp.Outcome)
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())

	return startResponse
}

func (s *standaloneActivityTestSuite) pollActivityTaskAndValidate(
	ctx context.Context,
	t *testing.T,
	activityID string,
	taskQueue string,
	runID string,
) *workflowservice.PollActivityTaskQueueResponse {
	pollTaskResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	require.Equal(t, activityID, pollTaskResp.GetActivityId())
	protorequire.ProtoEqual(t, s.tv.ActivityType(), pollTaskResp.GetActivityType())
	require.EqualValues(t, 1, pollTaskResp.Attempt)

	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus(),
		"expected Running but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, info.GetRunState(),
		"expected Started but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, s.tv.WorkerIdentity(), info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, activityResp.Outcome)
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())

	return pollTaskResp
}

func (s *standaloneActivityTestSuite) validateCompletion(
	ctx context.Context,
	t *testing.T,
	activityID string,
	runID string,
	workerIdentity string,
) {
	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.GetStatus(),
		"expected Completed but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState(),
		"expected Unspecified but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, workerIdentity, info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())
	protorequire.ProtoEqual(t, defaultResult, activityResp.GetOutcome().GetResult())
}

func (s *standaloneActivityTestSuite) validateFailure(
	ctx context.Context,
	t *testing.T,
	activityID string,
	runID string,
	expectedHeartbeatDetails *commonpb.Payloads,
	workerIdentity string,
) {
	activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, info.GetStatus(),
		"expected Failed but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState(),
		"expected Unspecified but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, workerIdentity, info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	protorequire.ProtoEqual(t, defaultFailure, info.GetLastFailure())
	protorequire.ProtoEqual(t, defaultFailure, activityResp.GetOutcome().GetFailure())

	if expectedHeartbeatDetails != nil {
		protorequire.ProtoEqual(t, expectedHeartbeatDetails, info.GetHeartbeatDetails())
	}
}

func (s *standaloneActivityTestSuite) validateBaseActivityResponse(
	t *testing.T,
	activityID string,
	expectedRunID string,
	response *workflowservice.DescribeActivityExecutionResponse,
) {
	require.NotNil(t, response.LongPollToken)
	require.Equal(t, activityID, response.GetInfo().GetActivityId())
	require.Equal(t, s.tv.ActivityType(), response.GetInfo().GetActivityType())
	require.Equal(t, expectedRunID, response.RunId)
	require.NotNil(t, response.GetInfo().GetScheduleTime())
	protorequire.ProtoEqual(t, defaultInput, response.GetInput())
}

func (s *standaloneActivityTestSuite) startActivity(ctx context.Context, activityID string, taskQueue string) (*workflowservice.StartActivityExecutionResponse, error) {
	return s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout: durationpb.New(1 * time.Minute),
		RequestId:           s.tv.RequestID(),
	})
}
