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
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
		IdReusePolicy: enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
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
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
		IdReusePolicy: enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
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
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
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

	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})
	require.NoError(t, err)

	info := activityResp.GetInfo()
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus())
	require.Equal(t, "Test Cancellation", info.GetCanceledReason())
	protorequire.ProtoEqual(t, details, activityResp.GetFailure().GetCanceledFailureInfo().GetDetails())
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

	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})
	require.NoError(t, err)

	info := activityResp.GetInfo()
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus())
	require.Equal(t, "Test Cancellation", info.GetCanceledReason())
	protorequire.ProtoEqual(t, details, activityResp.GetFailure().GetCanceledFailureInfo().GetDetails())
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

	// TODO: we should get the cancel request from heart beat once we implement it

	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})
	require.NoError(t, err)

	info := activityResp.GetInfo()
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, info.GetRunState())
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

			activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace:   s.Namespace().String(),
				ActivityId:  activityID,
				RunId:       runID,
				IncludeInfo: true,
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

		activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:      s.Namespace().String(),
			ActivityId:     activityID,
			RunId:          runID,
			IncludeInfo:    true,
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

	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, s.tv.WorkerIdentity(), info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, info.GetLastFailure())

	expectedFailure := &failurepb.Failure{
		Message:     "Test Termination",
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
	}
	protorequire.ProtoEqual(t, expectedFailure, activityResp.GetFailure())
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

func (s *standaloneActivityTestSuite) TestScheduleToCloseTimeout_WithRetry() {
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
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			// It's not possible to guarantee (e.g. via NextRetryDelay or RetryPolicy) that a retry
			// will start with a delay <1s because of the use of TimerProcessorMaxTimeShift in the
			// timer queue. Therefore we allow 1s for the ActivityDispatchTask to be executed, and
			// time out the activity 1s into Attempt 2.
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		},
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
	pollTaskResp, err = s.pollActivityTaskQueue(ctx, taskQueue)
	require.NoError(t, err)

	// Wait for schedule-to-close timeout.
	pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResp.RunId,
		IncludeInfo:    true,
		IncludeOutcome: true,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitCompletion{
			WaitCompletion: &workflowservice.PollActivityExecutionRequest_CompletionWaitOptions{},
		},
	})
	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, pollResp.GetInfo().GetStatus())
	require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, pollResp.GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

// TestStartToCloseTimeout tests that a start-to-close timeout is recorded after the activity is
// started. It also verifies that PollActivityExecution can be used to poll for a TimedOut state
// change caused by execution of a timer task.
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
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue.Name,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Second),
		},
		RequestId: "test-request-id",
	})
	require.NoError(t, err)

	// First poll: activity has not started yet
	pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:   s.Namespace().String(),
		ActivityId:  activityID,
		RunId:       startResp.RunId,
		IncludeInfo: true,
	})
	require.NoError(t, err)
	require.NotNil(t, pollResp)
	require.NotNil(t, pollResp.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, pollResp.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pollResp.GetInfo().GetRunState())

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
	pollResp, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:   s.Namespace().String(),
		ActivityId:  activityID,
		RunId:       startResp.RunId,
		IncludeInfo: true,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
				LongPollToken: pollResp.StateChangeLongPollToken,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, pollResp)
	require.NotNil(t, pollResp.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, pollResp.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, pollResp.GetInfo().GetRunState())

	// Third poll: activity has timed out
	pollResp, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResp.RunId,
		IncludeInfo:    true,
		IncludeOutcome: true,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
				LongPollToken: pollResp.StateChangeLongPollToken,
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, pollResp)
	require.NotNil(t, pollResp.GetInfo())

	// The activity has timed out due to StartToClose. This is an attempt failure, therefore the
	// failure should be in ActivityExecutionInfo.LastFailure as well as set as the outcome failure.
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, pollResp.GetInfo().GetStatus())
	failure := pollResp.GetInfo().GetLastFailure()
	require.NotNil(t, failure)
	timeoutFailure := failure.GetTimeoutFailureInfo()
	require.NotNil(t, timeoutFailure)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutFailure.GetTimeoutType())

	require.NotNil(t, pollResp.GetFailure())
	protorequire.ProtoEqual(t, failure, pollResp.GetFailure())
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, pollResp.GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", pollResp.GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_NoWait() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()

	startResp, err := s.startActivity(ctx, activityID, taskQueue.Name)
	require.NoError(t, err)

	t.Run("MinimalResponse", func(t *testing.T) {
		pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			// Omit RunID to verify that latest run will be used
			IncludeInfo:    false,
			IncludeInput:   false,
			IncludeOutcome: false,
		})
		require.NoError(t, err)
		require.NotNil(t, pollResp.StateChangeLongPollToken)
		require.Equal(t, startResp.RunId, pollResp.RunId)
		require.Nil(t, pollResp.Info)
		require.Nil(t, pollResp.Input)
		require.Nil(t, pollResp.GetResult())
		require.Nil(t, pollResp.GetFailure())
	})

	t.Run("FullResponse", func(t *testing.T) {
		pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:      s.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeInfo:    true,
			IncludeInput:   true,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.NotNil(t, pollResp.StateChangeLongPollToken)
		require.NotNil(t, pollResp.Info)
		s.assertActivityExecutionInfo(
			t,
			pollResp.Info,
			activityID,
			startResp.RunId,
			enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		)
		protorequire.ProtoEqual(t, defaultInput, pollResp.Input)

		// Activity is scheduled but not completed, so no outcome yet
		require.Nil(t, pollResp.GetResult())
		require.Nil(t, pollResp.GetFailure())
	})
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_WaitAnyStateChange() {
	// Long poll for any state change. PollActivityTaskQueue is used to cause a state change.
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()

	startResp, err := s.startActivity(ctx, activityID, taskQueue.Name)
	require.NoError(t, err)

	// First poll lacks token and therefore responds immediately, returning a token
	firstPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId, // RunID is now required by validation
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{},
		},
		IncludeInfo:  true,
		IncludeInput: true,
	})
	require.NoError(t, err)
	require.NotNil(t, firstPollResp.StateChangeLongPollToken)
	require.NotNil(t, firstPollResp.Info)
	require.Equal(t, firstPollResp.RunId, startResp.RunId)
	s.assertActivityExecutionInfo(
		t,
		firstPollResp.Info,
		activityID,
		startResp.RunId,
		enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
		enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
	)

	taskQueuePollErr := make(chan error, 1)
	activityPollDone := make(chan struct{})
	var activityPollResp *workflowservice.PollActivityExecutionResponse
	var activityPollErr error

	go func() {
		defer close(activityPollDone)
		// Second poll uses token and therefore waits for a state transition
		activityPollResp, activityPollErr = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:    s.Namespace().String(),
			ActivityId:   activityID,
			RunId:        startResp.RunId,
			IncludeInfo:  true,
			IncludeInput: true,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: firstPollResp.StateChangeLongPollToken,
				},
			},
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
		require.NoError(t, activityPollErr)
		require.NotNil(t, activityPollResp)
		require.NotNil(t, activityPollResp.Info)
		s.assertActivityExecutionInfo(
			t,
			activityPollResp.Info,
			activityID,
			startResp.RunId,
			enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			enumspb.PENDING_ACTIVITY_STATE_STARTED,
		)
		protorequire.ProtoEqual(t, defaultInput, activityPollResp.Input)

	case <-ctx.Done():
		t.Fatal("PollActivityExecution timed out")
	}

	err = <-taskQueuePollErr
	require.NoError(t, err)
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_WaitCompletion() {
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
				protorequire.ProtoEqual(t, defaultResult, response.GetResult())
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
				protorequire.ProtoEqual(t, defaultFailure, response.GetInfo().GetLastFailure())
				protorequire.ProtoEqual(t, defaultFailure, response.GetFailure())
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

			activityPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace:      s.Namespace().String(),
				ActivityId:     activityID,
				RunId:          startResp.RunId,
				IncludeInfo:    true,
				IncludeInput:   true,
				IncludeOutcome: true,
				WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitCompletion{
					WaitCompletion: &workflowservice.PollActivityExecutionRequest_CompletionWaitOptions{},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, activityPollResp)
			require.NotNil(t, activityPollResp.Info)
			s.assertActivityExecutionInfo(
				t,
				activityPollResp.Info,
				activityID,
				startResp.RunId,
				tc.expectedStatus,
				enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED,
			)

			protorequire.ProtoEqual(t, defaultInput, activityPollResp.GetInput())
			tc.completionValidationFn(t, activityPollResp)
		})
	}
}

// TODO(dan): add tests that PollActivityExecution can wait for deletion, termination, cancellation etc

func (s *standaloneActivityTestSuite) TestPollActivityExecution_DeadlineExceeded() {
	t := s.T()
	ctx := testcore.NewContext()

	// Start an activity and get initial long-poll state token
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()
	startResp, err := s.startActivity(ctx, activityID, taskQueue.Name)
	require.NoError(t, err)
	pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{},
		},
	})
	require.NoError(t, err)

	// The PollActivityExecution calls below use a long-poll token and will necessarily time out,
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

		// PollActivityExecution will return when this long poll timeout expires.
		s.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)

		pollResp, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:   s.Namespace().String(),
			ActivityId:  activityID,
			RunId:       startResp.RunId,
			IncludeInfo: true,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: pollResp.StateChangeLongPollToken,
				},
			},
		})
		// The server uses an empty non-error response to indicate to the caller that it should resubmit
		// its long-poll.
		require.NoError(t, err)
		require.Empty(t, pollResp.GetInfo())
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
		// PollActivityExecution will return when this long poll timeout expires.
		s.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)

		_, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: pollResp.StateChangeLongPollToken,
				},
			},
		})
		require.NoError(t, err)
		require.Empty(t, pollResp.GetInfo())
	})

	// Case 3: caller sets a deadline that is < the buffer. In this case PollActivityExecution will
	// return an empty result immediately, and there is a race between caller receiving that and
	// caller's client timing out the request. Therefore we do not test this.
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

	t.Run("LongPollNonExistentActivity", func(t *testing.T) {
		// Poll to get a token
		validPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: existingActivityID,
			RunId:      existingRunID,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: nil,
				},
			},
		})
		require.NoError(t, err)

		// Use the token with a non-existent activity
		_, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: "non-existent-activity",
			RunId:      existingRunID,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: validPollResp.StateChangeLongPollToken,
				},
			},
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, "activity execution not found", notFoundErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_InvalidArgument() {

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
		request     *workflowservice.PollActivityExecutionRequest
		expectedErr string
	}{
		{
			name: "EmptyNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "",
				ActivityId: validActivityID,
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
				ActivityId: validActivityID,
				RunId:      "invalid-uuid",
			},
			expectedErr: "invalid run id",
		},
		{
			name: "RunIdNotRequiredWhenWaitPolicyAbsent",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "",
			},
			expectedErr: "",
		},
		{
			name: "RunIdNotRequiredWhenLongPollTokenAbsent",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "",
				WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
					WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
						LongPollToken: nil,
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "RunIdRequiredWhenLongPollTokenPresent",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: validActivityID,
				RunId:      "",
				WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
					WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
						LongPollToken: []byte("valid-token"),
					},
				},
			},
			expectedErr: "run id is required",
		},
		{
			name: "MalformedLongPollToken",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      existingRunID,
				WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
					WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
						LongPollToken: []byte("invalid-token"),
					},
				},
			},
			expectedErr: "invalid long poll token",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.FrontendClient().PollActivityExecution(ctx, tc.request)
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
		validPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: existingActivityID,
			RunId:      existingRunID,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: nil,
				},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, validPollResp.StateChangeLongPollToken)

		activityID2 := s.tv.Any().String()
		startResp2, err := s.startActivity(ctx, activityID2, tq.Name)
		require.NoError(t, err)
		require.NotEmpty(t, startResp2.GetRunId())

		_, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: activityID2,
			RunId:      startResp2.GetRunId(),
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: validPollResp.StateChangeLongPollToken,
				},
			},
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "long poll token does not match execution", invalidArgErr.Message)
	})

	// TODO(dan): add test for long poll token from non-existent execution
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
	if info.ScheduledTime != nil && info.ExpirationTime != nil {
		require.Less(t, info.ScheduledTime, info.ExpirationTime)
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

	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResponse.RunId,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, startResponse.RunId, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
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

	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, info.GetRunState())
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
	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, workerIdentity, info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())
	protorequire.ProtoEqual(t, defaultResult, activityResp.GetResult())
}

func (s *standaloneActivityTestSuite) validateFailure(
	ctx context.Context,
	t *testing.T,
	activityID string,
	runID string,
	expectedHeartbeatDetails *commonpb.Payloads,
	workerIdentity string,
) {
	activityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInfo:    true,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	s.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, workerIdentity, info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	protorequire.ProtoEqual(t, defaultFailure, info.GetLastFailure())
	protorequire.ProtoEqual(t, defaultFailure, activityResp.GetFailure())

	if expectedHeartbeatDetails != nil {
		protorequire.ProtoEqual(t, expectedHeartbeatDetails, info.GetHeartbeatDetails())
	}
}

func (s *standaloneActivityTestSuite) validateBaseActivityResponse(
	t *testing.T,
	activityID string,
	expectedRunID string,
	response *workflowservice.PollActivityExecutionResponse,
) {
	require.NotNil(t, response.StateChangeLongPollToken)
	require.Equal(t, activityID, response.GetInfo().GetActivityId())
	require.Equal(t, s.tv.ActivityType(), response.GetInfo().GetActivityType())
	require.Equal(t, expectedRunID, response.RunId)
	require.NotNil(t, response.GetInfo().GetScheduledTime())
	protorequire.ProtoEqual(t, defaultInput, response.GetInput())
}

func (s *standaloneActivityTestSuite) startActivity(ctx context.Context, activityID string, taskQueue string) (*workflowservice.StartActivityExecutionResponse, error) {
	return s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
		RequestId: s.tv.RequestID(),
	})
}
