package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/operatorservice/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultStartToCloseTimeout = 1 * time.Minute
	defaultIdentity            = "test-worker"
)

var (
	defaultInput            = payloads.EncodeString("Input")
	defaultHeartbeatDetails = payloads.EncodeString("Heartbeat Details")
	defaultResult           = payloads.EncodeString("Done")
	defaultRetryPolicy      = &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(time.Second),
		BackoffCoefficient: 2.0,
		MaximumAttempts:    0,
		MaximumInterval:    durationpb.New(100 * time.Second),
	}
	defaultFailure = &failurepb.Failure{
		Message: "Failed Activity",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "Test",
			NonRetryable: true,
		}},
	}
	defaultHeader = &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"key-1": payload.EncodeString("value-1"),
			"key-2": payload.EncodeString("value-2"),
		},
	}
	defaultSearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": payload.EncodeString("value1"),
		},
	}
	defaultUserMetadata = &sdkpb.UserMetadata{
		Summary: payload.EncodeString("test-summary"),
		Details: payload.EncodeString("test-details"),
	}
	defaultMaxIDLengthLimit = dynamicconfig.MaxIDLengthLimit.Get(
		dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), log.NewNoopLogger()))()
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

func (s *standaloneActivityTestSuite) TestIDReusePolicy() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("RejectDuplicate", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("AllowDuplicateFailedOnly", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})
}

func (s *standaloneActivityTestSuite) TestIDConflictPolicy() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("Fail", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		firstStartResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)

		startWithFail := func(requestID string) (*workflowservice.StartActivityExecutionResponse, error) {
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
				RequestId:           requestID,
			})
		}

		t.Run("SecondStartFails", func(t *testing.T) {
			_, err := startWithFail("different-request-id")
			var alreadyStartedErr *serviceerror.ActivityExecutionAlreadyStarted
			require.ErrorAs(t, err, &alreadyStartedErr)
			require.Equal(t, s.tv.RequestID(), alreadyStartedErr.StartRequestId)
			require.Equal(t, firstStartResp.GetRunId(), alreadyStartedErr.RunId)
		})

		t.Run("SecondStartWithSameRequestIdReturnsExistingRun", func(t *testing.T) {
			resp, err := startWithFail(s.tv.RequestID())
			require.NoError(t, err)
			require.Equal(t, firstStartResp.RunId, resp.RunId)
			require.False(t, resp.GetStarted())
		})
	})

	t.Run("UseExisting", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		firstStartResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)

		startWithUseExisting := func(requestID string) (*workflowservice.StartActivityExecutionResponse, error) {
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
				IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
				RequestId:           requestID,
			})
		}

		t.Run("SecondStartReturnsExistingRun", func(t *testing.T) {
			resp, err := startWithUseExisting("different-request-id")
			require.NoError(t, err)
			require.Equal(t, firstStartResp.RunId, resp.RunId)
			require.False(t, resp.GetStarted())
		})
		t.Run("SecondStartWithSameRequestIdReturnsExistingRun", func(t *testing.T) {
			resp, err := startWithUseExisting(s.tv.RequestID())
			require.NoError(t, err)
			require.Equal(t, firstStartResp.RunId, resp.RunId)
			require.False(t, resp.GetStarted())
		})

		t.Run("DoesNotApplyToCompletedActivity", func(t *testing.T) {
			pollTaskResp := s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, firstStartResp.RunId)
			_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
				Namespace: s.Namespace().String(),
				TaskToken: pollTaskResp.TaskToken,
				Result:    defaultResult,
				Identity:  defaultIdentity,
			})
			require.NoError(t, err)

			// USE_EXISTING only applies to running activities; completed activities
			// are governed by reuse policy (default ALLOW_DUPLICATE creates new)
			resp, err := startWithUseExisting("different-request-id")
			require.NoError(t, err)
			require.NotEqual(t, firstStartResp.RunId, resp.RunId)
			require.True(t, resp.GetStarted())
		})
	})
}

func (s *standaloneActivityTestSuite) TestPollActivityTaskQueue() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())
	namespace := s.Namespace().String()

	startToCloseTimeout := durationpb.New(1 * time.Minute)
	scheduleToCloseTimeout := durationpb.New(2 * time.Minute)
	heartbeatTimeout := durationpb.New(20 * time.Second)
	priority := &commonpb.Priority{
		FairnessKey: "test-key",
	}

	startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    namespace,
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout:    startToCloseTimeout,
		ScheduleToCloseTimeout: scheduleToCloseTimeout,
		HeartbeatTimeout:       heartbeatTimeout,
		RequestId:              s.tv.RequestID(),
		Priority:               priority,
		Header:                 defaultHeader,
	})
	require.NoError(t, err)

	pollTaskResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	require.Equal(t, activityID, pollTaskResp.GetActivityId())
	require.Equal(t, namespace, pollTaskResp.GetWorkflowNamespace())
	protorequire.ProtoEqual(t, s.tv.ActivityType(), pollTaskResp.GetActivityType())
	require.Equal(t, startResp.GetRunId(), pollTaskResp.GetActivityRunId())
	protorequire.ProtoEqual(t, defaultInput, pollTaskResp.GetInput())
	require.False(t, pollTaskResp.GetStartedTime().AsTime().IsZero())
	require.False(t, pollTaskResp.GetScheduledTime().AsTime().IsZero())
	require.EqualValues(t, 1, pollTaskResp.Attempt)
	protorequire.ProtoEqual(t, startToCloseTimeout, pollTaskResp.GetStartToCloseTimeout())
	protorequire.ProtoEqual(t, scheduleToCloseTimeout, pollTaskResp.GetScheduleToCloseTimeout())
	protorequire.ProtoEqual(t, heartbeatTimeout, pollTaskResp.GetHeartbeatTimeout())
	protorequire.ProtoEqual(t, priority, pollTaskResp.GetPriority())
	protorequire.ProtoEqual(t, defaultHeader, pollTaskResp.GetHeader())
	require.NotNil(t, pollTaskResp.TaskToken)
}

func (s *standaloneActivityTestSuite) TestStart() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("RequestIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:    s.Namespace().String(),
				ActivityId:   s.tv.ActivityID(),
				ActivityType: s.tv.ActivityType(),
				Identity:     s.tv.WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: s.tv.TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           string(make([]byte, defaultMaxIDLengthLimit+1)),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("request ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:    s.Namespace().String(),
				ActivityId:   s.tv.ActivityID(),
				ActivityType: s.tv.ActivityType(),
				Identity:     string(make([]byte, defaultMaxIDLengthLimit+1)),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: s.tv.TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           s.tv.RequestID(),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InputTooLarge", func(t *testing.T) {
			blobSizeLimitError := 1000
			cleanup := s.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			input := payloads.EncodeString(string(make([]byte, blobSizeLimitError+1)))

			_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:    s.Namespace().String(),
				ActivityId:   s.tv.ActivityID(),
				ActivityType: s.tv.ActivityType(),
				Identity:     s.tv.WorkerIdentity(),
				Input:        input,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: s.tv.TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           s.tv.RequestID(),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "input exceeds length limit")
		})

		t.Run("SearchAttributesInvalid", func(t *testing.T) {
			invalidSearchAttributes := &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"InvalidSearchAttributeKey": payload.EncodeString("value"),
				},
			}

			_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:    s.Namespace().String(),
				ActivityId:   s.tv.ActivityID(),
				ActivityType: s.tv.ActivityType(),
				Identity:     s.tv.WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: s.tv.TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           s.tv.RequestID(),
				SearchAttributes:    invalidSearchAttributes,
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
		})
	})
}

func (s *standaloneActivityTestSuite) TestComplete() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("ByToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("ByIDWithRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("ByIDWithoutRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("StaleToken", func(t *testing.T) {
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

		// Complete with stale token (activity already completed)
		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to complete with old token.
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

		// Try to complete with the old attempt 1 token - should fail with NotFound
		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Result:    defaultResult,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")

		// Complete with the attempt 2 token and should succeed
		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedTokenNamespace", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to complete with existing token
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
			Result:    defaultResult,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestFail() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("ByToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("WithHeartbeatDetails", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("ByIDWithRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("ByIDWithoutRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := s.FrontendClient().RespondActivityTaskFailedById(ctx, &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			Failure:    defaultFailure,
			Identity:   s.tv.WorkerIdentity(),
		})
		require.NoError(t, err)

		s.validateFailure(ctx, t, activityID, runID, nil, s.tv.WorkerIdentity())
	})

	t.Run("StaleToken", func(t *testing.T) {
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

		// Fail with stale token (activity already completed)
		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure:   defaultFailure,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to complete with old token.
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

		// Try to fail with the old attempt 1 token - should fail with NotFound
		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure:   defaultFailure,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")

		// Fail with the attempt 2 token and should be no error
		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Failure:   defaultFailure,
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedTokenNamespace", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to fail with existing token
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
			Failure:   defaultFailure,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestRequestCancel() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("ByToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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

		heartbeatResp, err := s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

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
		require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0))
		require.NotNil(t, info.GetCloseTime())
		protorequire.ProtoEqual(t, details, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetDetails())
	})

	testByIDCases := []struct {
		name         string
		includeRunID bool
	}{
		{
			name:         "with run ID",
			includeRunID: true,
		},
		{
			name:         "without run ID",
			includeRunID: false,
		},
	}
	for _, tc := range testByIDCases {
		s.Run(tc.name, func() {
			t := s.T()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)

			activityID := testcore.RandomizeStr(tc.name)
			taskQueue := testcore.RandomizeStr(tc.name)

			startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
			runID := startResp.RunId

			s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
				Identity:   "cancelling-worker",
				RequestId:  s.tv.RequestID(),
				Reason:     "Test Cancellation",
			})
			require.NoError(t, err)

			heartbeatResp, err := s.FrontendClient().RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

			details := &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					payload.EncodeString("Canceled Details"),
				},
			}

			cancelReq := &workflowservice.RespondActivityTaskCanceledByIdRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				Details:    details,
				Identity:   "new-worker",
			}

			if tc.includeRunID {
				cancelReq.RunId = runID
			}

			_, err = s.FrontendClient().RespondActivityTaskCanceledById(ctx, cancelReq)
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
		})
	}

	t.Run("FailsIfNeverRequested", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
	})

	t.Run("DuplicateRequestIDSucceeds", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		for i := 0; i < 2; i++ {
			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
				Identity:   "cancelling-worker",
				RequestId:  "cancel-request-id",
				Reason:     "Test Cancellation",
			})
			require.NoError(t, err)
		}

		heartbeatResp, err := s.FrontendClient().RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

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
	})

	t.Run("DifferentRequestIDFails", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "cancelling-worker",
			RequestId:  "cancel-request-id",
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "cancelling-worker",
			RequestId:  "different-cancel-request-id",
			Reason:     "Test Cancellation",
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	testAfterFinishedCases := []struct {
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
	for _, tc := range testAfterFinishedCases {
		s.Run(tc.name, func() {
			t := s.T()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

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

	t.Run("Retry", func(t *testing.T) {
		runTest := func(t *testing.T, requestCancellation bool) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

			startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:              s.Namespace().String(),
				ActivityId:             activityID,
				ActivityType:           s.tv.ActivityType(),
				TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
				ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
					MaximumAttempts: 2,
				},
			})
			require.NoError(t, err)
			runID := startResp.RunId

			pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: s.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, pollResp.Attempt)

			if requestCancellation {
				_, err = s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Identity:   "test",
					RequestId:  s.tv.RequestID(),
				})
				require.NoError(t, err)
			}

			retryableFailure := &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			}
			_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
				Namespace: s.Namespace().String(),
				TaskToken: pollResp.TaskToken,
				Failure:   retryableFailure,
			})
			require.NoError(t, err)

			activityResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
			require.NoError(t, err)

			if requestCancellation {
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, activityResp.GetInfo().GetStatus())
			} else {
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, activityResp.GetInfo().GetStatus())
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, activityResp.GetInfo().GetRunState())
			}
		}
		t.Run("AllowedIfNotCancelRequested", func(t *testing.T) {
			runTest(t, false)
		})
		t.Run("PreventedIfCancelRequested", func(t *testing.T) {
			runTest(t, true)
		})
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyActivityID", func(t *testing.T) {
			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace: s.Namespace().String(),
				Reason:    "Test Cancellation",
				Identity:  "cancelling-worker",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  s.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   "cancelling-worker",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("RequestIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RequestId:  string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  s.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   "cancelling-worker",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("request ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  s.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
				Namespace:  s.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   "cancelling-worker",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})

		t.Run("ReasonTooLong", func(t *testing.T) {
			blobSizeLimitError := 1000
			cleanup := s.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  s.Namespace().String(),
				Reason:     string(make([]byte, blobSizeLimitError+1)),
				Identity:   "cancelling-worker",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "reason exceeds length limit", invalidArgErr.Message)
		})
	})

	t.Run("ImmediatelyCancelled_WhenInScheduledState", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
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
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus())
	})

	t.Run("StaleToken", func(t *testing.T) {
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

		// Fail with stale token (activity already completed)
		_, err = s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to complete with old token.
		// Use NextRetryDelay=1s to ensure the retry dispatch happens within test timeout.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
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

		_, err = s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			RequestId:  s.tv.RequestID(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		// Try to cancel with the old attempt 1 token - should fail with NotFound
		_, err = s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), "activity task not found")

		// Heartbeat then cancel with the attempt 2 token and should be no error
		heartbeatResp, err := s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Failure:   defaultFailure,
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedTokenNamespace", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to cancel with existing token
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})

	t.Run("NonExistent", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())

		_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			Reason:     "Test Cancellation",
			Identity:   "canceller",
		})

		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, fmt.Sprintf("activity not found for ID: %s", activityID), notFoundErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestTerminate() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("TerminatedSuccessfully", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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
		require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0)) // Terminated doesn't set attempt completion, thus expect 0 here
		require.NotNil(t, info.GetCloseTime())
		require.Equal(t, s.tv.WorkerIdentity(), info.GetLastWorkerIdentity())
		require.NotNil(t, info.GetLastStartedTime())
		require.Nil(t, info.GetLastFailure())

		expectedFailure := &failurepb.Failure{
			Message:     "Test Termination",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
		}
		protorequire.ProtoEqual(t, expectedFailure, activityResp.GetOutcome().GetFailure())
	})

	t.Run("AlreadyCompletedCannotTerminate", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

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

	})

	t.Run("DuplicateRequestIDSucceeds", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		require.NoError(t, err)
	})

	t.Run("DifferentRequestIDFails", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		s.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id-2",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	t.Run("NonExistent", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())

		_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})

		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, fmt.Sprintf("activity not found for ID: %s", activityID), notFoundErr.Message)
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyActivityID", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
				Namespace: s.Namespace().String(),
				Reason:    "Test Termination",
				Identity:  "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  s.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("RequestIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RequestId:  string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  s.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("request ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  s.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
				Namespace:  s.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})

		t.Run("ReasonTooLong", func(t *testing.T) {
			blobSizeLimitError := 1000
			cleanup := s.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  s.Namespace().String(),
				Reason:     string(make([]byte, blobSizeLimitError+1)),
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "reason exceeds length limit", invalidArgErr.Message)
		})
	})
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

	describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.Greater(t, describeResp.GetInfo().GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, describeResp.GetInfo().GetCloseTime().AsTime().IsZero())
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
	describeResp1, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.NotNil(t, describeResp1)
	require.NotNil(t, describeResp1.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp1.GetInfo().GetStatus(),
		"expected Running but is %s", describeResp1.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, describeResp1.GetInfo().GetRunState(),
		"expected Scheduled but is %s", describeResp1.GetInfo().GetRunState())

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
	describeResp2, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:     s.Namespace().String(),
		ActivityId:    activityID,
		RunId:         startResp.RunId,
		LongPollToken: describeResp1.LongPollToken,
	})
	require.NoError(t, err)
	require.NotNil(t, describeResp2)
	require.NotNil(t, describeResp2.GetInfo())
	require.Greater(t, describeResp2.GetInfo().GetStateTransitionCount(), describeResp1.GetInfo().GetStateTransitionCount())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp2.GetInfo().GetStatus(),
		"expected Running but is %s", describeResp2.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, describeResp2.GetInfo().GetRunState(),
		"expected Started but is %s", describeResp2.GetInfo().GetRunState())

	// Third poll: activity has timed out
	describeResp3, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResp.RunId,
		IncludeOutcome: true,
		LongPollToken:  describeResp2.LongPollToken,
	})

	require.NoError(t, err)
	require.NotNil(t, describeResp3)
	require.NotNil(t, describeResp3.GetInfo())
	require.Greater(t, describeResp3.GetInfo().GetStateTransitionCount(), describeResp2.GetInfo().GetStateTransitionCount())

	// The activity has timed out due to StartToClose. This is an attempt failure, therefore the
	// failure should be in ActivityExecutionInfo.LastFailure as well as set as the outcome failure.
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, describeResp3.GetInfo().GetStatus(),
		"expected TimedOut but is %s", describeResp3.GetInfo().GetStatus())
	require.Greater(t, describeResp3.GetInfo().GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, describeResp3.GetInfo().GetCloseTime().AsTime().IsZero())
	failure := describeResp3.GetInfo().GetLastFailure()
	require.NotNil(t, failure)
	timeoutFailure := failure.GetTimeoutFailureInfo()
	require.NotNil(t, timeoutFailure)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutFailure.GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", timeoutFailure.GetTimeoutType())

	require.NotNil(t, describeResp3.GetOutcome().GetFailure())
	protorequire.ProtoEqual(t, failure, describeResp3.GetOutcome().GetFailure())
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, describeResp3.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", describeResp3.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

// TestScheduleToStartTimeout tests that a schedule-to-start timeout is recorded after the activity is
// created but never started. It also verifies that DescribeActivityExecution can be used to long-poll for a TimedOut
// state change caused by execution of a timer task.
func (s *standaloneActivityTestSuite) TestScheduleToStartTimeout() {
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
		ScheduleToStartTimeout: durationpb.New(100 * time.Millisecond),
		StartToCloseTimeout:    durationpb.New(30 * time.Second),
		RequestId:              "test-request-id",
	})
	require.NoError(t, err)

	describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)

	// Long poll to await activity timeout
	describeResp, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      s.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResp.RunId,
		IncludeOutcome: true,
		LongPollToken:  describeResp.LongPollToken,
	})

	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, describeResp.GetInfo().GetStatus(),
		"expected TimedOut but is %s", describeResp.GetInfo().GetStatus())
	require.Greater(t, describeResp.GetInfo().GetExecutionDuration().AsDuration(), time.Duration(0))
	require.NotNil(t, describeResp.GetInfo().GetCloseTime())
	require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, describeResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected ScheduleToStartTimeout but is %s", describeResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_NoWait() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := s.tv.ActivityID()
	taskQueue := s.tv.TaskQueue()

	startReq := &workflowservice.StartActivityExecutionRequest{
		Namespace:              s.Namespace().String(),
		ActivityId:             activityID,
		ActivityType:           s.tv.ActivityType(),
		Header:                 defaultHeader,
		HeartbeatTimeout:       durationpb.New(45 * time.Second),
		Identity:               s.tv.WorkerIdentity(),
		Input:                  defaultInput,
		ScheduleToStartTimeout: durationpb.New(30 * time.Second),
		ScheduleToCloseTimeout: durationpb.New(3 * time.Minute),
		StartToCloseTimeout:    durationpb.New(1 * time.Minute),
		RequestId:              s.tv.RequestID(),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(2 * time.Second),
			BackoffCoefficient: 1.5,
			MaximumAttempts:    3,
			MaximumInterval:    durationpb.New(111 * time.Second),
		},
		Priority: &commonpb.Priority{
			FairnessKey: "test-key",
		},
		SearchAttributes: defaultSearchAttributes,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue.GetName(),
		},
		UserMetadata: defaultUserMetadata,
	}

	startResp, err := s.FrontendClient().StartActivityExecution(ctx, startReq)
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

		respInfo := describeResp.GetInfo()
		require.NotNil(t, describeResp.LongPollToken)
		require.NotNil(t, respInfo)

		expectedExpirationTime := timestamppb.New(respInfo.GetScheduleTime().AsTime().Add(
			startReq.GetScheduleToCloseTimeout().AsDuration()))

		expected := &activitypb.ActivityExecutionInfo{
			ActivityId:             activityID,
			ActivityType:           s.tv.ActivityType(),
			Attempt:                1,
			ExpirationTime:         expectedExpirationTime,
			Header:                 defaultHeader,
			HeartbeatTimeout:       startReq.GetHeartbeatTimeout(),
			RetryPolicy:            startReq.GetRetryPolicy(),
			RunId:                  startResp.RunId,
			RunState:               enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			Priority:               startReq.GetPriority(),
			ScheduleToCloseTimeout: startReq.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: startReq.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    startReq.GetStartToCloseTimeout(),
			Status:                 enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			SearchAttributes:       defaultSearchAttributes,
			TaskQueue:              taskQueue.Name,
			UserMetadata:           defaultUserMetadata,
		}

		diff := cmp.Diff(expected, respInfo,
			protocmp.Transform(),
			// Ignore non-deterministic fields. Validated separately.
			protocmp.IgnoreFields(&activitypb.ActivityExecutionInfo{},
				"execution_duration",
				"schedule_time",
				"state_transition_count",
			),
		)
		require.Empty(t, diff)
		require.Equal(t, respInfo.GetExecutionDuration().AsDuration(), time.Duration(0)) // Never completed, so expect 0
		require.Nil(t, describeResp.GetInfo().GetCloseTime())
		require.Positive(t, respInfo.GetScheduleTime().AsTime().Unix())
		require.Positive(t, respInfo.GetStateTransitionCount())

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

	expected := &activitypb.ActivityExecutionInfo{
		ActivityId:             activityID,
		ActivityType:           s.tv.ActivityType(),
		Attempt:                1,
		HeartbeatTimeout:       durationpb.New(0),
		RetryPolicy:            defaultRetryPolicy,
		RunId:                  startResp.RunId,
		RunState:               enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		SearchAttributes:       &commonpb.SearchAttributes{},
		ScheduleToCloseTimeout: durationpb.New(0),
		ScheduleToStartTimeout: durationpb.New(0),
		StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
		Status:                 enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
		TaskQueue:              taskQueue.Name,
	}
	diff := cmp.Diff(expected, firstDescribeResp.GetInfo(),
		protocmp.Transform(),
		// Ignore non-deterministic fields. Validated separately.
		protocmp.IgnoreFields(&activitypb.ActivityExecutionInfo{},
			"execution_duration",
			"schedule_time",
			"state_transition_count",
		),
	)
	require.Empty(t, diff)

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

		expected := &activitypb.ActivityExecutionInfo{
			ActivityId:             activityID,
			ActivityType:           s.tv.ActivityType(),
			Attempt:                1,
			HeartbeatTimeout:       durationpb.New(0),
			LastWorkerIdentity:     defaultIdentity,
			RetryPolicy:            defaultRetryPolicy,
			RunId:                  startResp.RunId,
			RunState:               enumspb.PENDING_ACTIVITY_STATE_STARTED,
			ScheduleToCloseTimeout: durationpb.New(0),
			ScheduleToStartTimeout: durationpb.New(0),
			SearchAttributes:       &commonpb.SearchAttributes{},
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			TaskQueue:              taskQueue.Name,
		}
		diff := cmp.Diff(expected, describeResp.GetInfo(),
			protocmp.Transform(),
			// Ignore non-deterministic fields. Validated separately.
			protocmp.IgnoreFields(&activitypb.ActivityExecutionInfo{},
				"execution_duration",
				"last_started_time",
				"schedule_time",
				"state_transition_count",
			),
		)
		require.Empty(t, diff)

		protorequire.ProtoEqual(t, defaultInput, describeResp.Input)

	case <-ctx.Done():
		t.Fatal("DescribeActivityExecution timed out")
	}

	err = <-taskQueuePollErr
	require.NoError(t, err)
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution_Completed() {
	testCases := []struct {
		name             string
		expectedStatus   enumspb.ActivityExecutionStatus
		taskCompletionFn func(ctx context.Context, taskToken []byte, activityID, runID string) error
		outcomeValidator func(*testing.T, *workflowservice.DescribeActivityExecutionResponse)
	}{
		{
			name:           "successful completion",
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
				_, err := s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
					Result:    defaultResult,
					Identity:  defaultIdentity,
				})
				return err
			},
			outcomeValidator: func(t *testing.T, response *workflowservice.DescribeActivityExecutionResponse) {
				protorequire.ProtoEqual(t, defaultResult, response.GetOutcome().GetResult())
				require.Nil(t, response.GetOutcome().GetFailure())
			},
		},
		{
			name:           "failure completion",
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
			taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
				_, err := s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
					Failure:   defaultFailure,
					Identity:  defaultIdentity,
				})
				return err
			},
			outcomeValidator: func(t *testing.T, response *workflowservice.DescribeActivityExecutionResponse) {
				protorequire.ProtoEqual(t, defaultFailure, response.GetOutcome().GetFailure())
				require.Nil(t, response.GetOutcome().GetResult())
			},
		},
		{
			name:           "termination",
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
			taskCompletionFn: func(ctx context.Context, _ []byte, activityID, runID string) error {
				_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Reason:     "test termination",
				})
				return err
			},
			outcomeValidator: func(t *testing.T, response *workflowservice.DescribeActivityExecutionResponse) {
				require.NotNil(t, response.GetOutcome().GetFailure())
				require.NotNil(t, response.GetOutcome().GetFailure().GetTerminatedFailureInfo())
				require.Nil(t, response.GetOutcome().GetResult())
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			t := s.T()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			activityID := s.tv.Any().String()
			taskQueue := s.tv.TaskQueue()

			startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:           s.Namespace().String(),
				ActivityId:          activityID,
				ActivityType:        s.tv.ActivityType(),
				Header:              defaultHeader,
				HeartbeatTimeout:    durationpb.New(45 * time.Second),
				Identity:            s.tv.WorkerIdentity(),
				Input:               defaultInput,
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           s.tv.RequestID(),
				RetryPolicy:         defaultRetryPolicy,
				Priority:            &commonpb.Priority{FairnessKey: "test-key"},
				SearchAttributes:    defaultSearchAttributes,
				TaskQueue:           taskQueue,
				UserMetadata:        defaultUserMetadata,
			})
			require.NoError(t, err)

			pollTaskResp, err := s.pollActivityTaskQueue(ctx, taskQueue.Name)
			require.NoError(t, err)
			err = tc.taskCompletionFn(ctx, pollTaskResp.TaskToken, activityID, startResp.RunId)
			require.NoError(t, err)

			describeResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:      s.Namespace().String(),
				ActivityId:     activityID,
				RunId:          startResp.RunId,
				IncludeInput:   true,
				IncludeOutcome: true,
			})
			require.NoError(t, err)
			require.NotNil(t, describeResp)
			require.Equal(t, startResp.RunId, describeResp.GetRunId())
			protorequire.ProtoEqual(t, defaultInput, describeResp.GetInput())
			// info fields
			info := describeResp.GetInfo()
			require.NotNil(t, info)
			require.Equal(t, activityID, info.GetActivityId())
			require.Equal(t, startResp.RunId, info.GetRunId())
			protorequire.ProtoEqual(t, s.tv.ActivityType(), info.GetActivityType())
			require.Equal(t, tc.expectedStatus, info.GetStatus())
			require.Equal(t, taskQueue.Name, info.GetTaskQueue())
			require.Equal(t, int32(1), info.GetAttempt())
			protorequire.ProtoEqual(t, defaultRetryPolicy, info.GetRetryPolicy())
			protorequire.ProtoEqual(t, defaultHeader, info.GetHeader())
			protorequire.ProtoEqual(t, &commonpb.Priority{FairnessKey: "test-key"}, info.GetPriority())
			protorequire.ProtoEqual(t, defaultSearchAttributes, info.GetSearchAttributes())
			protorequire.ProtoEqual(t, defaultUserMetadata, info.GetUserMetadata())
			require.Equal(t, 45*time.Second, info.GetHeartbeatTimeout().AsDuration())
			require.Equal(t, defaultStartToCloseTimeout, info.GetStartToCloseTimeout().AsDuration())
			require.Equal(t, defaultIdentity, info.GetLastWorkerIdentity())
			// time fields
			require.NotNil(t, info.GetScheduleTime())
			require.Positive(t, info.GetScheduleTime().AsTime().Unix())
			require.NotNil(t, info.GetLastStartedTime())
			require.Positive(t, info.GetLastStartedTime().AsTime().Unix())
			require.NotNil(t, info.GetCloseTime())
			require.Positive(t, info.GetCloseTime().AsTime().Unix())
			require.GreaterOrEqual(t, info.GetCloseTime().AsTime().UnixNano(), info.GetLastStartedTime().AsTime().UnixNano())
			require.Positive(t, info.GetStateTransitionCount())

			tc.outcomeValidator(t, describeResp)
		})
	}
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution() {
	testCases := []struct {
		name                   string
		taskCompletionFn       func(ctx context.Context, taskToken []byte, activityID, runID string) error
		completionValidationFn func(*testing.T, *workflowservice.PollActivityExecutionResponse)
	}{
		{
			name: "successful completion",
			taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
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
			name: "failure completion",
			taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
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
		{
			name: "cancellation",
			taskCompletionFn: func(ctx context.Context, taskToken []byte, activityID, runID string) error {
				_, err := s.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
				})
				if err != nil {
					return err
				}
				_, err = s.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
					Namespace: s.Namespace().String(),
					TaskToken: taskToken,
				})
				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				require.NotNil(t, response.GetOutcome().GetFailure())
				require.NotNil(t, response.GetOutcome().GetFailure().GetCanceledFailureInfo())
			},
		},
		{
			name: "termination",
			taskCompletionFn: func(ctx context.Context, _ []byte, activityID, runID string) error {
				_, err := s.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Reason:     "test termination",
				})
				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				require.NotNil(t, response.GetOutcome().GetFailure())
				require.NotNil(t, response.GetOutcome().GetFailure().GetTerminatedFailureInfo())
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
			err = tc.taskCompletionFn(ctx, pollTaskResp.TaskToken, activityID, startResp.RunId)
			require.NoError(t, err)
			pollActivityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.NotNil(t, pollActivityResp)
			require.Equal(t, startResp.RunId, pollActivityResp.GetRunId())
			tc.completionValidationFn(t, pollActivityResp)
		})
	}
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_EmptyRunID() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	activityID := s.tv.Any().String()
	taskQueue := s.tv.TaskQueue().String()

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)

	pollTaskResp, err := s.pollActivityTaskQueue(ctx, taskQueue)
	require.NoError(t, err)

	_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Result:    defaultResult,
	})
	require.NoError(t, err)

	pollActivityResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      "", // resolves to current run ID
	})
	require.NoError(t, err)
	require.NotNil(t, pollActivityResp)
	require.Equal(t, startResp.RunId, pollActivityResp.GetRunId())
	protorequire.ProtoEqual(t, defaultResult, pollActivityResp.GetOutcome().GetResult())
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
			expectedErrMsg: "activity not found for ID: non-existent-activity",
		},
		{
			name: "NonExistentRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "11111111-2222-3333-4444-555555555555",
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: fmt.Sprintf("activity not found for ID: %s", existingActivityID),
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

func (s *standaloneActivityTestSuite) TestListActivityExecutions() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	activityType := s.tv.ActivityType().GetName()
	taskQueue := s.tv.TaskQueue().GetName()
	startResp := s.startAndValidateActivity(ctx, t, activityID, taskQueue)
	runID := startResp.RunId

	verifyListQuery := func(t *testing.T, query string, pageSize int32) {
		t.Helper()
		var resp *workflowservice.ListActivityExecutionsResponse
		s.Eventually(
			func() bool {
				var err error
				resp, err = s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
					Namespace: s.Namespace().String(),
					PageSize:  pageSize,
					Query:     query,
				})
				return err == nil && len(resp.GetExecutions()) >= 1
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		require.Len(t, resp.GetExecutions(), 1, "expected exactly 1 result for query: %s", query)
		exec := resp.GetExecutions()[0]
		// Verify all ActivityExecutionListInfo fields
		s.Equal(activityID, exec.GetActivityId())
		s.Equal(runID, exec.GetRunId())
		s.Equal(activityType, exec.GetActivityType().GetName())
		s.Equal(taskQueue, exec.GetTaskQueue())
		s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, exec.GetStatus())
		s.NotNil(exec.GetScheduleTime())
		s.Nil(exec.GetCloseTime())         // Running activity has no close time
		s.Nil(exec.GetExecutionDuration()) // Running activity has no execution duration
		s.GreaterOrEqual(exec.GetStateSizeBytes(), int64(0))
		s.GreaterOrEqual(exec.GetStateTransitionCount(), int64(0))
	}

	t.Run("QueryByActivityId", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityId = '%s'", activityID), 10)
	})

	t.Run("QueryByActivityType", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityType = '%s'", activityType), 10)
	})

	t.Run("QueryByExecutionStatus", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ExecutionStatus = 'Running' AND ActivityType = '%s'", activityType), 10)
	})

	t.Run("QueryByTaskQueue", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("TaskQueue = '%s' AND ActivityType = '%s'", taskQueue, activityType), 10)
	})

	t.Run("QueryByMultipleFields", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityId = '%s' AND ActivityType = '%s'", activityID, activityType), 10)
	})

	t.Run("QueryByCustomSearchAttribute", func(t *testing.T) {
		customSAName := "CustomKeywordField"
		customSAValue := "custom-sa-test-value"
		customSAActivityID := "custom-sa-activity-id"

		_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           s.Namespace().String(),
			ActivityId:          customSAActivityID,
			ActivityType:        &commonpb.ActivityType{Name: "custom-sa-activity-type"},
			Identity:            s.tv.WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: s.tv.TaskQueue().GetName()},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			RequestId:           s.tv.RequestID(),
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					customSAName: payload.EncodeString(customSAValue),
				},
			},
		})
		require.NoError(t, err)

		var resp *workflowservice.ListActivityExecutionsResponse
		s.Eventually(
			func() bool {
				var err error
				resp, err = s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
					Namespace: s.Namespace().String(),
					PageSize:  10,
					Query:     fmt.Sprintf("%s = '%s'", customSAName, customSAValue),
				})
				return err == nil && len(resp.GetExecutions()) >= 1
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		require.Len(t, resp.GetExecutions(), 1)
		exec := resp.GetExecutions()[0]
		s.Equal(customSAActivityID, exec.GetActivityId())
		s.NotNil(exec.GetSearchAttributes())
		returnedSA := exec.GetSearchAttributes().GetIndexedFields()[customSAName]
		s.NotNil(returnedSA)
		var returnedValue string
		s.NoError(payload.Decode(returnedSA, &returnedValue))
		s.Equal(customSAValue, returnedValue)
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		_, err := s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		_, err := s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		_, err := s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: "non-existent-namespace",
			PageSize:  10,
			Query:     "",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
	})

	t.Run("ZeroPageSizeDefaultsToConfigMax", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityId = '%s'", activityID), 0)
	})

	t.Run("ExceededPageSizeIsCapped", func(t *testing.T) {
		maxPageSize := int32(1)
		cleanup := s.OverrideDynamicConfig(
			dynamicconfig.FrontendVisibilityMaxPageSize,
			maxPageSize,
		)
		defer cleanup()

		testActivityType := testcore.RandomizeStr(t.Name())

		// Start multiple activities of the same type
		for i := 0; i < 2; i++ {
			_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:           s.Namespace().String(),
				ActivityId:          testcore.RandomizeStr(t.Name()),
				ActivityType:        &commonpb.ActivityType{Name: testActivityType},
				Identity:            s.tv.WorkerIdentity(),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
				},
				RequestId: s.tv.RequestID(),
			})
			require.NoError(t, err)
		}

		// Wait for both activities to be indexed in Elasticsearch before testing pagination
		s.Eventually(
			func() bool {
				countResp, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("ActivityType = '%s'", testActivityType),
				})
				return err == nil && countResp.GetCount() == 2
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)

		// Get first page. Use pageSize > FrontendVisibilityMaxPageSize to test it is capped by the server
		var resp *workflowservice.ListActivityExecutionsResponse
		var err error
		resp, err = s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  maxPageSize + 1,
			Query:     fmt.Sprintf("ActivityType = '%s'", testActivityType),
		})
		require.NoError(t, err)
		require.Len(t, resp.GetExecutions(), 1)

		// Get next page. Use pageSize > FrontendVisibilityMaxPageSize to test it is capped by the server
		resp, err = s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace:     s.Namespace().String(),
			PageSize:      maxPageSize + 1,
			Query:         fmt.Sprintf("ActivityType = '%s'", testActivityType),
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, resp.GetExecutions(), 1)

		// Ensure no more results
		resp, err = s.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace:     s.Namespace().String(),
			PageSize:      maxPageSize + 1,
			Query:         fmt.Sprintf("ActivityType = '%s'", testActivityType),
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetExecutions())
		require.Nil(t, resp.GetNextPageToken())
	})
}

func (s *standaloneActivityTestSuite) TestCountActivityExecutions() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	activityID := s.tv.ActivityID()
	activityType := s.tv.ActivityType().GetName()
	s.startAndValidateActivity(ctx, t, activityID, s.tv.TaskQueue().GetName())

	verifyCountQuery := func(t *testing.T, query string, expectedCount int) {
		t.Helper()
		s.Eventually(
			func() bool {
				resp, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     query,
				})
				return err == nil && resp.GetCount() == int64(expectedCount)
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	t.Run("CountByActivityId", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("ActivityId = '%s'", activityID), 1)
	})

	t.Run("CountByActivityType", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("ActivityType = '%s'", activityType), 1)
	})

	t.Run("CountByExecutionStatus", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("ExecutionStatus = 'Running' AND ActivityType = '%s'", activityType), 1)
	})

	t.Run("CountByTaskQueue", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("TaskQueue = '%s' AND ActivityType = '%s'", s.tv.TaskQueue().GetName(), activityType), 1)
	})

	t.Run("GroupByExecutionStatus", func(t *testing.T) {
		groupByType := &commonpb.ActivityType{Name: "count-groupby-test-type"}
		taskQueue := s.tv.TaskQueue().GetName()

		for i := range 3 {
			id := fmt.Sprintf("%s-%d", groupByType.Name, i)
			resp, err := s.startActivityWithType(ctx, id, taskQueue, groupByType)
			require.NoError(t, err)
			require.NotEmpty(t, resp.GetRunId())
		}

		query := fmt.Sprintf("ActivityType = '%s' GROUP BY ExecutionStatus", groupByType.Name)
		var resp *workflowservice.CountActivityExecutionsResponse
		s.Eventually(
			func() bool {
				var err error
				resp, err = s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     query,
				})
				return err == nil && resp.GetCount() == 3
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)

		require.Len(t, resp.GetGroups(), 1)
		s.Equal(int64(3), resp.GetGroups()[0].GetCount())
		var groupValue string
		require.NoError(t, payload.Decode(resp.GetGroups()[0].GetGroupValues()[0], &groupValue))
		s.Equal("Running", groupValue)
	})

	t.Run("CountByCustomSearchAttribute", func(t *testing.T) {
		customSAName := "ActivityCountCustomKeyword"
		customSAValue := "count-custom-sa-value"

		_, err := s.OperatorClient().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
			Namespace: s.Namespace().String(),
			SearchAttributes: map[string]enumspb.IndexedValueType{
				customSAName: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
		})
		require.NoError(t, err)

		s.Eventually(func() bool {
			descResp, err := s.OperatorClient().ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{
				Namespace: s.Namespace().String(),
			})
			if err != nil {
				return false
			}
			_, ok := descResp.CustomAttributes[customSAName]
			return ok
		}, 10*time.Second, 100*time.Millisecond)

		for i := range 2 {
			_, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:           s.Namespace().String(),
				ActivityId:          fmt.Sprintf("count-custom-sa-%d", i),
				ActivityType:        &commonpb.ActivityType{Name: "count-custom-sa-type"},
				Identity:            s.tv.WorkerIdentity(),
				Input:               defaultInput,
				TaskQueue:           &taskqueuepb.TaskQueue{Name: s.tv.TaskQueue().GetName()},
				StartToCloseTimeout: durationpb.New(1 * time.Minute),
				RequestId:           s.tv.RequestID(),
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						customSAName: payload.EncodeString(customSAValue),
					},
				},
			})
			require.NoError(t, err)
		}

		s.Eventually(
			func() bool {
				resp, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("%s = '%s'", customSAName, customSAValue),
				})
				return err == nil && resp.GetCount() == 2
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	})

	t.Run("GroupByUnsupportedField", func(t *testing.T) {
		_, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "GROUP BY ActivityType",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.Contains(err.Error(), "'GROUP BY' clause is only supported for ExecutionStatus")
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		_, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		_, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		_, err := s.FrontendClient().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
			Namespace: "non-existent-namespace",
			Query:     "",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
	})
}

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
		cleanup1 := s.OverrideDynamicConfig(activity.LongPollBuffer, 1*time.Second)
		defer cleanup1()
		ctx, cancel := context.WithTimeout(ctx, 9999*time.Millisecond)
		defer cancel()

		// DescribeActivityExecution will return when this long poll timeout expires.
		cleanup2 := s.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)
		defer cleanup2()

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
		cleanup1 := s.OverrideDynamicConfig(activity.LongPollBuffer, 29*time.Second)
		defer cleanup1()
		// DescribeActivityExecution will return when this long poll timeout expires.
		cleanup2 := s.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)
		defer cleanup2()

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
			expectedErrMsg: "activity not found for ID: non-existent-activity",
		},
		{
			name: "NonExistentRunID",
			request: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "11111111-2222-3333-4444-555555555555",
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: fmt.Sprintf("activity not found for ID: %s", existingActivityID),
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
		require.Equal(t, "activity not found for ID: non-existent-activity", notFoundErr.Message)
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

	t.Run("LongPollTokenFromDifferentNamespace", func(t *testing.T) {
		// Get a valid poll token from activity in main namespace
		validPollResp, err := s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  existingNamespace,
			ActivityId: existingActivityID,
			RunId:      existingRunID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, validPollResp.LongPollToken)

		// Start an activity in a different namespace
		externalNamespace := s.ExternalNamespace().String()
		externalActivityID := s.tv.Any().String()
		externalStartResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   externalActivityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tq.Name,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)
		require.NotEmpty(t, externalStartResp.GetRunId())

		// Try to use main namespace's poll token with external namespace's activity
		_, err = s.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     externalNamespace,
			ActivityId:    externalActivityID,
			RunId:         externalStartResp.GetRunId(),
			LongPollToken: validPollResp.LongPollToken,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "long poll token does not match execution", invalidArgErr.Message)
	})
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

	t.Run("MismatchedNamespaceToken", func(t *testing.T) {
		// Start an activity and get a valid task token, then complete it
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to heartbeat with existing token
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := s.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := s.ExternalNamespace().String()
		_, err = s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: s.tv.ActivityType(),
			Identity:     s.tv.WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           s.tv.Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = s.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
			Details:   heartbeatDetails,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
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

	t.Run("RecordHeartbeatByIDStaysAlive", func(t *testing.T) {
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
		_, err = s.FrontendClient().RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Details:    heartbeatDetails,
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

func (s *standaloneActivityTestSuite) pollActivityTaskQueue(ctx context.Context, taskQueue string) (*workflowservice.PollActivityTaskQueueResponse, error) {
	return s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: defaultIdentity,
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
	require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, info.GetCloseTime().AsTime().IsZero())
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
	require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, info.GetCloseTime().AsTime().IsZero())
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
	return s.startActivityWithType(ctx, activityID, taskQueue, s.tv.ActivityType())
}

func (s *standaloneActivityTestSuite) startActivityWithType(ctx context.Context, activityID string, taskQueue string, activityType *commonpb.ActivityType) (*workflowservice.StartActivityExecutionResponse, error) {
	return s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: activityType,
		Identity:     s.tv.WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		RequestId:           s.tv.RequestID(),
	})
}
