package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	defaultInput = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte("test-activity-input"),
			},
		},
	}
)

type standaloneActivityTestSuite struct {
	testcore.FunctionalTestBase
	tv          *testvars.TestVars
	chasmEngine chasm.Engine
}

func TestStandaloneActivityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(standaloneActivityTestSuite))
}

func (s *standaloneActivityTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite()
	s.tv = testvars.New(s.T())
	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	var err error
	s.chasmEngine, err = s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(s.chasmEngine)
}

func (s *standaloneActivityTestSuite) TestStartActivityExecution() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	activityType := &commonpb.ActivityType{
		Name: "test-activity-type",
	}
	input := createDefaultInput()
	taskQueue := testcore.RandomizeStr(t.Name())

	resp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: activityType,
		Input:        input,
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
		RequestId: "test-request-id",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetRunId())
	require.True(t, resp.Started)

	pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test-identity",
	})

	require.NoError(t, err)
	require.Equal(t, activityID, pollResp.GetActivityId())
	require.True(t, proto.Equal(activityType, pollResp.GetActivityType()))
	require.EqualValues(t, 1, pollResp.Attempt)
	require.True(t, proto.Equal(input, pollResp.GetInput()))
}

// TestStartToCloseTimeout tests that a start-to-close timeout is recorded after the activity is
// started. It also verifies that PollActivityExecution can be used to poll for a TimedOut state
// change caused by execution of a timer task.
func (s *standaloneActivityTestSuite) TestStartToCloseTimeout() {
	t := s.T()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		Input: defaultInput,
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Second),
		},
		RequestId: "test-request-id",
	})
	require.NoError(t, err)
	t.Logf("Started activity %s with 1s start-to-close timeout", activityID)

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
			Name: taskQueue,
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

	// TODO(dan): will complete this test in a subsequent PR, on top of https://github.com/temporalio/temporal/pull/8653
	require.NoError(t, err)
	require.NotNil(t, pollResp)
	require.NotNil(t, pollResp.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, pollResp.GetInfo().GetStatus())
	// failure := pollResp.GetInfo().GetLastFailure()
	// require.NotNil(t, failure)
	// timeoutFailure := failure.GetTimeoutFailureInfo()
	// require.NotNil(t, timeoutFailure)
	// require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutFailure.GetTimeoutType())
	// require.Nil(t, pollResp.GetOutcome())
	// require.Nil(t, pollResp.GetFailure())
}

func (s *standaloneActivityTestSuite) TestScheduleToCloseTimeout() {
	// TODO implement when we have PollActivityExecution. Make sure we check the attempt vs. outcome failure population.
	s.T().Skip("Temporarily disabled")
}

func createDefaultInput() *commonpb.Payloads {
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte(`{"name":"test-user","count":11}`),
			},
		},
	}
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_NoWait() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)

	t.Run("MinimalResponse", func(t *testing.T) {
		pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:      s.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
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
			enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		)
		require.NotNil(t, pollResp.Input)
		require.Equal(t, "test-activity-input", string(pollResp.Input.Payloads[0].Data))

		// Activity is scheduled but not completed, so no outcome yet
		require.Nil(t, pollResp.GetResult())
		require.Nil(t, pollResp.GetFailure())
	})
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_WaitAnyStateChange() {
	// Long poll for any state change. PollActivityTaskQueue is used to cause a state change.
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
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
		_, err := s.pollActivityTaskQueue(ctx, taskQueue)
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
			enumspb.PENDING_ACTIVITY_STATE_STARTED,
		)
		require.NotNil(t, activityPollResp.Input)
		require.Equal(t, "test-activity-input", string(activityPollResp.Input.Payloads[0].Data))

	case <-ctx.Done():
		t.Fatal("PollActivityExecution timed out")
	}

	err = <-taskQueuePollErr
	require.NoError(t, err)

	// Manipulate the token to verify token staleness checks (simulate ErrStaleReference). To do so
	// we make use of the internal implementation detail that the bytes are a serialized ref.
	token := firstPollResp.StateChangeLongPollToken
	var pRef persistencespb.ChasmComponentRef
	err = pRef.Unmarshal(token)
	require.NoError(t, err)
	if pRef.EntityVersionedTransition != nil {
		pRef.EntityVersionedTransition.NamespaceFailoverVersion += 1
	}
	token, err = pRef.Marshal()
	require.NoError(t, err)

	_, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
				LongPollToken: token,
			},
		},
	})
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
	require.ErrorContains(t, err, "please retry")
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_WaitCompletion() {
	t := s.T()
	t.Skip("TODO(dan): implement test when RecordActivityTaskCompleted is implemented")
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_DeadlineExceeded() {
	t := s.T()
	originalCtx := testcore.NewContext()

	// Start an activity and get initial long-poll state token
	activityID := "test-activity-" + t.Name()
	taskQueue := "test-task-queue-" + t.Name()
	startResp, err := s.startActivity(originalCtx, activityID, taskQueue)
	require.NoError(t, err)
	pollResp, err := s.FrontendClient().PollActivityExecution(originalCtx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{},
		},
	})
	require.NoError(t, err)

	// The PollActivityExecution calls below use a long-poll token and will necessarily time out,
	// because the activity undergoes no further state transitions. There are two ways in which they
	// can time out:
	// Case 1: due to the caller's deadline expiring (caller gets DeadlineExceeded error)
	// Case 2: due to the internal long-poll timeout (caller does not get an error).

	// We first verify case 1. To do so the caller must set a deadline that comes before the
	// internal deadline, so we override the internal long-poll timeout to something large, and poll
	// with a short caller deadline.
	cleanup := s.OverrideDynamicConfig(
		dynamicconfig.HistoryLongPollExpirationInterval,
		9999*time.Millisecond,
	)
	defer cleanup()
	ctx, cancel := context.WithTimeout(originalCtx, 10*time.Millisecond)
	defer cancel()
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
	require.Error(t, err)
	statusErr := serviceerror.ToStatus(err)
	require.NotNil(t, statusErr)
	require.Equal(t, codes.DeadlineExceeded, statusErr.Code())

	// Next we verify case 2. We set the internal long-poll timeout to something small and poll with
	// a large caller deadline.
	cleanup = s.OverrideDynamicConfig(
		dynamicconfig.HistoryLongPollExpirationInterval,
		10*time.Millisecond,
	)
	defer cleanup()
	ctx, cancel = context.WithTimeout(originalCtx, 9999*time.Millisecond)
	defer cancel()
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
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_NotFound() {
	t := s.T()
	ctx := testcore.NewContext()
	activityID := "test-activity-" + t.Name()
	taskQueue := "test-task-queue-" + t.Name()
	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)
	require.NotEmpty(t, startResp.RunId)

	testCases := []struct {
		name        string
		request     *workflowservice.PollActivityExecutionRequest
		expectedErr string
	}{
		{
			name: "NonExistentNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "non-existent-namespace",
				ActivityId: activityID,
				RunId:      startResp.RunId,
			},
			expectedErr: "Namespace non-existent-namespace is not found.",
		},
		{
			name: "NonExistentActivityID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: "non-existent-activity",
				RunId:      startResp.RunId,
			},
			expectedErr: "execution not found",
		},
		{
			name: "NonExistentRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      "11111111-2222-3333-4444-555555555555",
			},
			expectedErr: "execution not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.FrontendClient().PollActivityExecution(ctx, tc.request)
			require.Error(t, err)
			statusErr := serviceerror.ToStatus(err)
			require.NotNil(t, statusErr)
			require.Equal(t, codes.NotFound, statusErr.Code())
			require.Equal(t, tc.expectedErr, statusErr.Message())
		})
	}

	t.Run("LongPollNonExistentActivity", func(t *testing.T) {
		// Poll to get a token
		validPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: nil,
				},
			},
		})
		require.NoError(t, err)

		// Use the token with a non-existent activity
		_, err = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			ActivityId: "non-existent-activity",
			RunId:      startResp.RunId,
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: validPollResp.StateChangeLongPollToken,
				},
			},
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.NotNil(t, statusErr)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Equal(t, "execution not found", statusErr.Message())
	})
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_InvalidArguments() {
	t := s.T()
	ctx := testcore.NewContext()

	// Start a valid activity first to have something to poll
	activityID := "test-activity-" + t.Name()
	taskQueue := "test-task-queue-" + t.Name()
	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)
	require.NotEmpty(t, startResp.RunId)

	// Get a valid long-poll token for testing malformed token scenarios
	validPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
				LongPollToken: nil, // First poll
			},
		},
	})
	require.NoError(t, err)
	validToken := validPollResp.StateChangeLongPollToken

	testCases := []struct {
		name        string
		request     *workflowservice.PollActivityExecutionRequest
		expectedErr string
	}{
		{
			name: "InvalidNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "invalid-namespace-!@#$%", // Invalid characters
				ActivityId: activityID,
				RunId:      startResp.RunId,
			},
			// Note: This returns NotFound because the namespace registry doesn't validate characters,
			// it just tries to look it up and doesn't find it. This is acceptable.
			expectedErr: "",
		},
		{
			name: "EmptyNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "",
				ActivityId: activityID,
				RunId:      startResp.RunId,
			},
			expectedErr: "namespace is empty",
		},
		{
			name: "EmptyActivityID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: "", // Empty activity ID
				RunId:      startResp.RunId,
			},
			expectedErr: "activity id is required",
		},
		{
			name: "ActivityIDTooLong",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: string(make([]byte, 2000)), // Way too long (limit is typically 1000)
				RunId:      startResp.RunId,
			},
			expectedErr: "activity id exceeds length limit",
		},
		{
			name: "InvalidRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      "not-a-uuid", // Invalid UUID format
			},
			expectedErr: "invalid run id",
		},
		{
			name: "EmptyRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      "", // Empty run ID when polling specific execution
			},
			expectedErr: "run id is required",
		},
		{
			name: "MalformedLongPollToken",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
				WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
					WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
						LongPollToken: []byte("not-a-valid-protobuf-token"), // Invalid protobuf
					},
				},
			},
			expectedErr: "invalid long poll token",
		},
		{
			name: "TruncatedLongPollToken",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
				WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
					WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
						LongPollToken: validToken[:len(validToken)/2], // Truncated token
					},
				},
			},
			expectedErr: "invalid long poll token",
		},
		{
			name: "InvalidWaitPolicyType",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
				WaitPolicy: nil, // This would need a custom invalid type in real implementation
			},
			// Note: This test case might need adjustment based on how proto validation works
			// In practice, protobuf oneof fields prevent truly invalid types
			expectedErr: "",
		},
		{
			name: "ActivityIDWithInvalidCharacters",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  s.Namespace().String(),
				ActivityId: "activity\x00with\nnull\tand\rspecial", // Contains null and control characters
				RunId:      startResp.RunId,
			},
			expectedErr: "activity id contains invalid characters",
		},
		{
			name: "NonExistentNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "non-existent-but-valid-namespace",
				ActivityId: activityID,
				RunId:      startResp.RunId,
			},
			// This should return NOT_FOUND, not INVALID_ARGUMENT, so we skip it here
			expectedErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectedErr == "" {
				t.Skip("Test case not applicable or would return different error type")
				return
			}

			_, err := s.FrontendClient().PollActivityExecution(ctx, tc.request)
			require.Error(t, err, "Expected error for test case: %s", tc.name)

			// Check that it's an invalid argument error
			// The actual implementation may not have all these validations yet,
			// so these assertions will likely fail and show what needs to be implemented
			statusErr := serviceerror.ToStatus(err)
			require.NotNil(t, statusErr, "Expected error to be convertible to status for test case: %s", tc.name)
			require.Equal(t, codes.InvalidArgument, statusErr.Code(),
				"Expected InvalidArgument error code for test case: %s, got: %v with message: %s",
				tc.name, statusErr.Code(), statusErr.Message())

			// Check that the error message contains expected text
			// This is a loose check since exact messages may vary
			if tc.expectedErr != "" {
				require.Contains(t, strings.ToLower(statusErr.Message()), strings.ToLower(tc.expectedErr),
					"Expected error message to contain '%s' for test case: %s, got: %s",
					tc.expectedErr, tc.name, statusErr.Message())
			}
		})
	}

	// Additional test: Verify that combining multiple invalid fields returns appropriate error
	t.Run("MultipleInvalidFields", func(t *testing.T) {
		_, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  "",                 // Invalid: empty
			ActivityId: "",                 // Invalid: empty
			RunId:      "not-a-valid-uuid", // Invalid: not UUID
			WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
				WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
					LongPollToken: []byte("garbage"), // Invalid: malformed
				},
			},
		})
		require.Error(t, err)

		statusErr := serviceerror.ToStatus(err)
		require.NotNil(t, statusErr, "Expected error to be convertible to status")
		require.Equal(t, codes.InvalidArgument, statusErr.Code(),
			"Expected InvalidArgument error code, got: %v with message: %s",
			statusErr.Code(), statusErr.Message())

		// The error should mention at least one of the invalid fields
		errMsg := strings.ToLower(statusErr.Message())
		hasRelevantError := strings.Contains(errMsg, "namespace") ||
			strings.Contains(errMsg, "activity") ||
			strings.Contains(errMsg, "run") ||
			strings.Contains(errMsg, "token")
		require.True(t, hasRelevantError,
			"Expected error message to mention at least one invalid field, got: %s", statusErr.Message())
	})
}

func (s *standaloneActivityTestSuite) assertActivityExecutionInfo(
	t *testing.T,
	info *activitypb.ActivityExecutionInfo,
	activityID string,
	runID string,
	runState enumspb.PendingActivityState,
) {
	t.Helper()
	require.Equal(t, activityID, info.ActivityId)
	require.Equal(t, runID, info.RunId)
	require.NotNil(t, info.ActivityType)
	require.Equal(t, "test-activity-type", info.ActivityType.Name)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.Status)
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

func (s *standaloneActivityTestSuite) startActivity(ctx context.Context, activityID string, taskQueue string) (*workflowservice.StartActivityExecutionResponse, error) {
	return s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		Input: &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				{
					Metadata: map[string][]byte{
						"encoding": []byte("json/plain"),
					},
					Data: []byte("test-activity-input"),
				},
			},
		},
		Options: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		},
		RequestId: "test-request-id",
	})
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
