package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	chasmactivitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type standaloneActivityTestSuite struct {
	testcore.FunctionalTestBase
	chasmEngine chasm.Engine
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
	taskQueue := uuid.New().String()

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

func (s *standaloneActivityTestSuite) TestScheduleToStartShouldTimeout() {
	// TODO implement when we have PollActivityExecution. Make sure we check the attempt vs. outcome failure population.
	s.T().Skip("Temporarily disabled")
}

func (s *standaloneActivityTestSuite) TestScheduleToCloseShouldTimeout() {
	// TODO implement when we have PollActivityExecution. Make sure we check the attempt vs. outcome failure population.
	s.T().Skip("Temporarily disabled")
}

func (s *standaloneActivityTestSuite) TestStartToCloseShouldTimeout() {
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
	// Long poll for any state change. PollActivityTaskQueue is used to cause a state change.
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := uuid.New().String()

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)

	pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		RunId:        startResp.RunId,
		IncludeInfo:  true,
		IncludeInput: true,
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
	// TODO(dan): test IncludeOutcome
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_WaitAnyStateChange() {
	// Long poll for any state change. PollActivityTaskQueue is used to cause a state change.
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := uuid.New().String()

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)

	// First poll lacks token and therefore responds immediately, returning a token
	firstPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{},
		},
		IncludeInfo:  true,
		IncludeInput: true,
	})
	require.NoError(t, err)
	require.NotNil(t, firstPollResp.StateChangeLongPollToken)
	require.NotNil(t, firstPollResp.Info)
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
	var tokenProto chasmactivitypb.ComponentStateToken
	err = tokenProto.Unmarshal(token)
	require.NoError(t, err)
	if tokenProto.VersionedTransition != nil {
		tokenProto.VersionedTransition.NamespaceFailoverVersion += 1
	}
	token, err = tokenProto.Marshal()
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
	require.Error(t, err)
	require.ErrorContains(t, err, "cached mutable state could potentially be stale")
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_WaitCompletion() {
	t := s.T()
	t.Skip("TODO(dan): implement test when RecordActivityTaskCompleted is implemented")
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
