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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
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

// TestStartToCloseTimeout tests that a start-to-close timeout is recorded after the activity is
// started. It also verifies that PollActivityExecution can be used to poll for a TimedOut state
// change caused by execution of a timer task.
func (s *standaloneActivityTestSuite) TestStartToCloseTimeout() {
	t := s.T()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := uuid.New().String()

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
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, pollResp.GetInfo().GetStatus())

	// require.NotNil(t, pollResp.GetFailure())
	// require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, pollResp.GetFailure().GetTimeoutFailureInfo().GetTimeoutType())

	// require.NotNil(t, pollResp, "Response should not be nil")
	// require.Nil(t, pollResp.GetInfo(), "Info should be nil when no state change notification is received")
	// require.Nil(t, pollResp.GetOutcome(), "Outcome should be nil when no state change notification is received")

	// // Once the bug is fixed, the long-poll should return immediately with:
	// // - pollResp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	// // - pollResp.GetFailure() with TimeoutFailureInfo.TimeoutType == TIMEOUT_TYPE_START_TO_CLOSE
	// if pollResp.GetOutcome() != nil {
	// 	if failure := pollResp.GetFailure(); failure != nil {
	// 		t.Logf("Activity failed with: %v", failure.GetMessage())
	// 		// Check if it's a timeout failure
	// 		if timeoutFailure := failure.GetTimeoutFailureInfo(); timeoutFailure != nil {
	// 			t.Logf("Timeout type: %v", timeoutFailure.GetTimeoutType())
	// 			require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutFailure.GetTimeoutType(),
	// 				"Expected start-to-close timeout (activity_attempt_timeout)")
	// 		} else {
	// 			t.Fatal("Expected timeout failure but got different failure type")
	// 		}
	// 	} else if result := pollResp.GetResult(); result != nil {
	// 		t.Fatal("Activity succeeded when it should have timed out")
	// 	}
	// }
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
	require.Error(t, err)
	require.ErrorContains(t, err, "cached mutable state could potentially be stale")
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_WaitCompletion() {
	t := s.T()
	t.Skip("TODO(dan): implement test when RecordActivityTaskCompleted is implemented")
}

func (s *standaloneActivityTestSuite) Test_PollActivityExecution_WaitAnyStateChange_Success_UpdateComponent() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = chasm.NewEngineContext(ctx, s.chasmEngine)

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := uuid.New().String()

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)
	entityKey := chasm.EntityKey{
		NamespaceID: s.NamespaceID().String(),
		BusinessID:  activityID,
		EntityID:    startResp.RunId,
	}

	// Test PollActivityExecution(WaitAnyStateChange) without a long-poll token.
	pollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, pollResp.GetStateChangeLongPollToken())
	require.Equal(t, startResp.GetRunId(), pollResp.GetRunId())

	_, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*activity.Activity](entityKey),
		func(a *activity.Activity, ctx chasm.MutableContext, _ any) (any, error) {
			// Don't actually mutate; just trigger the notification
			return nil, nil
		}, nil)
	require.NoError(t, err)

	// Test PollActivityExecution(WaitAnyStateChange) with a long-poll token.
	secondPollResp, err := s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		WaitPolicy: &workflowservice.PollActivityExecutionRequest_WaitAnyStateChange{
			WaitAnyStateChange: &workflowservice.PollActivityExecutionRequest_StateChangeWaitOptions{
				LongPollToken: pollResp.GetStateChangeLongPollToken(),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, secondPollResp.StateChangeLongPollToken)
}

// Test_PollVisibility_UpdateFromParent tests that polling for visibility component is woken up when
// the parent activity is updated and modifies the visibility.
// TODO(dan): this test uses chasm APIs (ReadComponent, UpdateComponent, PollComponent) directly. Is
// this illegitimate in tests/?
func (s *standaloneActivityTestSuite) Test_PollVisibility_UpdateFromParent() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = chasm.NewEngineContext(ctx, s.chasmEngine)

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := uuid.New().String()

	startResp, err := s.startActivity(ctx, activityID, taskQueue)
	require.NoError(t, err)
	entityKey := chasm.EntityKey{
		NamespaceID: s.NamespaceID().String(),
		BusinessID:  activityID,
		EntityID:    startResp.RunId,
	}

	visibilityRef, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*activity.Activity](entityKey),
		func(a *activity.Activity, ctx chasm.Context, _ any) ([]byte, error) {
			visibility, err := a.Visibility.Get(ctx)
			if err != nil {
				return nil, err
			}
			return ctx.Ref(visibility)
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, visibilityRef)

	pollStarted := make(chan struct{})
	pollCompleted := make(chan []byte, 1)
	pollError := make(chan error, 1)

	go func() {
		close(pollStarted)

		_, ref, err := chasm.PollComponent(
			ctx,
			visibilityRef,
			func(v *chasm.Visibility, ctx chasm.Context, _ any) (any, bool, error) {
				sa, err := v.SA.Get(ctx)
				if err != nil {
					return nil, false, err
				}
				if sa != nil && len(sa.IndexedFields) > 0 {
					// State has changed, stop waiting
					return nil, true, nil
				}
				// State hasn't changed yet, keep waiting
				return nil, false, nil
			},
			nil,
		)
		if err != nil {
			pollError <- err
		} else {
			pollCompleted <- ref
		}
	}()

	<-pollStarted
	// Hope that subscription has been established after an arbitrary amount of time
	// TODO(dan)
	time.Sleep(100 * time.Millisecond)

	// Modify the visibility component via an update targeting its parent activity component
	_, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*activity.Activity](entityKey),
		func(a *activity.Activity, ctx chasm.MutableContext, _ any) (any, error) {
			visibility, err := a.Visibility.Get(ctx)
			if err != nil {
				return nil, err
			}
			visibility.SA = chasm.NewDataField(ctx, &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"TestField": payload.EncodeString("updated from parent"),
				},
			})
			return nil, nil
		}, nil)
	require.NoError(t, err)

	select {
	case ref := <-pollCompleted:
		require.NotNil(t, ref)
	case err := <-pollError:
		t.Fatalf("Poll failed with error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not complete within timeout")
	}
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
