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
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
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

	defaultResult = payloads.EncodeString("Done")

	defaultFailure = &failurepb.Failure{
		Message: "Failed Activity",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "Test",
			NonRetryable: true,
		}},
	}

	defaultHeartbeatDetails = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte("test-heartbeat-detail"),
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
		enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
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

	// Worker picks up activity task, triggering transition (via HandleStarted)
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
			enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
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
				protorequire.ProtoEqual(t, &failurepb.Failure{}, response.GetFailure())
			},
		},
	}

	for _, tc := range testCases {
		t := s.T()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		t.Cleanup(cancel)

		activityID := s.tv.ActivityID()
		taskQueue := s.tv.TaskQueue().String()

		startResp, err := s.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		activityPollDone := make(chan struct{})
		var activityPollResp *workflowservice.PollActivityExecutionResponse
		var activityPollErr error
		var taskError error

		go func() {
			defer close(activityPollDone)
			activityPollResp, activityPollErr = s.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
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
		}()

		// Worker picks up activity task and completes it
		go func() {
			pollTaskResp, err := s.pollActivityTaskQueue(ctx, taskQueue)
			if err != nil {
				taskError = err
				return
			}

			taskError = tc.taskCompletionFn(ctx, pollTaskResp.TaskToken)
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
				tc.expectedStatus,
				enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED,
			)

			protorequire.ProtoEqual(t, defaultInput, activityPollResp.GetInput())
			tc.completionValidationFn(t, activityPollResp)
		case <-ctx.Done():
			t.Fatal("PollActivityExecution timed out")
		}

		require.NoError(t, taskError)
	}
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
	protorequire.ProtoEqual(t, &failurepb.Failure{}, activityResp.GetFailure())

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
