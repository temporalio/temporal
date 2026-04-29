package tests

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusStandaloneTestSuite struct {
	parallelsuite.Suite[*NexusStandaloneTestSuite]
}

func TestNexusStandaloneTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusStandaloneTestSuite{})
}

func (s *NexusStandaloneTestSuite) newTestEnv(opts ...testcore.TestOption) *NexusTestEnv {
	return newNexusTestEnv(s.T(), true, append(
		opts,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
	)...)
}

func (s *NexusStandaloneTestSuite) TestStartStandaloneNexusOperation() {
	s.Run("StartAndDescribe", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		testInput := payload.EncodeString("test-input")
		testHeader := map[string]string{"test-key": "test-value"}
		testScheduleToStartTimeout := 2 * time.Minute
		testStartToCloseTimeout := 3 * time.Minute
		testUserMetadata := &sdkpb.UserMetadata{
			Summary: payload.EncodeString("test-summary"),
			Details: payload.EncodeString("test-details"),
		}
		testSearchAttributes := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("test-value"),
			},
		}
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:            "test-op",
			Endpoint:               endpointName,
			Input:                  testInput,
			NexusHeader:            testHeader,
			ScheduleToStartTimeout: durationpb.New(testScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(testStartToCloseTimeout),
			UserMetadata:           testUserMetadata,
			SearchAttributes:       testSearchAttributes,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		for _, tc := range []struct {
			name  string
			runID string
		}{
			{name: "WithRunID", runID: startResp.RunId},
			{name: "WithEmptyRunID", runID: ""},
		} {
			s.Run(tc.name, func(s *NexusStandaloneTestSuite) {
				descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
					Namespace:   env.Namespace().String(),
					OperationId: "test-op",
					RunId:       tc.runID,
				})
				s.NoError(err)
				s.Equal(startResp.RunId, descResp.RunId)

				info := descResp.GetInfo()
				protorequire.ProtoEqual(s.T(), &nexuspb.NexusOperationExecutionInfo{
					OperationId:            "test-op",
					RunId:                  startResp.RunId,
					Endpoint:               endpointName,
					Service:                "test-service",
					Operation:              "test-operation",
					Status:                 enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
					State:                  enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED,
					ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(testScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(testStartToCloseTimeout),
					NexusHeader:            testHeader,
					UserMetadata:           testUserMetadata,
					SearchAttributes:       testSearchAttributes,
					Attempt:                1,
					StateTransitionCount:   1,
					// Dynamic fields copied from actual response for comparison.
					RequestId:         info.GetRequestId(),
					ScheduleTime:      info.GetScheduleTime(),
					ExpirationTime:    info.GetExpirationTime(),
					ExecutionDuration: info.GetExecutionDuration(),
				}, info)
				s.NotEmpty(descResp.GetLongPollToken())
				s.NotEmpty(info.GetRequestId())
				s.NotNil(info.GetScheduleTime())
				s.NotNil(info.GetExpirationTime())
				s.NotNil(info.GetExecutionDuration())
			})
		}

		s.Run("IncludeInput", func(s *NexusStandaloneTestSuite) {
			descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:    env.Namespace().String(),
				OperationId:  "test-op",
				RunId:        startResp.RunId,
				IncludeInput: true,
			})
			s.NoError(err)
			protorequire.ProtoEqual(s.T(), testInput, descResp.GetInput())
		})
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})

	s.Run("IDConflictPolicyFail", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		resp1, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			RequestId:   "first-request-id",
		})
		s.NoError(err)

		// Second start with different request ID should fail.
		_, err = s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			RequestId:   "different-request-id",
		})
		s.Error(err)
		var alreadyStartedErr *serviceerror.NexusOperationExecutionAlreadyStarted
		s.ErrorAs(err, &alreadyStartedErr)
		s.ErrorContains(err, "nexus operation execution already started")
		s.Equal(resp1.RunId, alreadyStartedErr.RunId)
		s.Equal("first-request-id", alreadyStartedErr.StartRequestId)

		// Second start with same request ID should return existing run.
		resp2, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			RequestId:   "first-request-id",
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})

	s.Run("IDConflictPolicyUseExisting", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		resp1, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		resp2, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			Endpoint:         endpointName,
			RequestId:        "different-request-id",
			IdConflictPolicy: enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})
}

func (s *NexusStandaloneTestSuite) TestDescribeStandaloneNexusOperation() {
	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
		s.Equal("operation not found for ID: does-not-exist", notFound.Error())
	})

	s.Run("LongPollStateChange", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Obtain longpoll token.
		firstResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.NotEmpty(firstResp.GetLongPollToken())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, firstResp.GetInfo().GetStatus())

		ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
		defer cancel()

		// Start polling.
		type describeResult struct {
			resp *workflowservice.DescribeNexusOperationExecutionResponse
			err  error
		}
		describeResultCh := make(chan describeResult, 1)

		go func() {
			resp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:      env.Namespace().String(),
				OperationId:    "test-op",
				RunId:          startResp.RunId,
				IncludeOutcome: true,
				LongPollToken:  firstResp.GetLongPollToken(),
			})
			describeResultCh <- describeResult{resp: resp, err: err}
		}()

		// Wait 1s to ensure the poll is still active.
		select {
		case result := <-describeResultCh:
			s.NoError(result.err)
			s.T().Fatal("DescribeNexusOperationExecution returned before the state changed")
		case <-time.After(1 * time.Second):
		}

		// Terminate the operation.
		terminateErrCh := make(chan error, 1)
		go func() {
			_, err := env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				Reason:      "test termination",
			})
			terminateErrCh <- err
		}()

		select {
		case err := <-terminateErrCh:
			s.NoError(err)
		case <-ctx.Done():
			s.T().Fatal("TerminateNexusOperationExecution timed out")
		}

		// Verify the longpoll result.
		var longPollResp *workflowservice.DescribeNexusOperationExecutionResponse
		select {
		case result := <-describeResultCh:
			s.NoError(result.err)
			longPollResp = result.resp
		case <-ctx.Done():
			s.T().Fatal("DescribeNexusOperationExecution timed out")
		}

		s.Equal(startResp.RunId, longPollResp.GetRunId())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, longPollResp.GetInfo().GetStatus())
		s.Greater(longPollResp.GetInfo().GetStateTransitionCount(), firstResp.GetInfo().GetStateTransitionCount())
	})

	s.Run("LongPollTimeoutReturnsEmptyResponse", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		firstResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.NotEmpty(firstResp.GetLongPollToken())

		s.Run("CallerDeadlineNotExceeded", func(s *NexusStandaloneTestSuite) {
			env.OverrideDynamicConfig(nexusoperation.LongPollBuffer, time.Second)
			env.OverrideDynamicConfig(nexusoperation.LongPollTimeout, 10*time.Millisecond)

			ctx, cancel := context.WithTimeout(env.Context(), 5*time.Second)
			defer cancel()

			longPollResp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:     env.Namespace().String(),
				OperationId:   "test-op",
				RunId:         startResp.RunId,
				LongPollToken: firstResp.GetLongPollToken(),
			})
			s.NoError(err)
			protorequire.ProtoEqual(s.T(), &workflowservice.DescribeNexusOperationExecutionResponse{}, longPollResp)
		})

		s.Run("NoCallerDeadline", func(s *NexusStandaloneTestSuite) {
			// Frontend still imposes its own deadline upstream, so the buffer must fit within that.
			env.OverrideDynamicConfig(nexusoperation.LongPollBuffer, 29*time.Second)
			env.OverrideDynamicConfig(nexusoperation.LongPollTimeout, 10*time.Millisecond)

			longPollResp, err := env.FrontendClient().DescribeNexusOperationExecution(context.Background(), &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:     env.Namespace().String(),
				OperationId:   "test-op",
				RunId:         startResp.RunId,
				LongPollToken: firstResp.GetLongPollToken(),
			})
			s.NoError(err)
			protorequire.ProtoEqual(s.T(), &workflowservice.DescribeNexusOperationExecutionResponse{}, longPollResp)
		})
	})

	s.Run("IncludeOutcome_Success", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
		endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()
		expectedResult := payload.EncodeString("successful result")
		handlerLink := &commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
			WorkflowId: "handler-workflow",
			RunId:      "handler-run-id",
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   7,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		}

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
		defer cancel()

		pollerErrCh := make(chan error, 1)
		go func() {
			task, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-worker",
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			})
			if err != nil {
				pollerErrCh <- err
				return
			}
			expectedLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
			})
			startRequest := task.GetRequest().GetStartOperation()
			if len(startRequest.GetLinks()) != 1 {
				pollerErrCh <- fmt.Errorf("expected 1 link, got %d", len(startRequest.GetLinks()))
				return
			}
			if startRequest.GetLinks()[0].GetUrl() != expectedLink.URL.String() {
				pollerErrCh <- fmt.Errorf("unexpected link url: got %q want %q", startRequest.GetLinks()[0].GetUrl(), expectedLink.URL.String())
				return
			}
			if startRequest.GetLinks()[0].GetType() != expectedLink.Type {
				pollerErrCh <- fmt.Errorf("unexpected link type: got %q want %q", startRequest.GetLinks()[0].GetType(), expectedLink.Type)
				return
			}

			_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-worker",
				TaskToken: task.GetTaskToken(),
				Response: &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_SyncSuccess{
								SyncSuccess: &nexuspb.StartOperationResponse_Sync{
									Payload: expectedResult,
									Links: commonnexus.ConvertLinksToProto([]nexus.Link{
										commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink),
									}),
								},
							},
						},
					},
				},
			})
			pollerErrCh <- err
		}()

		s.EventuallyWithT(func(t *assert.CollectT) {
			descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:      env.Namespace().String(),
				OperationId:    "test-op",
				RunId:          startResp.RunId,
				IncludeOutcome: true,
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
			protorequire.ProtoEqual(t, expectedResult, descResp.GetResult())
			protorequire.ProtoSliceEqual(t, []*commonpb.Link{
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: handlerLink,
					},
				},
			}, descResp.GetInfo().GetLinks())

			pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, expectedResult, pollResp.GetResult())
		}, 10*time.Second, 100*time.Millisecond)

		s.NoError(<-pollerErrCh)
	})

	s.Run("IncludeOutcome_Failure", func(s *NexusStandaloneTestSuite) {
		// TODO: Add canceled last-attempt-failure coverage here once standalone cancellation tasks
		// can be completed through the public Nexus task APIs.

		s.Run("ScheduleToStartTimeout", func(s *NexusStandaloneTestSuite) {
			env := s.newTestEnv()
			taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
			endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()

			startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId:            "test-op",
				Endpoint:               endpointName,
				ScheduleToStartTimeout: durationpb.New(2 * time.Second),
			})
			s.NoError(err)

			s.EventuallyWithT(func(t *assert.CollectT) {
				descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
					Namespace:      env.Namespace().String(),
					OperationId:    "test-op",
					RunId:          startResp.RunId,
					IncludeOutcome: true,
				})
				require.NoError(t, err)
				require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT, descResp.GetInfo().GetStatus())
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation timed out",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
						},
					},
				}, descResp.GetFailure())

				pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
					Namespace:   env.Namespace().String(),
					OperationId: "test-op",
					RunId:       startResp.RunId,
					WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation timed out",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
						},
					},
				}, pollResp.GetFailure())
			}, 10*time.Second, 100*time.Millisecond)
		})

		s.Run("ScheduleToCloseTimeout_BeforeStart", func(s *NexusStandaloneTestSuite) {
			env := s.newTestEnv()
			taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
			endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()

			startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId:            "test-op",
				Endpoint:               endpointName,
				ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
			})
			s.NoError(err)

			s.EventuallyWithT(func(t *assert.CollectT) {
				descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
					Namespace:      env.Namespace().String(),
					OperationId:    "test-op",
					RunId:          startResp.RunId,
					IncludeOutcome: true,
				})
				require.NoError(t, err)
				require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT, descResp.GetInfo().GetStatus())
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation timed out",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
						},
					},
				}, descResp.GetFailure())

				pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
					Namespace:   env.Namespace().String(),
					OperationId: "test-op",
					RunId:       startResp.RunId,
					WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation timed out",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
						},
					},
				}, pollResp.GetFailure())
			}, 10*time.Second, 100*time.Millisecond)
		})

		testCases := []struct {
			name          string
			setupReq      func(*workflowservice.StartNexusOperationExecutionRequest)
			respond       func(context.Context, *NexusTestEnv, *workflowservice.PollNexusTaskQueueResponse) error
			assertOutcome func(*assert.CollectT, *workflowservice.DescribeNexusOperationExecutionResponse, *workflowservice.PollNexusOperationExecutionResponse)
		}{
			{
				name: "StartToCloseTimeout",
				setupReq: func(req *workflowservice.StartNexusOperationExecutionRequest) {
					req.StartToCloseTimeout = durationpb.New(2 * time.Second)
				},
				respond: func(ctx context.Context, env *NexusTestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
						Namespace: env.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Response: &nexuspb.Response{
							Variant: &nexuspb.Response_StartOperation{
								StartOperation: &nexuspb.StartOperationResponse{
									Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
										AsyncSuccess: &nexuspb.StartOperationResponse_Async{
											OperationToken: "test-operation-token",
										},
									},
								},
							},
						},
					})
					return err
				},
				assertOutcome: func(t *assert.CollectT, descResp *workflowservice.DescribeNexusOperationExecutionResponse, pollResp *workflowservice.PollNexusOperationExecutionResponse) {
					require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT, descResp.GetInfo().GetStatus())
					protorequire.ProtoEqual(t, &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
							},
						},
					}, descResp.GetFailure())
					protorequire.ProtoEqual(t, &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
							},
						},
					}, pollResp.GetFailure())
				},
			},
			{
				name: "ScheduleToCloseTimeout_AfterStart",
				setupReq: func(req *workflowservice.StartNexusOperationExecutionRequest) {
					req.ScheduleToCloseTimeout = durationpb.New(4 * time.Second)
				},
				respond: func(ctx context.Context, env *NexusTestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
						Namespace: env.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Response: &nexuspb.Response{
							Variant: &nexuspb.Response_StartOperation{
								StartOperation: &nexuspb.StartOperationResponse{
									Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
										AsyncSuccess: &nexuspb.StartOperationResponse_Async{
											OperationToken: "test-operation-token",
										},
									},
								},
							},
						},
					})
					return err
				},
				assertOutcome: func(t *assert.CollectT, descResp *workflowservice.DescribeNexusOperationExecutionResponse, pollResp *workflowservice.PollNexusOperationExecutionResponse) {
					require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT, descResp.GetInfo().GetStatus())
					protorequire.ProtoEqual(t, &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
							},
						},
					}, descResp.GetFailure())
					protorequire.ProtoEqual(t, &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
							},
						},
					}, pollResp.GetFailure())
				},
			},
			{
				name: "TimeoutLastAttemptFailure",
				setupReq: func(req *workflowservice.StartNexusOperationExecutionRequest) {
					req.ScheduleToCloseTimeout = durationpb.New(4 * time.Second)
				},
				respond: func(ctx context.Context, env *NexusTestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
						Namespace: env.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Error: &nexuspb.HandlerError{
							ErrorType: string(nexus.HandlerErrorTypeInternal),
							Failure: &nexuspb.Failure{
								Message: "last attempt failure",
							},
						},
					})
					return err
				},
				assertOutcome: func(t *assert.CollectT, descResp *workflowservice.DescribeNexusOperationExecutionResponse, pollResp *workflowservice.PollNexusOperationExecutionResponse) {
					require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT, descResp.GetInfo().GetStatus())
					protorequire.ProtoEqual(t, &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
							},
						},
					}, descResp.GetFailure())
					protorequire.ProtoEqual(t, &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
							},
						},
					}, pollResp.GetFailure())
				},
			},
			{
				name: "TerminalFailure",
				respond: func(ctx context.Context, env *NexusTestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
						Namespace: env.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Response: &nexuspb.Response{
							Variant: &nexuspb.Response_StartOperation{
								StartOperation: &nexuspb.StartOperationResponse{
									Variant: &nexuspb.StartOperationResponse_Failure{
										Failure: &failurepb.Failure{Message: "final failure"},
									},
								},
							},
						},
					})
					return err
				},
				assertOutcome: func(t *assert.CollectT, descResp *workflowservice.DescribeNexusOperationExecutionResponse, pollResp *workflowservice.PollNexusOperationExecutionResponse) {
					require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
					expected := &failurepb.Failure{Message: "final failure"}
					protorequire.ProtoEqual(t, expected, descResp.GetFailure())
					protorequire.ProtoEqual(t, expected, pollResp.GetFailure())
				},
			},
		}

		for _, tc := range testCases {
			s.Run(tc.name, func(s *NexusStandaloneTestSuite) {
				env := s.newTestEnv()
				taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
				endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()
				startReq := &workflowservice.StartNexusOperationExecutionRequest{
					OperationId: "test-op",
					Endpoint:    endpointName,
				}
				if tc.setupReq != nil {
					tc.setupReq(startReq)
				}

				startResp, err := s.startNexusOperation(env, startReq)
				s.NoError(err)

				ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
				defer cancel()

				pollerErrCh := make(chan error, 1)
				go func() {
					task, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
						Namespace: env.Namespace().String(),
						Identity:  "test-worker",
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
					})
					if err != nil {
						pollerErrCh <- err
						return
					}
					pollerErrCh <- tc.respond(ctx, env, task)
				}()

				s.EventuallyWithT(func(t *assert.CollectT) {
					descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
						Namespace:      env.Namespace().String(),
						OperationId:    "test-op",
						RunId:          startResp.RunId,
						IncludeOutcome: true,
					})
					require.NoError(t, err)

					pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
						Namespace:   env.Namespace().String(),
						OperationId: "test-op",
						RunId:       startResp.RunId,
						WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					})
					require.NoError(t, err)
					tc.assertOutcome(t, descResp, pollResp)
				}, 10*time.Second, 100*time.Millisecond)

				s.NoError(<-pollerErrCh)
			})
		}
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace: env.Namespace().String(),
		})
		s.Error(err)
		s.ErrorContains(err, "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationCancel() {
	s.Run("RequestCancel", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		// Verify state after cancel — operation is still running
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), &nexuspb.NexusOperationExecutionInfo{
			OperationId:            "test-op",
			RunId:                  startResp.RunId,
			Endpoint:               endpointName,
			Service:                "test-service",
			Operation:              "test-operation",
			Status:                 enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			State:                  enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED,
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			NexusHeader:            map[string]string{},
			SearchAttributes:       &commonpb.SearchAttributes{},
			Attempt:                1,
			StateTransitionCount:   descResp.GetInfo().GetStateTransitionCount(),
			// Dynamic fields copied from actual response for comparison.
			RequestId:         descResp.GetInfo().GetRequestId(),
			ScheduleTime:      descResp.GetInfo().GetScheduleTime(),
			ExpirationTime:    descResp.GetInfo().GetExpirationTime(),
			ExecutionDuration: descResp.GetInfo().GetExecutionDuration(),
		}, descResp.GetInfo())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, descResp.GetInfo().GetStatus())
	})

	s.Run("AlreadyCanceled", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Cancel the operation.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "cancel-request-id",
		})
		s.NoError(err)

		// Cancel again with same request ID — should be idempotent.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "cancel-request-id",
		})
		s.NoError(err)

		// Cancel with a different request ID — should error.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "different-request-id",
		})
		s.Error(err)
		s.Contains(err.Error(), "cancellation already requested")
	})

	s.Run("RequestCancel_ForwardsOriginalNexusHeaders", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
		taskHeaderCh := make(chan string, 1)

		listenAddr := nexustest.AllocListenAddress()
		nexustest.NewNexusServer(s.T(), listenAddr, nexustest.Handler{
			OnStartOperation: func(
				ctx context.Context,
				service string,
				operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-operation-token"}, nil
			},
			OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				taskHeaderCh <- options.Header.Get("x-test-header")
				return nil
			},
		})

		_, err := env.OperatorClient().CreateNexusEndpoint(env.Context(), &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_External_{
						External: &nexuspb.EndpointTarget_External{
							Url: "http://" + listenAddr,
						},
					},
				},
			},
		})
		s.NoError(err)

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			NexusHeader: map[string]string{"x-test-header": "expected-value"},
		})
		s.NoError(err)

		_, err = env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
		})
		s.NoError(err)

		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		select {
		case headerValue := <-taskHeaderCh:
			s.Equal("expected-value", headerValue)
		case <-time.After(10 * time.Second):
			s.T().Fatal("CancelOperation was not invoked")
		}
	})

	s.Run("AlreadyTerminated", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation first.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Identity:    "test-identity",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Cancel a terminated operation — should error.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.Error(err)
		s.Contains(err.Error(), "operation already completed")
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestTerminateStandaloneNexusOperation() {
	s.Run("Terminate", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Identity:    "test-identity",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Verify outcome after terminate.
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:      env.Namespace().String(),
			OperationId:    "test-op",
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		s.NoError(err)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
		failure := descResp.GetFailure()
		s.NotNil(failure)
		s.Equal("test termination", failure.GetMessage())
		s.NotNil(failure.GetTerminatedFailureInfo())
		s.Equal("test-identity", failure.GetTerminatedFailureInfo().GetIdentity())
	})

	s.Run("AlreadyTerminated", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Terminate again with same request ID — should be idempotent.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination again",
		})
		s.NoError(err)

		// Terminate with a different request ID — should error.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "different-request-id",
			Reason:      "test termination different",
		})
		s.Error(err)
		s.Contains(err.Error(), "already terminated")
	})

	s.Run("AlreadyCanceled", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Cancel the operation first.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		// Terminate a canceled operation — should succeed.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Verify state changed to terminated (terminate overrides cancel request).
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
			Reason:      "test termination",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestListStandaloneNexusOperation() {
	s.Run("ListAndVerifyFields", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "list-test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "OperationId = 'list-test-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		op := listResp.GetOperations()[0]
		protorequire.ProtoEqual(s.T(), &nexuspb.NexusOperationExecutionListInfo{
			OperationId:          "list-test-op",
			RunId:                startResp.RunId,
			Endpoint:             endpointName,
			Service:              "test-service",
			Operation:            "test-operation",
			Status:               enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			StateTransitionCount: op.GetStateTransitionCount(),
			SearchAttributes:     op.GetSearchAttributes(),
			// Dynamic fields copied from actual response for comparison.
			ScheduleTime: op.GetScheduleTime(),
		}, op)
		s.NotNil(op.GetScheduleTime())
	})

	s.Run("ListWithCustomSearchAttributes", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		testSA := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("list-sa-value"),
			},
		}
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "sa-op",
			Endpoint:         endpointName,
			SearchAttributes: testSA,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "CustomKeywordField = 'list-sa-value'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Equal("sa-op", listResp.GetOperations()[0].GetOperationId())
		returnedSA := listResp.GetOperations()[0].GetSearchAttributes().GetIndexedFields()["CustomKeywordField"]
		s.NotNil(returnedSA)
		var returnedValue string
		s.NoError(payload.Decode(returnedSA, &returnedValue))
		s.Equal("list-sa-value", returnedValue)
	})

	s.Run("QueryByMultipleFields", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "multi-op",
			Endpoint:    endpointName,
			Service:     "multi-service",
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' AND Service = 'multi-service'", endpointName),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Equal("multi-op", listResp.GetOperations()[0].GetOperationId())
	})

	s.Run("QueryBySupportedSearchAttributes", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()
		testStartTime := time.Now().UTC().Format(time.RFC3339Nano)

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "supported-closed-op",
			Endpoint:    endpointName,
			Service:     "supported-closed-service",
			Operation:   "supported-closed-operation",
		})
		s.NoError(err)

		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "supported-closed-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		requestID := descResp.GetInfo().GetRequestId()
		s.NotEmpty(requestID)

		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "supported-closed-op",
			RunId:       startResp.RunId,
			RequestId:   "supported-closed-op-terminate",
			Identity:    "test-identity",
			Reason:      "close for visibility query coverage",
		})
		s.NoError(err)

		for _, tc := range []struct {
			name   string
			query  string
			assert func(*testing.T, *nexuspb.NexusOperationExecutionListInfo)
		}{
			{
				name:  "OperationId",
				query: "OperationId = 'supported-closed-op'",
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
				},
			},
			{
				name:  "RunId",
				query: fmt.Sprintf("RunId = '%s'", startResp.RunId),
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, startResp.RunId, op.GetRunId())
				},
			},
			{
				name:  "RequestId",
				query: fmt.Sprintf("RequestId = '%s' AND OperationId = 'supported-closed-op'", requestID),
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
				},
			},
			{
				name:  "Endpoint",
				query: fmt.Sprintf("Endpoint = '%s' AND OperationId = 'supported-closed-op'", endpointName),
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, endpointName, op.GetEndpoint())
				},
			},
			{
				name:  "Service",
				query: "Service = 'supported-closed-service' AND OperationId = 'supported-closed-op'",
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-service", op.GetService())
				},
			},
			{
				name:  "Operation",
				query: "Operation = 'supported-closed-operation' AND OperationId = 'supported-closed-op'",
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-operation", op.GetOperation())
				},
			},
			{
				name:  "StartTime",
				query: fmt.Sprintf(`StartTime > "%s" AND OperationId = 'supported-closed-op'`, testStartTime),
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
				},
			},
			{
				name:  "ExecutionTime",
				query: fmt.Sprintf(`ExecutionTime > "%s" AND OperationId = 'supported-closed-op'`, testStartTime),
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
				},
			},
			{
				name:  "CloseTime",
				query: fmt.Sprintf(`CloseTime > "%s" AND OperationId = 'supported-closed-op'`, testStartTime),
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
				},
			},
			{
				name:  "ExecutionStatus",
				query: "ExecutionStatus = 'Terminated' AND OperationId = 'supported-closed-op'",
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, op.GetStatus())
				},
			},
			{
				name:  "ExecutionDuration",
				query: "ExecutionDuration > '0s' AND OperationId = 'supported-closed-op'",
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
					require.NotNil(t, op.GetExecutionDuration())
				},
			},
			{
				name:  "StateTransitionCount",
				query: "StateTransitionCount > 0 AND OperationId = 'supported-closed-op'",
				assert: func(t *testing.T, op *nexuspb.NexusOperationExecutionListInfo) {
					require.Equal(t, "supported-closed-op", op.GetOperationId())
					require.Positive(t, op.GetStateTransitionCount())
				},
			},
		} {
			s.Run(tc.name, func(s *NexusStandaloneTestSuite) {
				var resp *workflowservice.ListNexusOperationExecutionsResponse
				s.EventuallyWithT(func(t *assert.CollectT) {
					var err error
					resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
						Namespace: env.Namespace().String(),
						PageSize:  10,
						Query:     tc.query,
					})
					require.NoError(t, err, "case=%s query=%s", tc.name, tc.query)
					require.Len(t, resp.GetOperations(), 1, "case=%s query=%s", tc.name, tc.query)
				}, testcore.WaitForESToSettle, 100*time.Millisecond)
				tc.assert(s.T(), resp.GetOperations()[0])
			})
		}
	})

	s.Run("PageSizeCapping", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 2 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("paged-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		// Wait for both to be indexed.
		query := fmt.Sprintf("Endpoint = '%s'", endpointName)
		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     query,
			})
			require.NoError(t, err)
			require.Len(t, resp.GetOperations(), 2)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)

		// Override max page size to 1.
		env.OverrideDynamicConfig(dynamicconfig.FrontendVisibilityMaxPageSize, 1)

		// PageSize 0 should default to max (1), returning only 1 result.
		resp, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  0,
			Query:     query,
		})
		s.NoError(err)
		s.Len(resp.GetOperations(), 1)

		// PageSize > max should also be capped. First page.
		resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  2,
			Query:     query,
		})
		s.NoError(err)
		s.Len(resp.GetOperations(), 1)

		// Second page.
		resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     env.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		s.NoError(err)
		s.Len(resp.GetOperations(), 1)

		// No more results.
		resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     env.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		s.NoError(err)
		s.Empty(resp.GetOperations())
		s.Nil(resp.GetNextPageToken())
	})

	s.Run("InvalidQuery", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	s.Run("InvalidSearchAttribute", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	s.Run("NamespaceNotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func (s *NexusStandaloneTestSuite) TestCountStandaloneNexusOperation() {
	s.Run("CountByOperationID", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "OperationId = 'count-op'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("CountByEndpoint", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 3 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("count-ep-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("CountByExecutionStatus", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("ExecutionStatus = 'Running' AND Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("GroupByExecutionStatus", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 3 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("group-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		var countResp *workflowservice.CountNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			countResp, err = env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' GROUP BY ExecutionStatus", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), countResp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Len(countResp.GetGroups(), 1)
		s.Equal(int64(3), countResp.GetGroups()[0].GetCount())
		var groupValue string
		s.NoError(payload.Decode(countResp.GetGroups()[0].GetGroupValues()[0], &groupValue))
		s.Equal("Running", groupValue)
	})

	s.Run("CountByCustomSearchAttribute", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 2 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("count-sa-op-%d", i),
				Endpoint:    endpointName,
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"CustomKeywordField": payload.EncodeString("count-sa-value"),
					},
				},
			})
			s.NoError(err)
		}

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "CustomKeywordField = 'count-sa-value'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(2), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("GroupByUnsupportedField", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "GROUP BY Endpoint",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "'GROUP BY' clause is only supported for ExecutionStatus")
	})

	s.Run("InvalidQuery", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	s.Run("InvalidSearchAttribute", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	s.Run("NamespaceNotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func (s *NexusStandaloneTestSuite) TestDeleteStandaloneNexusOperation() {
	s.Run("Scheduled", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		s.eventuallyDeleted(env, s.T(), "test-op", startResp.RunId)
	})

	s.Run("NoRunID", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			// RunId not set
		})
		s.NoError(err)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
		})
		s.NoError(err)

		s.eventuallyDeleted(env, s.T(), "test-op", startResp.RunId)
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
		})
		s.ErrorAs(err, new(*serviceerror.NotFound))
	})

	s.Run("AlreadyDeleted", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		s.eventuallyDeleted(env, s.T(), "test-op", startResp.RunId)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.ErrorAs(err, new(*serviceerror.NotFound))
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace: env.Namespace().String(),
		})
		s.Error(err)
		s.ErrorContains(err, "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationPoll() {
	s.Run("WaitStageStarted", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
		endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()
		handlerLink := &commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
			WorkflowId: "handler-workflow",
			RunId:      "handler-run-id",
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   7,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		}

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
		defer cancel()

		type pollResult struct {
			resp *workflowservice.PollNexusOperationExecutionResponse
			err  error
		}
		pollResultCh := make(chan pollResult, 1)
		pollStartedCh := make(chan struct{}, 1)

		go func() {
			pollStartedCh <- struct{}{}
			resp, err := env.FrontendClient().PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			})
			pollResultCh <- pollResult{resp: resp, err: err}
		}()

		select {
		case <-pollStartedCh:
		case <-ctx.Done():
			s.T().Fatal("PollNexusOperationExecution did not start before timeout")
		}

		// PollNexusOperationExecution should not resolve before the operation is started.
		select {
		case result := <-pollResultCh:
			s.NoError(result.err)
			s.T().Fatal("PollNexusOperationExecution returned before the operation started")
		default:
		}

		task, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			Identity:  "test-worker",
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		})
		s.NoError(err)

		expectedLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		startRequest := task.GetRequest().GetStartOperation()
		s.Len(startRequest.GetLinks(), 1)
		s.Equal(expectedLink.URL.String(), startRequest.GetLinks()[0].GetUrl())
		s.Equal(expectedLink.Type, startRequest.GetLinks()[0].GetType())

		_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			Identity:  "test-worker",
			TaskToken: task.GetTaskToken(),
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
							AsyncSuccess: &nexuspb.StartOperationResponse_Async{
								OperationToken: "test-operation-token",
								Links: commonnexus.ConvertLinksToProto([]nexus.Link{
									commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink),
								}),
							},
						},
					},
				},
			},
		})
		s.NoError(err)

		// Verify the poll result.
		select {
		case result := <-pollResultCh:
			s.NoError(result.err)
			protorequire.ProtoEqual(s.T(), &workflowservice.PollNexusOperationExecutionResponse{
				RunId:          startResp.RunId,
				WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
				OperationToken: "test-operation-token",
			}, result.resp)
		case <-ctx.Done():
			s.T().Fatal("PollNexusOperationExecution did not resolve before timeout")
		}

		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		protorequire.ProtoSliceEqual(s.T(), []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: handlerLink,
				},
			},
		}, descResp.GetInfo().GetLinks())
	})

	s.Run("WaitStageClosed", func(s *NexusStandaloneTestSuite) {
		for _, tc := range []struct {
			name      string
			withRunID bool
		}{
			{name: "WithEmptyRunID"},
			{name: "WithRunID", withRunID: true},
		} {
			s.Run(tc.name, func(s *NexusStandaloneTestSuite) {
				env := s.newTestEnv()
				endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

				startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
					OperationId: "test-op",
					Endpoint:    endpointName,
				})
				s.NoError(err)

				ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
				defer cancel()

				// Start pollling.
				type pollResult struct {
					resp *workflowservice.PollNexusOperationExecutionResponse
					err  error
				}
				pollResultCh := make(chan pollResult, 1)
				pollStartedCh := make(chan struct{}, 1)

				go func() {
					runID := ""
					if tc.withRunID {
						runID = startResp.RunId
					}

					pollStartedCh <- struct{}{}
					resp, err := env.FrontendClient().PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
						Namespace:   env.Namespace().String(),
						OperationId: "test-op",
						RunId:       runID,
						WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					})
					pollResultCh <- pollResult{resp: resp, err: err}
				}()

				select {
				case <-pollStartedCh:
				case <-ctx.Done():
					s.T().Fatal("PollNexusOperationExecution did not start before timeout")
				}

				// PollNexusOperationExecution should not resolve before the operation is closed.
				select {
				case result := <-pollResultCh:
					s.NoError(result.err)
					s.T().Fatal("PollNexusOperationExecution returned before the state changed")
				default:
				}

				// Terminate the operation.
				terminateErrCh := make(chan error, 1)
				go func() {
					_, err := env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
						Namespace:   env.Namespace().String(),
						OperationId: "test-op",
						RunId:       startResp.RunId,
						Reason:      "test termination",
					})
					terminateErrCh <- err
				}()

				select {
				case err := <-terminateErrCh:
					s.NoError(err)
				case <-ctx.Done():
					s.T().Fatal("TerminateNexusOperationExecution timed out")
				}

				// Verify the poll result.
				var result pollResult
				select {
				case result = <-pollResultCh:
				case <-ctx.Done():
					s.T().Fatal("PollNexusOperationExecution did not resolve before timeout")
				}
				s.NoError(result.err)
				pollResp := result.resp

				protorequire.ProtoEqual(s.T(), &workflowservice.PollNexusOperationExecutionResponse{
					RunId:          startResp.RunId,
					WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					OperationToken: pollResp.GetOperationToken(),
					Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{
						Failure: pollResp.GetFailure(),
					},
				}, pollResp)
				s.NotNil(pollResp.GetFailure().GetTerminatedFailureInfo())
			})
		}
	})

	s.Run("UnspecifiedWaitStageDefaultsToClosed", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			Reason:      "test termination",
		})
		s.NoError(err)

		// Poll with UNSPECIFIED WaitStage — should behave the same as CLOSED.
		pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED,
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), &workflowservice.PollNexusOperationExecutionResponse{
			RunId:          startResp.RunId,
			WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			OperationToken: pollResp.GetOperationToken(),
			Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{
				Failure: pollResp.GetFailure(),
			},
		}, pollResp)
		s.NotNil(pollResp.GetFailure().GetTerminatedFailureInfo())
	})

	s.Run("ReturnsLastAttemptFailure", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
		endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:            "test-op",
			Endpoint:               endpointName,
			ScheduleToCloseTimeout: durationpb.New(4 * time.Second),
		})
		s.NoError(err)

		ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
		defer cancel()

		pollerErrCh := make(chan error, 1)
		go func() {
			task, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-worker",
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			})
			if err != nil {
				pollerErrCh <- err
				return
			}
			_, err = env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-worker",
				TaskToken: task.GetTaskToken(),
				Error: &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure: &nexuspb.Failure{
						Message: "last attempt failure",
					},
				},
			})
			pollerErrCh <- err
		}()

		s.EventuallyWithT(func(t *assert.CollectT) {
			pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, pollResp.GetWaitStage())
			protorequire.ProtoEqual(t, &failurepb.Failure{
				Message: "operation timed out",
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
					TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
						TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
					},
				},
			}, pollResp.GetFailure())
		}, 10*time.Second, 100*time.Millisecond)

		s.NoError(<-pollerErrCh)
	})

	s.Run("NamespaceNotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   "non-existent-namespace",
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		var namespaceNotFoundErr *serviceerror.NamespaceNotFound
		s.ErrorAs(err, &namespaceNotFoundErr)
		s.Contains(namespaceNotFoundErr.Error(), "non-existent-namespace")
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "non-existent-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Equal("operation not found for ID: non-existent-op", notFoundErr.Error())
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := s.newTestEnv()

		_, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "", // required field
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestAsyncCompletionIgnoresTransitionFieldsInCallbackToken() {
	env := s.newTestEnv()
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: "handler-workflow",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   7,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	handlerNexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink)

	type callbackInfo struct {
		token string
		url   string
	}
	callbackCh := make(chan callbackInfo, 1)
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service string,
			operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			callbackCh <- callbackInfo{
				token: options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
				url:   options.CallbackURL,
			}
			nexus.AddHandlerLinks(ctx, handlerNexusLink)
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-operation-token"}, nil
		},
	})

	_, err := env.OperatorClient().CreateNexusEndpoint(env.Context(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: "test-op",
		Endpoint:    endpointName,
	})
	s.NoError(err)

	ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
	defer cancel()
	var callbackToken string
	var callbackURL string
	select {
	case callback := <-callbackCh:
		callbackToken = callback.token
		callbackURL = callback.url
	case <-ctx.Done():
		s.FailNow("timed out waiting for Nexus callback details", ctx.Err().Error())
	}

	gen := &commonnexus.CallbackTokenGenerator{}
	decodedToken, err := commonnexus.DecodeCallbackToken(callbackToken)
	s.NoError(err)
	completionToken, err := gen.DecodeCompletion(decodedToken)
	s.NoError(err)

	// Deliberately corrupt transition fields in the callback token. Completion should
	// still succeed because the handler strips these fields before validation.
	ref := &persistencespb.ChasmComponentRef{}
	s.NoError(ref.Unmarshal(completionToken.GetComponentRef()))
	s.NotNil(ref.ExecutionVersionedTransition)
	s.NotNil(ref.ComponentInitialVersionedTransition)
	ref.ExecutionVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: ref.ExecutionVersionedTransition.NamespaceFailoverVersion + 1000,
		TransitionCount:          ref.ExecutionVersionedTransition.TransitionCount + 1000,
	}
	ref.ComponentInitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: ref.ComponentInitialVersionedTransition.NamespaceFailoverVersion + 1000,
		TransitionCount:          ref.ComponentInitialVersionedTransition.TransitionCount + 1000,
	}
	completionToken.ComponentRef, err = ref.Marshal()
	s.NoError(err)

	callbackToken, err = gen.Tokenize(completionToken)
	s.NoError(err)

	c := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	err = c.CompleteOperation(ctx, callbackURL, nexusrpc.CompleteOperationOptions{
		Result: payload.EncodeString("result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:      env.Namespace().String(),
			OperationId:    "test-op",
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		protorequire.ProtoEqual(t, payload.EncodeString("result"), descResp.GetResult())
		protorequire.ProtoSliceEqual(t, []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: handlerLink,
				},
			},
		}, descResp.GetInfo().GetLinks())
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *NexusStandaloneTestSuite) startNexusOperation(
	env *NexusTestEnv,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	req.Namespace = cmp.Or(req.Namespace, env.Namespace().String())
	req.Service = cmp.Or(req.Service, "test-service")
	req.Operation = cmp.Or(req.Operation, "test-operation")
	req.RequestId = cmp.Or(req.RequestId, env.Tv().RequestID())
	if req.ScheduleToCloseTimeout == nil {
		req.ScheduleToCloseTimeout = durationpb.New(10 * time.Minute)
	}

	return env.FrontendClient().StartNexusOperationExecution(env.Context(), req)
}

func (s *NexusStandaloneTestSuite) eventuallyDeleted(env *NexusTestEnv, t *testing.T, operationID, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       runID,
		})
		_, ok := errors.AsType[*serviceerror.NotFound](err)
		return ok
	}, 10*time.Second, 100*time.Millisecond)
}
