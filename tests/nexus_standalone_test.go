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
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

var nexusStandaloneOpts = []testcore.TestOption{
	testcore.WithDedicatedCluster(),
	testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	testcore.WithDynamicConfig(nexusoperation.Enabled, true),
}

func TestStartStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("StartAndDescribe", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		testInput := payload.EncodeString("test-input")
		testHeader := map[string]string{"test-key": "test-value"}
		testUserMetadata := &sdkpb.UserMetadata{
			Summary: payload.EncodeString("test-summary"),
			Details: payload.EncodeString("test-details"),
		}
		testSearchAttributes := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("test-value"),
			},
		}
		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			Endpoint:         endpointName,
			Input:            testInput,
			NexusHeader:      testHeader,
			UserMetadata:     testUserMetadata,
			SearchAttributes: testSearchAttributes,
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
			t.Run(tc.name, func(t *testing.T) {
				descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
					Namespace:   s.Namespace().String(),
					OperationId: "test-op",
					RunId:       tc.runID,
				})
				s.NoError(err)
				s.Equal(startResp.RunId, descResp.RunId)

				info := descResp.GetInfo()
				protorequire.ProtoEqual(t, &nexuspb.NexusOperationExecutionInfo{
					OperationId:            "test-op",
					RunId:                  startResp.RunId,
					Endpoint:               endpointName,
					Service:                "test-service",
					Operation:              "test-operation",
					Status:                 enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
					State:                  enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED,
					ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
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

		// Describe with IncludeInput.
		descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:    s.Namespace().String(),
			OperationId:  "test-op",
			RunId:        startResp.RunId,
			IncludeInput: true,
		})
		s.NoError(err)
		protorequire.ProtoEqual(t, testInput, descResp.GetInput())
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("Validation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})

	t.Run("IDConflictPolicyFail", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		resp1, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Second start with different request ID should fail.
		_, err = startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			RequestId:   "different-request-id",
		})
		s.Error(err)
		var alreadyStartedErr *serviceerror.AlreadyExists
		s.ErrorAs(err, &alreadyStartedErr)
		s.ErrorContains(err, "nexus operation execution already started")

		// Second start with same request ID should return existing run.
		resp2, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})

	t.Run("IDConflictPolicyUseExisting", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		resp1, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		resp2, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
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

func TestDescribeStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("NotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
		s.Equal("operation not found for ID: does-not-exist", notFound.Error())
	})

	t.Run("LongPollStateChange", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Obtain longpoll token.
		firstResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.NotEmpty(firstResp.GetLongPollToken())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, firstResp.GetInfo().GetStatus())

		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()

		// Start polling.
		type describeResult struct {
			resp *workflowservice.DescribeNexusOperationExecutionResponse
			err  error
		}
		describeResultCh := make(chan describeResult, 1)

		go func() {
			resp, err := s.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:      s.Namespace().String(),
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
			require.NoError(t, result.err)
			t.Fatal("DescribeNexusOperationExecution returned before the state changed")
		case <-time.After(1 * time.Second):
		}

		// Terminate the operation.
		terminateErrCh := make(chan error, 1)
		go func() {
			_, err := s.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   s.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				Reason:      "test termination",
			})
			terminateErrCh <- err
		}()

		select {
		case err := <-terminateErrCh:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("TerminateNexusOperationExecution timed out")
		}

		// Verify the longpoll result.
		var longPollResp *workflowservice.DescribeNexusOperationExecutionResponse
		select {
		case result := <-describeResultCh:
			require.NoError(t, result.err)
			longPollResp = result.resp
		case <-ctx.Done():
			t.Fatal("DescribeNexusOperationExecution timed out")
		}

		s.Equal(startResp.RunId, longPollResp.GetRunId())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, longPollResp.GetInfo().GetStatus())
		s.Greater(longPollResp.GetInfo().GetStateTransitionCount(), firstResp.GetInfo().GetStateTransitionCount())
	})

	t.Run("LongPollTimeoutReturnsEmptyResponse", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		firstResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.NotEmpty(firstResp.GetLongPollToken())

		t.Run("CallerDeadlineNotExceeded", func(t *testing.T) {
			s.OverrideDynamicConfig(nexusoperation.LongPollBuffer, time.Second)
			s.OverrideDynamicConfig(nexusoperation.LongPollTimeout, 10*time.Millisecond)

			ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
			defer cancel()

			longPollResp, err := s.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:     s.Namespace().String(),
				OperationId:   "test-op",
				RunId:         startResp.RunId,
				LongPollToken: firstResp.GetLongPollToken(),
			})
			s.NoError(err)
			protorequire.ProtoEqual(t, &workflowservice.DescribeNexusOperationExecutionResponse{}, longPollResp)
		})

		t.Run("NoCallerDeadline", func(t *testing.T) {
			// Frontend still imposes its own deadline upstream, so the buffer must fit within that.
			s.OverrideDynamicConfig(nexusoperation.LongPollBuffer, 29*time.Second)
			s.OverrideDynamicConfig(nexusoperation.LongPollTimeout, 10*time.Millisecond)

			longPollResp, err := s.FrontendClient().DescribeNexusOperationExecution(context.Background(), &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:     s.Namespace().String(),
				OperationId:   "test-op",
				RunId:         startResp.RunId,
				LongPollToken: firstResp.GetLongPollToken(),
			})
			s.NoError(err)
			protorequire.ProtoEqual(t, &workflowservice.DescribeNexusOperationExecutionResponse{}, longPollResp)
		})
	})

	t.Run("IncludeOutcome_Success", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		taskQueue := testcore.RandomizedNexusEndpoint(t.Name())
		endpointName := createNexusEndpointWithTaskQueue(s, taskQueue)
		expectedResult := payload.EncodeString("successful result")
		handlerLink := &commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: "handler-workflow",
			RunId:      "handler-run-id",
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   7,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		}

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()

		pollerErrCh := make(chan error, 1)
		go func() {
			task, err := s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: s.Namespace().String(),
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

			_, err = s.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
				Namespace: s.Namespace().String(),
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

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:      s.Namespace().String(),
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
			// TODO(stephan): Add standalone async-start link coverage once the async completion path is wired up.

			pollResp, err := s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   s.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, expectedResult, pollResp.GetResult())
		}, 10*time.Second, 100*time.Millisecond)

		s.NoError(<-pollerErrCh)
	})

	t.Run("IncludeOutcome_Failure", func(t *testing.T) {
		testCases := []struct {
			name                   string
			setup                  func(*workflowservice.StartNexusOperationExecutionRequest)
			respond                func(context.Context, *testcore.TestEnv, *workflowservice.PollNexusTaskQueueResponse) error
			expectedStatus         enumspb.NexusOperationExecutionStatus
			expectedFailureMessage string
		}{
			{
				name: "TimeoutLastAttemptFailure",
				setup: func(req *workflowservice.StartNexusOperationExecutionRequest) {
					req.ScheduleToCloseTimeout = durationpb.New(2 * time.Second)
				},
				respond: func(ctx context.Context, s *testcore.TestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "last attempt failure")
					temporalFailure, err := temporalNexusFailure(handlerErr)
					require.NoError(t, err)

					_, err = s.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
						Namespace: s.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Failure:   temporalFailure,
					})
					return err
				},
				expectedStatus:         enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT,
				expectedFailureMessage: "last attempt failure",
			},
			{
				name: "Canceled",
				respond: func(ctx context.Context, s *testcore.TestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := s.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
						Namespace: s.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Response: &nexuspb.Response{
							Variant: &nexuspb.Response_StartOperation{
								StartOperation: &nexuspb.StartOperationResponse{
									Variant: &nexuspb.StartOperationResponse_Failure{
										Failure: &failurepb.Failure{
											Message: "cancel failure",
											FailureInfo: &failurepb.Failure_CanceledFailureInfo{
												CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
											},
										},
									},
								},
							},
						},
					})
					return err
				},
				expectedStatus:         enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED,
				expectedFailureMessage: "cancel failure",
			},
			{
				name: "TerminalFailure",
				respond: func(ctx context.Context, s *testcore.TestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := s.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
						Namespace: s.Namespace().String(),
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
				expectedStatus:         enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED,
				expectedFailureMessage: "final failure",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s := testcore.NewEnv(t, nexusStandaloneOpts...)
				taskQueue := testcore.RandomizedNexusEndpoint(t.Name())
				endpointName := createNexusEndpointWithTaskQueue(s, taskQueue)
				startReq := &workflowservice.StartNexusOperationExecutionRequest{
					OperationId: "test-op",
					Endpoint:    endpointName,
				}
				if tc.setup != nil {
					tc.setup(startReq)
				}

				startResp, err := startNexusOperation(s, startReq)
				s.NoError(err)

				ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
				defer cancel()

				pollerErrCh := make(chan error, 1)
				go func() {
					task, err := s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
						Namespace: s.Namespace().String(),
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
					pollerErrCh <- tc.respond(ctx, s, task)
				}()

				require.EventuallyWithT(t, func(t *assert.CollectT) {
					descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
						Namespace:      s.Namespace().String(),
						OperationId:    "test-op",
						RunId:          startResp.RunId,
						IncludeOutcome: true,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectedStatus, descResp.GetInfo().GetStatus())
					require.Equal(t, tc.expectedFailureMessage, descResp.GetFailure().GetMessage())

					pollResp, err := s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
						Namespace:   s.Namespace().String(),
						OperationId: "test-op",
						RunId:       startResp.RunId,
						WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectedFailureMessage, pollResp.GetFailure().GetMessage())
				}, 10*time.Second, 100*time.Millisecond)

				s.NoError(<-pollerErrCh)
			})
		}
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("Validation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace: s.Namespace().String(),
		})
		s.Error(err)
		s.ErrorContains(err, "operation_id is required")
	})
}

func TestStandaloneNexusOperationCancel(t *testing.T) {
	t.Parallel()

	t.Run("RequestCancel", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		_, err = s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		// Verify state after cancel — operation is still running
		descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		protorequire.ProtoEqual(t, &nexuspb.NexusOperationExecutionInfo{
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

	t.Run("AlreadyCanceled", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Cancel the operation.
		_, err = s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "cancel-request-id",
		})
		s.NoError(err)

		// Cancel again with same request ID — should be idempotent.
		_, err = s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "cancel-request-id",
		})
		s.NoError(err)

		// Cancel with a different request ID — should error.
		_, err = s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "different-request-id",
		})
		s.Error(err)
		s.Contains(err.Error(), "cancellation already requested")
	})

	t.Run("AlreadyTerminated", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation first.
		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Cancel a terminated operation — should error.
		_, err = s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.Error(err)
		s.Contains(err.Error(), "operation already completed")
	})

	t.Run("NotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("Validation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func TestTerminateStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("Terminate", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Verify outcome after terminate.
		descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:      s.Namespace().String(),
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
	})

	t.Run("AlreadyTerminated", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation.
		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Terminate again with same request ID — should be idempotent.
		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination again",
		})
		s.NoError(err)

		// Terminate with a different request ID — should error.
		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "different-request-id",
			Reason:      "test termination different",
		})
		s.Error(err)
		s.Contains(err.Error(), "already terminated")
	})

	t.Run("AlreadyCanceled", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Cancel the operation first.
		_, err = s.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		// Terminate a canceled operation — should succeed.
		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Verify state changed to terminated (terminate overrides cancel request).
		descResp, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	})

	t.Run("NotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "does-not-exist",
			Reason:      "test termination",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("Validation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func TestListStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("ListAndVerifyFields", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "list-test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "OperationId = 'list-test-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		op := listResp.GetOperations()[0]
		protorequire.ProtoEqual(t, &nexuspb.NexusOperationExecutionListInfo{
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
		require.NotNil(t, op.GetScheduleTime())
	})

	t.Run("ListWithQueryFilter", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointA := createNexusEndpoint(s)
		endpointB := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "filter-op-1",
			Endpoint:    endpointA,
		})
		s.NoError(err)

		_, err = startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "filter-op-2",
			Endpoint:    endpointB,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointA),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "filter-op-1", listResp.GetOperations()[0].GetOperationId())
	})

	t.Run("ListWithCustomSearchAttributes", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		testSA := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("list-sa-value"),
			},
		}
		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "sa-op",
			Endpoint:         endpointName,
			SearchAttributes: testSA,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "CustomKeywordField = 'list-sa-value'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "sa-op", listResp.GetOperations()[0].GetOperationId())
		returnedSA := listResp.GetOperations()[0].GetSearchAttributes().GetIndexedFields()["CustomKeywordField"]
		require.NotNil(t, returnedSA)
		var returnedValue string
		require.NoError(t, payload.Decode(returnedSA, &returnedValue))
		require.Equal(t, "list-sa-value", returnedValue)
	})

	t.Run("QueryByExecutionStatus", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "ExecutionStatus = 'Running' AND OperationId = 'status-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "status-op", listResp.GetOperations()[0].GetOperationId())
	})

	t.Run("QueryByMultipleFields", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "multi-op",
			Endpoint:    endpointName,
			Service:     "multi-service",
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' AND Service = 'multi-service'", endpointName),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Equal(t, "multi-op", listResp.GetOperations()[0].GetOperationId())
	})

	t.Run("PageSizeCapping", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 2 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("paged-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		// Wait for both to be indexed.
		query := fmt.Sprintf("Endpoint = '%s'", endpointName)
		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     query,
			})
			require.NoError(t, err)
			require.Len(t, resp.GetOperations(), 2)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)

		// Override max page size to 1.
		s.OverrideDynamicConfig(dynamicconfig.FrontendVisibilityMaxPageSize, 1)

		// PageSize 0 should default to max (1), returning only 1 result.
		resp, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  0,
			Query:     query,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetOperations(), 1)

		// PageSize > max should also be capped. First page.
		resp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  2,
			Query:     query,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetOperations(), 1)

		// Second page.
		resp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     s.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, resp.GetOperations(), 1)

		// No more results.
		resp, err = s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     s.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetOperations())
		require.Nil(t, resp.GetNextPageToken())
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func TestCountStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("CountByOperationID", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "OperationId = 'count-op'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("CountByEndpoint", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 3 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("count-ep-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("CountByExecutionStatus", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("ExecutionStatus = 'Running' AND Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("GroupByExecutionStatus", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 3 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("group-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		var countResp *workflowservice.CountNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			countResp, err = s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' GROUP BY ExecutionStatus", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), countResp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		require.Len(t, countResp.GetGroups(), 1)
		require.Equal(t, int64(3), countResp.GetGroups()[0].GetCount())
		var groupValue string
		require.NoError(t, payload.Decode(countResp.GetGroups()[0].GetGroupValues()[0], &groupValue))
		require.Equal(t, "Running", groupValue)
	})

	t.Run("CountByCustomSearchAttribute", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		for i := range 2 {
			_, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
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
			resp, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "CustomKeywordField = 'count-sa-value'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(2), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("GroupByUnsupportedField", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "GROUP BY Endpoint",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "'GROUP BY' clause is only supported for ExecutionStatus")
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().CountNexusOperationExecutions(s.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func TestDeleteStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	t.Run("Scheduled", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = s.FrontendClient().DeleteNexusOperationExecution(s.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		eventuallyNexusOperationDeleted(s, t, "test-op", startResp.RunId)
	})

	t.Run("NoRunID", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			// RunId not set
		})
		s.NoError(err)

		_, err = s.FrontendClient().DeleteNexusOperationExecution(s.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
		})
		s.NoError(err)

		eventuallyNexusOperationDeleted(s, t, "test-op", startResp.RunId)
	})

	t.Run("NotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().DeleteNexusOperationExecution(s.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "does-not-exist",
		})
		s.ErrorAs(err, new(*serviceerror.NotFound))
	})

	t.Run("AlreadyDeleted", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = s.FrontendClient().DeleteNexusOperationExecution(s.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		eventuallyNexusOperationDeleted(s, t, "test-op", startResp.RunId)

		_, err = s.FrontendClient().DeleteNexusOperationExecution(s.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.ErrorAs(err, new(*serviceerror.NotFound))
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("Validation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().DeleteNexusOperationExecution(s.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace: s.Namespace().String(),
		})
		s.Error(err)
		s.ErrorContains(err, "operation_id is required")
	})
}

func TestStandaloneNexusOperationPoll(t *testing.T) {
	t.Parallel()

	t.Run("WaitStageClosed", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			withRunID bool
		}{
			{name: "WithEmptyRunID"},
			{name: "WithRunID", withRunID: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := testcore.NewEnv(t, nexusStandaloneOpts...)
				endpointName := createNexusEndpoint(s)

				startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
					OperationId: "test-op",
					Endpoint:    endpointName,
				})
				require.NoError(t, err)

				ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
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
					resp, err := s.FrontendClient().PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
						Namespace:   s.Namespace().String(),
						OperationId: "test-op",
						RunId:       runID,
						WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					})
					pollResultCh <- pollResult{resp: resp, err: err}
				}()

				select {
				case <-pollStartedCh:
				case <-ctx.Done():
					t.Fatal("PollNexusOperationExecution did not start before timeout")
				}

				// PollNexusOperationExecution should not resolve before the operation is closed.
				select {
				case result := <-pollResultCh:
					require.NoError(t, result.err)
					t.Fatal("PollNexusOperationExecution returned before the state changed")
				default:
				}

				// Terminate the operation.
				terminateErrCh := make(chan error, 1)
				go func() {
					_, err := s.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
						Namespace:   s.Namespace().String(),
						OperationId: "test-op",
						RunId:       startResp.RunId,
						Reason:      "test termination",
					})
					terminateErrCh <- err
				}()

				select {
				case err := <-terminateErrCh:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("TerminateNexusOperationExecution timed out")
				}

				// Verify the poll result.
				var result pollResult
				select {
				case result = <-pollResultCh:
				case <-ctx.Done():
					t.Fatal("PollNexusOperationExecution did not resolve before timeout")
				}
				require.NoError(t, result.err)
				pollResp := result.resp

				protorequire.ProtoEqual(t, &workflowservice.PollNexusOperationExecutionResponse{
					RunId:          startResp.RunId,
					WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					OperationToken: pollResp.GetOperationToken(),
					Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{
						Failure: pollResp.GetFailure(),
					},
				}, pollResp)
				require.NotNil(t, pollResp.GetFailure().GetTerminatedFailureInfo())
			})
		}
	})

	t.Run("UnspecifiedWaitStageDefaultsToClosed", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		require.NoError(t, err)

		// Terminate the operation.
		_, err = s.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			Reason:      "test termination",
		})
		require.NoError(t, err)

		// Poll with UNSPECIFIED WaitStage — should behave the same as CLOSED.
		pollResp, err := s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &workflowservice.PollNexusOperationExecutionResponse{
			RunId:          startResp.RunId,
			WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			OperationToken: pollResp.GetOperationToken(),
			Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{
				Failure: pollResp.GetFailure(),
			},
		}, pollResp)
		require.NotNil(t, pollResp.GetFailure().GetTerminatedFailureInfo())
	})

	t.Run("ReturnsLastAttemptFailure", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		taskQueue := testcore.RandomizedNexusEndpoint(t.Name())
		endpointName := createNexusEndpointWithTaskQueue(s, taskQueue)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:            "test-op",
			Endpoint:               endpointName,
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		})
		s.NoError(err)

		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()

		pollerErrCh := make(chan error, 1)
		go func() {
			task, err := s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: s.Namespace().String(),
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

			handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "last attempt failure")
			temporalFailure, err := temporalNexusFailure(handlerErr)
			if err != nil {
				pollerErrCh <- err
				return
			}

			_, err = s.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
				Namespace: s.Namespace().String(),
				Identity:  "test-worker",
				TaskToken: task.GetTaskToken(),
				Failure:   temporalFailure,
			})
			pollerErrCh <- err
		}()

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			pollResp, err := s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   s.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, pollResp.GetWaitStage())
			require.Equal(t, "last attempt failure", pollResp.GetFailure().GetMessage())
		}, 10*time.Second, 100*time.Millisecond)

		s.NoError(<-pollerErrCh)
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   "non-existent-namespace",
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		var namespaceNotFoundErr *serviceerror.NamespaceNotFound
		require.ErrorAs(t, err, &namespaceNotFoundErr)
		require.Contains(t, namespaceNotFoundErr.Error(), "non-existent-namespace")
	})

	t.Run("NotFound", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)
		endpointName := createNexusEndpoint(s)

		startResp, err := startNexusOperation(s, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "non-existent-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, "operation not found for ID: non-existent-op", notFoundErr.Error())
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	t.Run("Validation", func(t *testing.T) {
		s := testcore.NewEnv(t, nexusStandaloneOpts...)

		_, err := s.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: "", // required field
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func startNexusOperation(
	s *testcore.TestEnv,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	req.Namespace = cmp.Or(req.Namespace, s.Namespace().String())
	req.Service = cmp.Or(req.Service, "test-service")
	req.Operation = cmp.Or(req.Operation, "test-operation")
	req.RequestId = cmp.Or(req.RequestId, s.Tv().RequestID())
	if req.ScheduleToCloseTimeout == nil {
		req.ScheduleToCloseTimeout = durationpb.New(10 * time.Minute)
	}
	return s.FrontendClient().StartNexusOperationExecution(s.Context(), req)
}

func createNexusEndpoint(s *testcore.TestEnv) string {
	return createNexusEndpointWithTaskQueue(s, "unused-for-test")
}

func createNexusEndpointWithTaskQueue(s *testcore.TestEnv, taskQueue string) string {
	name := testcore.RandomizedNexusEndpoint(s.T().Name())
	_, err := s.OperatorClient().CreateNexusEndpoint(s.Context(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)
	return name
}

func eventuallyNexusOperationDeleted(s *testcore.TestEnv, t *testing.T, operationID, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := s.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   s.Namespace().String(),
			OperationId: operationID,
			RunId:       runID,
		})
		var notFoundErr *serviceerror.NotFound
		return errors.As(err, &notFoundErr)
	}, 10*time.Second, 100*time.Millisecond)
}

func temporalNexusFailure(handlerErr *nexus.HandlerError) (*failurepb.Failure, error) {
	nexusFailure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(handlerErr)
	if err != nil {
		return nil, err
	}
	return commonnexus.NexusFailureToTemporalFailure(nexusFailure)
}
