package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	callback "go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type StandaloneCallbackSuite struct {
	testcore.FunctionalTestBase
}

func TestStandaloneCallbackSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(StandaloneCallbackSuite))
}

func (s *StandaloneCallbackSuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():          true,
			dynamicconfig.EnableCHASMCallbacks.Key(): true,
			callback.Enabled.Key():                   true,
			callback.AllowedAddresses.Key(): []any{
				map[string]any{"Pattern": "*", "AllowInsecure": true},
			},
		}),
	)
}

// startCallbackExecution creates a standalone callback execution with the given parameters.
// If cb is nil, a default non-routable callback is used.
// If completion is nil, a default success completion is used.
func (s *StandaloneCallbackSuite) startCallbackExecution(
	ctx context.Context,
	callbackID string,
	cb *commonpb.Callback,
	completion *callbackpb.CallbackExecutionCompletion,
	timeout time.Duration,
) (*workflowservice.StartCallbackExecutionResponse, error) {
	s.T().Helper()
	if cb == nil {
		cb = &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost:1/nonexistent",
				},
			},
		}
	}
	if completion == nil {
		completion = &callbackpb.CallbackExecutionCompletion{
			Result: &callbackpb.CallbackExecutionCompletion_Success{
				Success: testcore.MustToPayload(s.T(), "some-result"),
			},
		}
	}
	return s.FrontendClient().StartCallbackExecution(ctx, &workflowservice.StartCallbackExecutionRequest{
		Namespace:              s.Namespace().String(),
		Identity:               "test",
		RequestId:              uuid.NewString(),
		CallbackId:             callbackID,
		Callback:               cb,
		Input:                  &workflowservice.StartCallbackExecutionRequest_Completion{Completion: completion},
		ScheduleToCloseTimeout: durationpb.New(timeout),
	})
}

// mustStartCallbackExecution calls startCallbackExecution and asserts no error and non-empty run_id.
func (s *StandaloneCallbackSuite) mustStartCallbackExecution(
	ctx context.Context,
	callbackID string,
	cb *commonpb.Callback,
	completion *callbackpb.CallbackExecutionCompletion,
	timeout time.Duration,
) *workflowservice.StartCallbackExecutionResponse {
	s.T().Helper()
	resp, err := s.startCallbackExecution(ctx, callbackID, cb, completion, timeout)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	return resp
}

// TestNexusOperationCompletionViaStandaloneCallback tests that a Nexus operation started by a
// workflow can be completed by using the standalone StartCallbackExecution API to deliver the
// Nexus completion to the operation's callback URL.
//
// Flow:
//  1. A caller workflow starts a Nexus operation via an external endpoint
//  2. The external Nexus handler starts the operation asynchronously and captures the callback URL/token
//  3. StartCallbackExecution is called with the callback URL/token and a success payload
//  4. The CHASM callback execution delivers the Nexus completion to the callback URL
//  5. The caller workflow receives the operation result and completes
func (s *StandaloneCallbackSuite) TestNexusOperationCompletionViaStandaloneCallback() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	// Channel to transmit callback info from the Nexus handler to the goroutine
	// that will call StartCallbackExecution (simulating an external system receiving
	// the callback details and independently completing the operation).
	type callbackInfo struct {
		Token string
		URL   string
	}
	callbackCh := make(chan callbackInfo, 1)

	// Set up an external Nexus handler that starts operations asynchronously
	// and sends the callback URL + token to the completion goroutine.
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			callbackCh <- callbackInfo{
				Token: options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
				URL:   options.CallbackURL,
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: "test",
			}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	// Goroutine that waits for the callback info from the Nexus handler and then
	// calls StartCallbackExecution to deliver the completion — simulating an
	// external system that receives the callback details out-of-band and
	// independently completes the Nexus operation.
	callbackID := "test-callback-" + uuid.NewString()
	completionErrCh := make(chan error, 1)
	go func() {
		select {
		case info := <-callbackCh:
			_, err := s.startCallbackExecution(ctx, callbackID, &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:   info.URL,
						Token: info.Token,
					},
				},
			}, &callbackpb.CallbackExecutionCompletion{
				Result: &callbackpb.CallbackExecutionCompletion_Success{
					Success: testcore.MustToPayload(s.T(), "result-from-standalone-callback"),
				},
			}, time.Minute)
			completionErrCh <- err
		case <-ctx.Done():
			completionErrCh <- ctx.Err()
		}
	}()

	// Register a caller workflow that starts a Nexus operation and waits for its result.
	callerWf := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		fut := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	w := worker.New(s.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWf)
	s.NoError(w.Start())
	defer w.Stop()

	// Start the caller workflow.
	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWf)
	s.NoError(err)

	// Verify the StartCallbackExecution call succeeded.
	s.NoError(<-completionErrCh)

	// The standalone callback delivers the completion to the Nexus callback URL,
	// which completes the Nexus operation in the caller workflow.
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result-from-standalone-callback", result)

	// Describe the callback execution and verify its state.
	descResp, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
	})
	s.NoError(err)
	s.NotNil(descResp.GetInfo())
	s.Equal(callbackID, descResp.GetInfo().GetCallbackId())
	s.NotNil(descResp.GetInfo().GetCreateTime())

	// Poll to verify the outcome is a success with no failure.
	pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
	})
	s.NoError(err)
	s.NotNil(pollResp.GetOutcome().GetSuccess())
	s.Nil(pollResp.GetOutcome().GetFailure())
}

// TestPollCallbackExecution tests that PollCallbackExecution long-polls for the outcome
// of a callback execution. It returns an empty response when the poll times out, and
// the outcome when the callback reaches a terminal state.
func (s *StandaloneCallbackSuite) TestPollCallbackExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	s.Run("returns_empty_for_non_terminal", func() {
		callbackID := "poll-test-" + uuid.NewString()
		s.mustStartCallbackExecution(ctx, callbackID, nil, nil, time.Minute)

		// Poll a non-terminal callback with a short timeout — should return an empty response.
		shortCtx, shortCancel := context.WithTimeout(ctx, time.Second*2)
		defer shortCancel()
		pollResp, err := s.FrontendClient().PollCallbackExecution(shortCtx, &workflowservice.PollCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
		})
		s.NoError(err)
		s.Nil(pollResp.GetOutcome())

		// Terminate, then poll should return the outcome.
		_, err = s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
			Identity:   "test",
			RequestId:  uuid.NewString(),
			Reason:     "testing poll",
		})
		s.NoError(err)

		pollResp, err = s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
		})
		s.NoError(err)
		s.NotNil(pollResp.GetOutcome().GetFailure())
		s.Equal("testing poll", pollResp.GetOutcome().GetFailure().GetMessage())
	})

	s.Run("blocks_until_complete", func() {
		callbackID := "poll-blocks-" + uuid.NewString()
		s.mustStartCallbackExecution(ctx, callbackID, nil, nil, time.Minute)

		// Start a long-poll in a goroutine.
		type pollResult struct {
			resp *workflowservice.PollCallbackExecutionResponse
			err  error
		}
		resultCh := make(chan pollResult, 1)
		go func() {
			resp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
				Namespace:  s.Namespace().String(),
				CallbackId: callbackID,
			})
			resultCh <- pollResult{resp: resp, err: err}
		}()

		// Verify the poll is still blocking (hasn't returned yet).
		select {
		case <-resultCh:
			s.Fail("expected poll to block, but it returned before terminate")
		case <-time.After(500 * time.Millisecond):
		}

		_, err := s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
			Identity:   "test",
			RequestId:  uuid.NewString(),
			Reason:     "testing poll blocks",
		})
		s.NoError(err)

		result := <-resultCh
		s.NoError(result.err)
		s.NotNil(result.resp.GetOutcome().GetFailure())
		s.Equal("testing poll blocks", result.resp.GetOutcome().GetFailure().GetMessage())
	})
}

// TestDeleteCallbackExecution tests that a standalone callback execution can be deleted.
// Delete terminates the callback if it's still running, then marks it for cleanup.
func (s *StandaloneCallbackSuite) TestDeleteCallbackExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	// Create a callback that points to a non-existent URL so it won't complete on its own.
	// The callback will be in SCHEDULED/BACKING_OFF state when we delete it.
	callbackID := "delete-test-" + uuid.NewString()
	startResp := s.mustStartCallbackExecution(ctx, callbackID, nil, nil, time.Minute)
	runID := startResp.GetRunId()

	// Describe using run_id to verify it was created.
	descResp, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
	})
	s.NoError(err)
	s.Equal(callbackID, descResp.GetInfo().GetCallbackId())
	s.Equal(runID, descResp.GetInfo().GetRunId())

	// Delete with wrong run_id should fail.
	_, err = s.FrontendClient().DeleteCallbackExecution(ctx, &workflowservice.DeleteCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      uuid.NewString(),
	})
	s.Error(err)

	// Delete the callback execution using correct run_id.
	_, err = s.FrontendClient().DeleteCallbackExecution(ctx, &workflowservice.DeleteCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
	})
	s.NoError(err)

	// Describe after delete — the callback should be in TERMINATED state.
	descResp, err = s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
	})
	s.NoError(err)
	s.Equal(enumspb.CALLBACK_EXECUTION_STATE_TERMINATED, descResp.GetInfo().GetState())
	s.NotNil(descResp.GetInfo().GetCloseTime())
}

// TestNexusOperationFailureViaStandaloneCallback tests that a standalone callback can deliver
// a failure completion to a Nexus operation, causing the caller workflow to receive the failure.
func (s *StandaloneCallbackSuite) TestNexusOperationFailureViaStandaloneCallback() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	type callbackInfo struct {
		Token string
		URL   string
	}
	callbackCh := make(chan callbackInfo, 1)

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			callbackCh <- callbackInfo{
				Token: options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
				URL:   options.CallbackURL,
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: "test",
			}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	// Goroutine delivers a FAILURE completion via StartCallbackExecution.
	completionErrCh := make(chan error, 1)
	go func() {
		select {
		case info := <-callbackCh:
			callbackID := "failure-callback-" + uuid.NewString()
			_, err := s.startCallbackExecution(ctx, callbackID, &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:   info.URL,
						Token: info.Token,
					},
				},
			}, &callbackpb.CallbackExecutionCompletion{
				Result: &callbackpb.CallbackExecutionCompletion_Failure{
					Failure: &failurepb.Failure{
						Message: "operation failed from standalone callback",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								NonRetryable: true,
							},
						},
					},
				},
			}, time.Minute)
			completionErrCh <- err
		case <-ctx.Done():
			completionErrCh <- ctx.Err()
		}
	}()

	// Caller workflow starts a Nexus operation and expects a failure.
	callerWf := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		fut := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	w := worker.New(s.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWf)
	s.NoError(w.Start())
	defer w.Stop()

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWf)
	s.NoError(err)

	// Verify StartCallbackExecution succeeded.
	s.NoError(<-completionErrCh)

	// The workflow should fail with a NexusOperationError wrapping the failure.
	var result string
	err = run.Get(ctx, &result)
	s.Error(err)

	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(err, &wee)

	var noe *temporal.NexusOperationError
	s.ErrorAs(wee, &noe)
	s.Contains(noe.Error(), "operation failed from standalone callback")
}

// TestStartCallbackExecution_InvalidArguments verifies request validation.
func (s *StandaloneCallbackSuite) TestStartCallbackExecution_InvalidArguments() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	validCallback := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "http://localhost:1/callback",
			},
		},
	}
	validCompletion := &callbackpb.CallbackExecutionCompletion{
		Result: &callbackpb.CallbackExecutionCompletion_Success{
			Success: testcore.MustToPayload(s.T(), "result"),
		},
	}

	tests := []struct {
		name   string
		mutate func(req *workflowservice.StartCallbackExecutionRequest)
		errMsg string
	}{
		{
			name: "missing callback_id",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.CallbackId = ""
			},
			errMsg: "CallbackId is not set",
		},
		{
			name: "missing callback",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.Callback = nil
			},
			errMsg: "Callback is not set",
		},
		{
			name: "missing callback URL",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.Callback = &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{Url: ""},
					},
				}
			},
			errMsg: "Callback URL is not set",
		},
		{
			name: "invalid callback URL scheme",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.Callback = &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{Url: "ftp://example.com/callback"},
					},
				}
			},
			errMsg: "unknown scheme",
		},
		{
			name: "callback URL missing host",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.Callback = &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{Url: "http:///callback"},
					},
				}
			},
			errMsg: "missing host",
		},
		{
			name: "missing completion",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.Input = nil
			},
			errMsg: "Completion is not set",
		},
		{
			name: "empty completion",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.Input = &workflowservice.StartCallbackExecutionRequest_Completion{Completion: &callbackpb.CallbackExecutionCompletion{}}
			},
			errMsg: "Completion must have either success or failure set",
		},
		{
			name: "missing schedule_to_close_timeout",
			mutate: func(req *workflowservice.StartCallbackExecutionRequest) {
				req.ScheduleToCloseTimeout = nil
			},
			errMsg: "ScheduleToCloseTimeout must be set",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			req := &workflowservice.StartCallbackExecutionRequest{
				Namespace:              s.Namespace().String(),
				Identity:               "test",
				RequestId:              uuid.NewString(),
				CallbackId:             "validation-test-" + uuid.NewString(),
				Callback:               validCallback,
				Input:                  &workflowservice.StartCallbackExecutionRequest_Completion{Completion: validCompletion},
				ScheduleToCloseTimeout: durationpb.New(time.Minute),
			}
			tc.mutate(req)
			_, err := s.FrontendClient().StartCallbackExecution(ctx, req)
			s.Error(err)
			s.Contains(err.Error(), tc.errMsg)
		})
	}
}

// TestStartCallbackExecution_DuplicateID verifies that starting a callback with
// an already-used callback_id returns an AlreadyExists error with the existing run_id.
func (s *StandaloneCallbackSuite) TestStartCallbackExecution_DuplicateID() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	callbackID := "dup-test-" + uuid.NewString()

	// First call succeeds.
	startResp := s.mustStartCallbackExecution(ctx, callbackID, nil, nil, time.Minute)
	existingRunID := startResp.GetRunId()

	// Second call with same callback_id should fail with AlreadyExists.
	_, err := s.startCallbackExecution(ctx, callbackID, nil, nil, time.Minute)
	s.Error(err)
	s.Contains(err.Error(), "already exists")

	// Verify the existing execution is still accessible via the run_id from the first start.
	descResp, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      existingRunID,
	})
	s.NoError(err)
	s.Equal(callbackID, descResp.GetInfo().GetCallbackId())
	s.Equal(existingRunID, descResp.GetInfo().GetRunId())
}

// TestListAndCountCallbackExecutions tests that standalone callback executions
// can be listed and counted via the visibility APIs, and verifies the returned data.
func (s *StandaloneCallbackSuite) TestListAndCountCallbackExecutions() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	// Create two callback executions with known IDs.
	callbackIDs := make([]string, 2)
	for i := range 2 {
		callbackIDs[i] = fmt.Sprintf("list-test-%d-%s", i, uuid.NewString())
		s.mustStartCallbackExecution(ctx, callbackIDs[i], nil, nil, time.Minute)
	}

	// List callback executions — visibility indexing may be async, so use EventuallyWithT.
	// Verify returned data includes our callback IDs and has valid fields.
	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err := s.FrontendClient().ListCallbackExecutions(ctx, &workflowservice.ListCallbackExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.GreaterOrEqual(t, len(listResp.GetExecutions()), 2) {
			return
		}

		// Collect returned callback IDs and verify fields.
		foundIDs := make(map[string]bool)
		for _, exec := range listResp.GetExecutions() {
			foundIDs[exec.GetCallbackId()] = true
			assert.NotEmpty(t, exec.GetCallbackId())
			assert.NotNil(t, exec.GetCreateTime())
		}
		for _, id := range callbackIDs {
			assert.True(t, foundIDs[id], "expected callback %s in list response", id)
		}
	}, 10*time.Second, 200*time.Millisecond)

	// List with ExecutionStatus query filter — newly started callbacks should be "Running".
	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err := s.FrontendClient().ListCallbackExecutions(ctx, &workflowservice.ListCallbackExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     fmt.Sprintf(`ExecutionStatus = "Running" AND CallbackId = %q`, callbackIDs[0]),
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, 1, len(listResp.GetExecutions())) {
			return
		}
		assert.Equal(t, callbackIDs[0], listResp.GetExecutions()[0].GetCallbackId())
	}, 10*time.Second, 200*time.Millisecond)

	// Terminate one callback to test filtering by terminal status.
	_, err := s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackIDs[1],
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Reason:     "testing list filter",
	})
	s.NoError(err)

	// List with ExecutionStatus = "Terminated" should find the terminated callback.
	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err := s.FrontendClient().ListCallbackExecutions(ctx, &workflowservice.ListCallbackExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     fmt.Sprintf(`ExecutionStatus = "Terminated" AND CallbackId = %q`, callbackIDs[1]),
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, 1, len(listResp.GetExecutions())) {
			return
		}
		assert.Equal(t, callbackIDs[1], listResp.GetExecutions()[0].GetCallbackId())
	}, 10*time.Second, 200*time.Millisecond)

	// Count callback executions.
	s.EventuallyWithT(func(t *assert.CollectT) {
		countResp, err := s.FrontendClient().CountCallbackExecutions(ctx, &workflowservice.CountCallbackExecutionsRequest{
			Namespace: s.Namespace().String(),
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.GreaterOrEqual(t, countResp.GetCount(), int64(2))
	}, 10*time.Second, 200*time.Millisecond)

	// Count with ExecutionStatus filter should only count scheduled callbacks.
	s.EventuallyWithT(func(t *assert.CollectT) {
		countResp, err := s.FrontendClient().CountCallbackExecutions(ctx, &workflowservice.CountCallbackExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     `ExecutionStatus = "Running"`,
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.GreaterOrEqual(t, countResp.GetCount(), int64(1))
	}, 10*time.Second, 200*time.Millisecond)
}

// TestStartCallbackExecution_SearchAttributes tests that search attributes provided at start
// are persisted and can be used to query callback executions via list filtering.
func (s *StandaloneCallbackSuite) TestStartCallbackExecution_SearchAttributes() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	callbackID := "sa-test-" + uuid.NewString()
	saValue := "sa-test-value-" + uuid.NewString()

	_, err := s.FrontendClient().StartCallbackExecution(ctx, &workflowservice.StartCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		Identity:   "test",
		RequestId:  uuid.NewString(),
		CallbackId: callbackID,
		Callback: &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost:1/nonexistent",
				},
			},
		},
		Input: &workflowservice.StartCallbackExecutionRequest_Completion{Completion: &callbackpb.CallbackExecutionCompletion{
			Result: &callbackpb.CallbackExecutionCompletion_Success{
				Success: testcore.MustToPayload(s.T(), "some-result"),
			},
		}},
		ScheduleToCloseTimeout: durationpb.New(time.Minute),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": testcore.MustToPayload(s.T(), saValue),
			},
		},
	})
	s.NoError(err)

	// Verify the search attribute is queryable via list.
	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err := s.FrontendClient().ListCallbackExecutions(ctx, &workflowservice.ListCallbackExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     fmt.Sprintf(`CustomKeywordField = %q AND CallbackId = %q`, saValue, callbackID),
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, 1, len(listResp.GetExecutions())) {
			return
		}
		assert.Equal(t, callbackID, listResp.GetExecutions()[0].GetCallbackId())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestTerminateCallbackExecution tests terminate, run_id validation, and request ID idempotency.
func (s *StandaloneCallbackSuite) TestTerminateCallbackExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	callbackID := "terminate-test-" + uuid.NewString()
	startResp := s.mustStartCallbackExecution(ctx, callbackID, nil, nil, time.Minute)
	requestID := uuid.NewString()
	runID := startResp.GetRunId()

	// Wrong run_id should fail for describe, poll, and terminate.
	wrongRunID := uuid.NewString()

	_, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      wrongRunID,
	})
	s.Error(err)

	shortCtx, shortCancel := context.WithTimeout(ctx, time.Second*2)
	defer shortCancel()
	_, err = s.FrontendClient().PollCallbackExecution(shortCtx, &workflowservice.PollCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      wrongRunID,
	})
	s.Error(err)

	_, err = s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      wrongRunID,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Reason:     "wrong run_id",
	})
	s.Error(err)

	// Terminate with correct run_id and known request ID.
	_, err = s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
		Identity:   "test",
		RequestId:  requestID,
		Reason:     "testing terminate",
	})
	s.NoError(err)

	// Describe after terminate — should be TERMINATED with correct run_id.
	descResp, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
	})
	s.NoError(err)
	s.Equal(enumspb.CALLBACK_EXECUTION_STATE_TERMINATED, descResp.GetInfo().GetState())
	s.NotNil(descResp.GetInfo().GetCloseTime())
	s.Equal(runID, descResp.GetInfo().GetRunId())

	// Poll to verify the outcome.
	pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
	})
	s.NoError(err)
	s.NotNil(pollResp.GetOutcome().GetFailure())
	s.Equal("testing terminate", pollResp.GetOutcome().GetFailure().GetMessage())

	// Same request ID should be a no-op (idempotent).
	_, err = s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		Identity:   "test",
		RequestId:  requestID,
		Reason:     "testing terminate",
	})
	s.NoError(err)

	// Different request ID should return FailedPrecondition.
	_, err = s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Reason:     "different request",
	})
	s.Error(err)
	s.Contains(err.Error(), "already terminated with request ID")
}

// TestCallbackExecutionFailedOutcome tests that when a callback fails with a non-retryable error
// (e.g., a 400 response from the target), the poll outcome contains the failure details.
func (s *StandaloneCallbackSuite) TestCallbackExecutionFailedOutcome() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// Start an HTTP server that always returns 400 Bad Request (non-retryable).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	callbackID := "failed-outcome-test-" + uuid.NewString()
	s.mustStartCallbackExecution(ctx, callbackID, &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: srv.URL + "/callback"},
		},
	}, nil, time.Minute)

	// Poll for the outcome — the callback should eventually fail with a non-retryable error.
	pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
	})
	s.NoError(err)
	s.NotNil(pollResp.GetOutcome().GetFailure())
	s.Contains(pollResp.GetOutcome().GetFailure().GetMessage(), "handler error (BAD_REQUEST)")
	s.True(pollResp.GetOutcome().GetFailure().GetApplicationFailureInfo().GetNonRetryable())
}

// TestNexusOperationCompletionBeforeStartHandlerReturns tests that a standalone callback can
// complete a Nexus operation even when the callback execution is started *before* the Nexus
// start handler returns to the caller. This exercises the race where the completion arrives
// while the operation is still in SCHEDULED state (i.e., before transitioning to STARTED).
//
// Flow:
//  1. A caller workflow starts a Nexus operation via an external endpoint.
//  2. The external Nexus handler captures the callback URL/token and calls
//     StartCallbackExecution to deliver the completion *before* returning async.
//  3. The handler then returns an async result.
//  4. The operation can be completed from SCHEDULED state directly, so the workflow
//     receives the result regardless of the start handler timing.
func (s *StandaloneCallbackSuite) TestNexusOperationCompletionBeforeStartHandlerReturns() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	// Set up an external Nexus handler that starts the standalone callback *inside*
	// the start handler — before returning the async result — to simulate a race
	// where the completion is delivered before the caller processes the start response.
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			token := options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			callbackURL := options.CallbackURL

			// Start the standalone callback execution to deliver the completion
			// BEFORE returning from this handler.
			callbackID := "race-callback-" + uuid.NewString()
			_, err := s.startCallbackExecution(ctx, callbackID, &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:   callbackURL,
						Token: token,
					},
				},
			}, &callbackpb.CallbackExecutionCompletion{
				Result: &callbackpb.CallbackExecutionCompletion_Success{
					Success: testcore.MustToPayload(s.T(), "result-before-start-returns"),
				},
			}, time.Minute)
			if err != nil {
				return nil, nexus.NewOperationErrorf(nexus.OperationStateFailed, "StartCallbackExecution failed inside handler: %w", err)
			}

			// Now return the async result — the completion has already been
			// delivered and the operation should already be completed.
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: "test",
			}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	// Register a caller workflow that starts a Nexus operation and waits for its result.
	callerWf := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		fut := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	w := worker.New(s.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWf)
	s.NoError(w.Start())
	defer w.Stop()

	// Start the caller workflow.
	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWf)
	s.NoError(err)

	// The standalone callback delivers the completion even though it was started
	// before the start handler returned. The operation transitions directly from
	// SCHEDULED to SUCCEEDED.
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result-before-start-returns", result)

	// Verify the operation token is recorded in the caller workflow's history.
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
	})
	s.NoError(err)
	startedIdx := slices.IndexFunc(histResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.NotEqual(-1, startedIdx, "expected NexusOperationStarted event in history")
	s.Equal("test", histResp.History.Events[startedIdx].GetNexusOperationStartedEventAttributes().GetOperationToken())
}

// TestScheduleToCloseTimeout verifies that a callback execution transitions to FAILED
// when its schedule-to-close timeout expires before the callback succeeds.
func (s *StandaloneCallbackSuite) TestScheduleToCloseTimeout() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// Short timeout so it fires quickly during the test.
	callbackID := "timeout-test-" + uuid.NewString()
	s.mustStartCallbackExecution(ctx, callbackID, nil, nil, 2*time.Second)

	// Poll until the callback reaches a terminal state due to timeout.
	pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
	})
	s.NoError(err)
	s.NotNil(pollResp.GetOutcome(), "expected terminal outcome after timeout")
	s.NotNil(pollResp.GetOutcome().GetFailure(), "expected failure outcome after timeout")
	s.Contains(pollResp.GetOutcome().GetFailure().GetMessage(), "timed out")
	s.NotNil(pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo())
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())

	// Describe should show FAILED state with timeout failure.
	descResp, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
		Namespace:      s.Namespace().String(),
		CallbackId:     callbackID,
		IncludeOutcome: true,
	})
	s.NoError(err)
	s.Equal(enumspb.CALLBACK_EXECUTION_STATE_FAILED, descResp.GetInfo().GetState())
	s.NotNil(descResp.GetInfo().GetCloseTime())
	s.NotNil(descResp.GetOutcome().GetFailure())
	s.NotNil(descResp.GetOutcome().GetFailure().GetTimeoutFailureInfo())
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, descResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}
