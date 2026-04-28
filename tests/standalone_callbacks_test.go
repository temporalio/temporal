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
	"github.com/stretchr/testify/require"
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
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	hsmcallbacks "go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Test suite for the Nexus "Standalone Callbacks". Which are Nexus operations corresponding to
// aysynchronous actions that take place outside of Temporal. (e.g. waiting for a payment to
// be processed, or webhook to be delivered, etc.)

// Minimal information that an external service would need to report the results of a callback.
type externalRequestInfo struct {
	Namespace string
	Token     string
	URL       string

	// Result of the callback, success or failure.
	Result *callbackpb.CallbackExecutionCompletion
}

// Result of the fake service after receiving a request. If the `StartCallbackExecution` request
// was accepted, reports the CallbackID, RunID. Otherwise the error.
type externalRequestResult struct {
	CallbackID string

	// Mutually exclusive with Error.
	RunID string
	Error error
}

// fakeExternalService simulates a service doing work asynchronously outside of Temporal.
// Test cases will create a Nexus handler that will start a Nexus operation, and then
// pass the context information to the fake service (via externalRequestInfo). The fake
// service will then notify Temporal the work is done (via StartCallbackExecution), and
// then put metadata into the `requestResults` channel.
type fakeExternalService struct {
	incommingRequests chan<- externalRequestInfo
	requestResults    <-chan externalRequestResult
}

// startFakeExternalService starts a new fake service Goroutine, using the supplied client.
// Will shut down and close its channels when the given context is complete.
func startFakeExternalService(ctx context.Context, client workflowservice.WorkflowServiceClient) *fakeExternalService {
	// Channels are buffered so that tests don't block on reads/writes.
	input := make(chan externalRequestInfo, 4)
	output := make(chan externalRequestResult, 4)

	// Logic of the actual faux return, processing requests. Reads requests to do work (from
	// a Nexus handler), and then reports the results out-of-band from the Nexus operation.
	go func() {
		defer close(input)
		defer close(output)

		for {
			select {
			case incommingRequest := <-input:
				// Uniquely identify the callback execution.
				callbackID := "faux-svc-callback-" + uuid.NewString()

				targetCallback := &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url:   incommingRequest.URL,
							Token: incommingRequest.Token,
						},
					},
				}

				resp, err := client.StartCallbackExecution(ctx, &workflowservice.StartCallbackExecutionRequest{
					Namespace:  incommingRequest.Namespace,
					Identity:   "faux-external-service",
					RequestId:  uuid.NewString(),
					CallbackId: callbackID,
					Callback:   targetCallback,
					Input: &workflowservice.StartCallbackExecutionRequest_Completion{
						Completion: incommingRequest.Result,
					},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				})

				// Make the result available to the testcase.
				output <- externalRequestResult{
					CallbackID: callbackID,
					RunID:      resp.GetRunId(),
					Error:      err,
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return &fakeExternalService{
		incommingRequests: input,
		requestResults:    output,
	}
}

func TestStandaloneCallbackSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(StandaloneCallbackSuite))
}

type StandaloneCallbackSuite struct {
	testcore.FunctionalTestBase
}

// Expose a helper to enable "require" assertions, to fail the test immediately instead of
// allowing cascading failures. e.g. s.Require().NotNil(err, "unable to do critical thing")
func (s *StandaloneCallbackSuite) Require() *require.Assertions {
	return require.New(s.T())
}

func (s *StandaloneCallbackSuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():           true,
			dynamicconfig.EnableCHASMCallbacks.Key():  true,
			callback.EnableStandaloneExecutions.Key(): true,

			// BUG: The frontend only honors the hsm callback rules, and will
			// ignore the callback.AllowedAddresses configruation. (See frontend/fx.go)
			callback.AllowedAddresses.Key(): []any{
				map[string]any{"Pattern": "*", "AllowInsecure": true},
			},
			// As a workaround, we specify the HSM rules, as those are the ones
			// that be honored.
			hsmcallbacks.AllowedAddresses.Key(): []any{
				map[string]any{"Pattern": "*", "AllowInsecure": true},
			},
		}),
	)
}

// Calls StartCallbackExecution, reporting the result of a callback that doesn't exist.
//
// This isn't expected to return an error, since the API is written to support the external
// operation having completed before the Temporal/Nexus side registration has completed.
//
// However, since the Nexus callback isn't even registered, the callback execution will
// aways result in timing out.
func (s *StandaloneCallbackSuite) callStartCallbackExecutionToBogusCallback(
	ctx context.Context,
	callbackID string,
	timeout time.Duration,
) *workflowservice.StartCallbackExecutionResponse {
	s.T().Helper()
	callback := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "http://localhost:1/nonexistent",
			},
		},
	}

	completion := &callbackpb.CallbackExecutionCompletion{
		Result: &callbackpb.CallbackExecutionCompletion_Success{
			Success: testcore.MustToPayload(s.T(), "some-result"),
		},
	}

	return s.callStartCallbackExecution(ctx, callbackID, callback, completion, timeout)
}

// Call the StartCallbackExecution API with the given parameters.
func (s *StandaloneCallbackSuite) callStartCallbackExecution(
	ctx context.Context,
	callbackID string,
	callback *commonpb.Callback,
	completion *callbackpb.CallbackExecutionCompletion,
	timeout time.Duration,
) *workflowservice.StartCallbackExecutionResponse {
	s.T().Helper()

	resp, err := s.FrontendClient().StartCallbackExecution(ctx, &workflowservice.StartCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		Identity:   "startCallbackExecution",
		RequestId:  uuid.NewString(),
		CallbackId: callbackID,
		Callback:   callback,
		Input: &workflowservice.StartCallbackExecutionRequest_Completion{
			Completion: completion,
		},
		ScheduleToCloseTimeout: durationpb.New(timeout),
	})
	s.Require().NoError(err, "Error calling StartCallbackExecution to bogus callback")

	return resp
}

// TestBasicaOperation tests that a Nexus operation started by a workflow can be completed using the
// StartCallbackExecution API to deliver the Nexus completion to the operation's callback URL.
//
// Flow:
//  1. A caller workflow (`callerWf`) starts a Nexus operation via an external endpoint.
//  2. The Nexus handler (`nexusHandler`) starts the operation asynchronously and captures
//     the callback URL and token.
//  3. The Nexus handler passes the data to a fake external service (`fakeSvc`), which then
//     StartCallbackExecution API with a successful payload.
//  4. The CHASM callback execution delivers the Nexus completion to the callback URL.
//  5. The caller workflow receives the operation's result and completes.
//
// The test is ran in two variants. First, where the out-of-band service reports a successful
// compoetion result. The second reports a failure. Causing the calling workflow to fail.
func (s *StandaloneCallbackSuite) TestBasicOperation() {

	// Implementation of the test scenario, standing up the workflow, Nexus operation,
	// external service, etc.
	//
	// Takes a CallbackExecutionCompletion to be reported by the external service, and a
	// verification function to test the calling Workflow behaved as expected.
	runStandaloneCallbackScenario := func(
		ctx context.Context,
		completionResult *callbackpb.CallbackExecutionCompletion,
		workflowRunVerificationFn func(client.WorkflowRun),
	) {
		taskQueue := testcore.RandomizeStr(s.T().Name())

		// Fake External Service
		//
		// Start a fake external service to handle async requests, and report
		// their results to Temporal.
		fakeSvc := startFakeExternalService(ctx, s.FrontendClient())

		// Nexus Handler
		//
		// Set up an external Nexus handler that starts operations asynchronously
		// and sends the callback URL and token to the fake service. The Nexus
		// handler terminates, while the fake service reports results out-of-band.
		nexusEndpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
		const (
			nexusSvcName = "nexus-service"
			nexusSvcOp   = "nexus-operation"
		)

		nexusHandler := nexustest.Handler{
			OnStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				s.Equal(nexusSvcName, service)
				s.Equal(nexusSvcOp, operation)

				// Send the request to the external service to do the work.
				fakeSvc.incommingRequests <- externalRequestInfo{
					Namespace: s.Namespace().String(),
					Token:     options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
					URL:       options.CallbackURL,

					Result: completionResult,
				}

				// End the Nexus operation.
				return &nexus.HandlerStartOperationResultAsync{
					OperationToken: fmt.Sprintf("operation-token-%s", uuid.NewString()),
				}, nil
			},
		}

		// Start the Nexus server.
		listenAddr := nexustest.AllocListenAddress()
		nexustest.NewNexusServer(s.T(), listenAddr, nexusHandler)

		// Register the Nexus endpoint with the Temporal service.
		createNexusEndpointReq := &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: nexusEndpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_External_{
						External: &nexuspb.EndpointTarget_External{
							Url: "http://" + listenAddr,
						},
					},
				},
			},
		}
		_, err := s.OperatorClient().CreateNexusEndpoint(ctx, createNexusEndpointReq)
		s.Require().NoError(err, "Error registering Nexus endpoint")

		// Calling Workflow
		//
		// This will create the Nexus operation, invoking the Handler workflow. The
		// workflow will then block until the Nexus operation completes, which will
		// not be until the fake external service has reported the callback's result.
		callerWf := func(ctx workflow.Context) (string, error) {
			c := workflow.NewNexusClient(nexusEndpointName, nexusSvcName)
			fut := c.ExecuteOperation(ctx, nexusSvcOp, "input", workflow.NexusOperationOptions{})

			var nexusOpResult string
			err := fut.Get(ctx, &nexusOpResult)
			return nexusOpResult, err
		}

		// Run the Test
		//
		// Construct and start the calling workflow Worker. Then wait for completion.
		callerWfWorker := worker.New(s.SdkClient(), taskQueue, worker.Options{})
		callerWfWorker.RegisterWorkflow(callerWf)
		s.Require().NoError(callerWfWorker.Start(), "Error starting calling workflow Worker")
		defer callerWfWorker.Stop()

		// Start
		startOpts := client.StartWorkflowOptions{
			TaskQueue: taskQueue,
		}
		callerWfRun, err := s.SdkClient().ExecuteWorkflow(ctx, startOpts, callerWf)
		s.Require().NoError(err, "Error running the caller Workflow")

		// Defer to a caller-supplied verification function to wait on the caller
		// workflow's result and verify the success/failure as applicable.
		workflowRunVerificationFn(callerWfRun)

		// If the fake service was called correctly, we expect to see the result
		// of it doing the out-of-band work.
		fakeSvcResult := <-fakeSvc.requestResults
		s.Require().NoError(fakeSvcResult.Error)

		// Additional Verification
		//
		// Use the Describe and Poll APIs to fetch the callback execution.
		descResp, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
			Namespace:      s.Namespace().String(),
			CallbackId:     fakeSvcResult.CallbackID,
			IncludeOutcome: false,
		})
		s.Require().NoError(err, "Error describing the callback execution")
		s.Nil(descResp.Outcome)

		gotInfo := descResp.GetInfo()
		s.Require().NotNil(gotInfo, "Got nil Info in response")
		s.Equal(fakeSvcResult.CallbackID, gotInfo.GetCallbackId())
		s.NotNil(gotInfo.GetCreateTime())

		// QUIRK: If looking at the "execution info", even a callback result that was a failure
		// will still have STATUS_SUCCEEDED. (Since it was successfully delivered.)
		//
		// You need to look at the outcome (CallbackExecutionOutcome) to know whether or not
		// the operation was a success.
		s.Equal(enumspb.CALLBACK_EXECUTION_STATUS_SUCCEEDED, gotInfo.GetStatus())
		s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, gotInfo.GetState())

		// Poll to verify the outcome as well.
		pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: fakeSvcResult.CallbackID,
		})
		s.Require().NoError(err, "Error polling completed callback execution")

		outcome := pollResp.GetOutcome()
		s.Require().NotNil(outcome, "Got nil Outcome")

		// Verify the CallbackExecution's result reported by the external service
		// matches the CallbackExecution's state.
		if completionResult.GetSuccess() != nil {
			s.NotNil(outcome.GetSuccess())
			s.Nil(outcome.GetFailure())
		} else {
			s.Nil(outcome.GetSuccess())
			s.NotNil(outcome.GetFailure())
		}
	}

	s.Run("success", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		wantPayloadStr := "successfully delivered payload via external svc"
		successCompletion := &callbackpb.CallbackExecutionCompletion{
			Result: &callbackpb.CallbackExecutionCompletion_Success{
				Success: testcore.MustToPayload(s.T(), wantPayloadStr),
			},
		}

		verifyWorkflowRunFn := func(workflowRun client.WorkflowRun) {
			var gotPayload string
			s.NoError(workflowRun.Get(ctx, &gotPayload))
			s.Equal(wantPayloadStr, gotPayload)
		}

		runStandaloneCallbackScenario(ctx, successCompletion, verifyWorkflowRunFn)
	})

	s.Run("failure", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		failureCompletion := &callbackpb.CallbackExecutionCompletion{
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
		}

		verifyWorkflowRunFn := func(workflowRun client.WorkflowRun) {
			// The workflow should fail with a NexusOperationError wrapping the failure.
			var unusedResult string
			err := workflowRun.Get(ctx, &unusedResult)

			// Confirm the (super-long) error contains the key information.
			s.ErrorContains(err, "workflow execution error")
			s.ErrorContains(err, workflowRun.GetRunID())
			s.ErrorContains(err, "nexus operation completed unsuccessfully")
			s.ErrorContains(err, "operation failed from standalone callback")

			// Confirm the error's type is correct as well.
			var wee *temporal.WorkflowExecutionError
			s.ErrorAs(err, &wee)

			var noe *temporal.NexusOperationError
			s.ErrorAs(wee, &noe)
			s.Contains(noe.Error(), "operation failed from standalone callback")
		}

		runStandaloneCallbackScenario(ctx, failureCompletion, verifyWorkflowRunFn)
	})
}

// TestPollCallbackExecution tests that PollCallbackExecution long-polls for the outcome
// of a callback execution. It returns an empty response when the poll times out, and
// the CallbackExecutionOutcome when the callback reaches a terminal state.
func (s *StandaloneCallbackSuite) TestPollCallbackExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.Run("returns_empty_for_non_terminal", func() {
		callbackID := "poll-test-" + uuid.NewString()

		// Report the result of a non-existent, non-routable callback. The CallbackExecution
		// will linger for 1m before timing out.
		s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, time.Minute)

		// Poll a non-terminal callback with a short timeout. Should return an empty response.
		// NOTE: Passing a shorter timeout like 1s will cause failure with "Workflow is busy."
		shortCtx, shortCancel := context.WithTimeout(ctx, 2*time.Second)
		defer shortCancel()
		pollResp, err := s.FrontendClient().PollCallbackExecution(shortCtx, &workflowservice.PollCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
		})
		s.NoError(err)
		s.Nil(pollResp.GetOutcome())

		// Terminate the CallbackExecution resource, then poll should return the outcome.
		_, err = s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
			Identity:   s.T().Name(),
			RequestId:  uuid.NewString(),
			Reason:     "testing poll behavior",
		})
		s.Require().NoError(err, "Unable to terminate CallbackExecution")

		pollResp, err = s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
		})
		s.NoError(err)
		s.NotNil(pollResp.GetOutcome().GetFailure())
		s.Equal("testing poll behavior", pollResp.GetOutcome().GetFailure().GetMessage())
	})

	s.Run("blocks_until_complete", func() {
		callbackID := "poll-blocks-" + uuid.NewString()
		s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, time.Minute)

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

		// Verify the poll is still blocking and hasn't returned yet.
		select {
		case <-resultCh:
			s.Fail("expected poll to block, but it returned before terminate")
		case <-time.After(500 * time.Millisecond):
		}

		// Terminate the CallbackExecution. Confirm that the poll result (from Goroutine) has completed.
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

	s.Run("returns_run_id", func() {
		callbackID := "poll-runid-" + uuid.NewString()
		startResp := s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, time.Minute)

		gotRunID := startResp.GetRunId()

		// Terminate so poll returns immediately.
		_, err := s.FrontendClient().TerminateCallbackExecution(ctx, &workflowservice.TerminateCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
			Identity:   "test",
			RequestId:  uuid.NewString(),
			Reason:     "testing run_id",
		})
		s.NoError(err)

		// Confirm the Poll result includes the RunID of the initial StartCallbackExecution request.
		pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
		})
		s.NoError(err)
		s.Equal(gotRunID, pollResp.GetRunId())
		s.NotNil(pollResp.GetOutcome().GetFailure())
	})

	s.Run("poll_after_timeout", func() {
		callbackID := "poll-timeout-" + uuid.NewString()
		// Start with a very short schedule-to-close timeout so it times out quickly.
		s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, 500*time.Millisecond)

		// Wait for the callback to time out, then poll for the outcome.
		const (
			waitUpTo      = 3 * time.Second
			checkInterval = 200 * time.Millisecond
		)
		s.EventuallyWithT(func(t *assert.CollectT) {
			pollResp, err := s.FrontendClient().PollCallbackExecution(ctx, &workflowservice.PollCallbackExecutionRequest{
				Namespace:  s.Namespace().String(),
				CallbackId: callbackID,
			})
			if !assert.NoError(t, err) {
				return
			}
			if !assert.NotNil(t, pollResp.GetOutcome()) {
				return
			}
			assert.NotNil(t, pollResp.GetOutcome().GetFailure())
			assert.NotNil(t, pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo())
			assert.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
		}, waitUpTo, checkInterval)
	})
}

// TestDeleteCallbackExecution verifies that a standalone callback execution can be deleted.
// Delete terminates the callback if it's still running, then marks it for cleanup.
func (s *StandaloneCallbackSuite) TestDeleteCallbackExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a callback that points to a non-existent URL so it won't complete on its own.
	// The callback will be in SCHEDULED/BACKING_OFF state when we delete it.
	callbackID := "delete-test-" + uuid.NewString()
	startResp := s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, time.Minute)
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
	s.Equal(enumspb.CALLBACK_EXECUTION_STATUS_RUNNING, descResp.GetInfo().GetStatus())

	// Delete with wrong run_id should fail.
	_, err = s.FrontendClient().DeleteCallbackExecution(ctx, &workflowservice.DeleteCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      uuid.NewString(),
	})
	s.ErrorContains(err, fmt.Sprintf("callback_execution not found for ID: %s", callbackID))

	// Delete the callback execution using correct run_id.
	_, err = s.FrontendClient().DeleteCallbackExecution(ctx, &workflowservice.DeleteCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		CallbackId: callbackID,
		RunId:      runID,
	})
	s.NoError(err)

	// Describe after delete — the callback should eventually be not found.
	const (
		waitUpTo      = 3 * time.Second
		checkInterval = 100 * time.Millisecond
	)
	s.EventuallyWithT(func(t *assert.CollectT) {
		_, err := s.FrontendClient().DescribeCallbackExecution(ctx, &workflowservice.DescribeCallbackExecutionRequest{
			Namespace:  s.Namespace().String(),
			CallbackId: callbackID,
			RunId:      runID,
		})
		assert.ErrorContains(t, err, "not found")
	}, waitUpTo, checkInterval)
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
// an already-used callback_id returns an AlreadyExists error with a different request_id,
// and that the same request_id is idempotent (returns the existing run_id without error).
func (s *StandaloneCallbackSuite) TestStartCallbackExecution_DuplicateID() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	callbackID := "dup-test-" + uuid.NewString()
	requestID := uuid.NewString()

	// Build the request explicitly so we can reuse the same request_id.
	req := &workflowservice.StartCallbackExecutionRequest{
		Namespace:  s.Namespace().String(),
		Identity:   "test",
		RequestId:  requestID,
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
	}

	// First call succeeds.
	startResp, err := s.FrontendClient().StartCallbackExecution(ctx, req)
	s.NoError(err)
	existingRunID := startResp.GetRunId()
	s.NotEmpty(existingRunID)

	// Same callback_id + same request_id should be idempotent (return existing run_id).
	dupResp, err := s.FrontendClient().StartCallbackExecution(ctx, req)
	s.NoError(err)
	s.Equal(existingRunID, dupResp.GetRunId())

	// Same callback_id + different request_id should fail with AlreadyExists.
	req.RequestId = uuid.NewString()
	_, err = s.FrontendClient().StartCallbackExecution(ctx, req)
	s.Error(err)
	s.Contains(err.Error(), "already exists")
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
		s.callStartCallbackExecutionToBogusCallback(ctx, callbackIDs[i], time.Minute)
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
	startResp := s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, time.Minute)
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
	s.Equal(enumspb.CALLBACK_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	s.Equal(enumspb.CALLBACK_STATE_TERMINATED, descResp.GetInfo().GetState())
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
	callback := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: srv.URL + "/callback"},
		},
	}
	completion := &callbackpb.CallbackExecutionCompletion{
		Result: &callbackpb.CallbackExecutionCompletion_Success{
			Success: testcore.MustToPayload(s.T(), "some-result"),
		},
	}
	s.callStartCallbackExecution(ctx, callbackID, callback, completion, time.Minute)

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
			callback := &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:   callbackURL,
						Token: token,
					},
				},
			}
			completion := &callbackpb.CallbackExecutionCompletion{
				Result: &callbackpb.CallbackExecutionCompletion_Success{
					Success: testcore.MustToPayload(s.T(), "result-before-start-returns"),
				},
			}
			s.callStartCallbackExecution(ctx, callbackID, callback, completion, time.Minute)

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
	s.callStartCallbackExecutionToBogusCallback(ctx, callbackID, 2*time.Second)

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
	s.Equal(enumspb.CALLBACK_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
	s.Equal(enumspb.CALLBACK_STATE_FAILED, descResp.GetInfo().GetState())
	s.NotNil(descResp.GetInfo().GetCloseTime())
	s.NotNil(descResp.GetOutcome().GetFailure())
	s.NotNil(descResp.GetOutcome().GetFailure().GetTimeoutFailureInfo())
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, descResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}
