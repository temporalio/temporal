package xdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
)

var op = nexus.NewOperationReference[string, string]("my-operation")

type NexusRequestForwardingSuite struct {
	xdcBaseSuite
}

func TestNexusRequestForwardingTestSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &NexusRequestForwardingSuite{}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *NexusRequestForwardingSuite) SetupSuite() {
	re, err := dynamicconfig.ConvertWildcardStringListToRegexp([]string{"internal-test-*"})
	if err != nil {
		panic(err)
	}
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key(): 1000,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                               1 * time.Millisecond,
		dynamicconfig.FrontendNexusRequestHeadersBlacklist.Key():                       dynamicconfig.GetTypedPropertyFn(re),
		callbacks.AllowedAddresses.Key():                                               []any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	}
	s.setupSuite()
}

func (s *NexusRequestForwardingSuite) SetupTest() {
	s.setupTest()
}

func (s *NexusRequestForwardingSuite) TearDownSuite() {
	s.tearDownSuite()
}

// Only tests dispatch by namespace+task_queue.
// TODO: Add test cases for dispatch by endpoint ID once endpoints support replication.
func (s *NexusRequestForwardingSuite) TestStartOperationForwardedFromStandbyToActive() {
	// Custom auth logic used to verify that headers removed when processing the initial request are still forwarded.
	testAuthHeader := "internal-test-auth-header"
	onAuth := func(ctx context.Context, claims *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
		dispatchNexusRequest, ok := target.Request.(*matchingservice.DispatchNexusTaskRequest)
		if ok {
			if _, set := dispatchNexusRequest.Request.Header[testAuthHeader]; !set {
				return authorization.Result{}, errors.New("auth header not set")
			}
			delete(dispatchNexusRequest.Request.Header, testAuthHeader)
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	s.clusters[0].Host().SetOnAuthorize(onAuth)
	s.clusters[1].Host().SetOnAuthorize(onAuth)

	ns := s.createGlobalNamespace()

	testCases := []struct {
		name      string
		taskQueue string
		header    nexus.Header
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, *nexus.ClientStartOperationResult[string], error, map[string][]*metricstest.CapturedRecording, map[string][]*metricstest.CapturedRecording)
	}{
		{
			name:      "success",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				_, ok := res.Request.Header[testAuthHeader]
				s.Falsef(ok, "expected test auth header to be stripped")
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_SyncSuccess{
								SyncSuccess: &nexuspb.StartOperationResponse_Sync{
									Payload: res.Request.GetStartOperation().GetPayload()}}}},
				}, nil
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				require.NoError(t, retErr)
				require.Equal(t, "input", result.Successful)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartNexusOperation", "sync_success")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "request_forwarded")
			},
		},
		{
			name:      "operation error",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				_, ok := res.Request.Header[testAuthHeader]
				s.Falsef(ok, "expected test auth header to be stripped")
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_OperationError{
								OperationError: &nexuspb.UnsuccessfulOperationError{
									OperationState: string(nexus.OperationStateFailed),
									Failure: &nexuspb.Failure{
										Message:  "deliberate test failure",
										Metadata: map[string]string{"k": "v"},
										Details:  []byte(`"details"`),
									}}}}},
				}, nil
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var operationError *nexus.OperationError
				require.ErrorAs(t, retErr, &operationError)
				require.Equal(t, nexus.OperationStateFailed, operationError.State)
				require.Equal(t, "deliberate test failure", operationError.Cause.Error())
				var failureError *nexus.FailureError
				require.ErrorAs(t, operationError.Cause, &failureError)
				require.Equal(t, map[string]string{"k": "v"}, failureError.Failure.Metadata)
				var details string
				err := json.Unmarshal(failureError.Failure.Details, &details)
				require.NoError(t, err)
				require.Equal(t, "details", details)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartNexusOperation", "operation_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "forwarded_request_error")
			},
		},
		{
			name:      "handler error",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				_, ok := res.Request.Header[testAuthHeader]
				s.Falsef(ok, "expected test auth header to be stripped")
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, retErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartNexusOperation", "handler_error:INTERNAL")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "forwarded_request_error")
			},
		},
		{
			name:      "redirect disabled by header",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			header:    nexus.Header{"xdc-redirection": "false", testAuthHeader: "stripped"},
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.FailNow("nexus task handler invoked when redirection should be disabled")
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "redirection not allowed"},
				}
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, retErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
				require.Equal(t, "cluster inactive", handlerErr.Cause.Error())
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "namespace_inactive_forwarding_disabled")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			caller := func(req *http.Request) (*http.Response, error) {
				req.Header.Set(testAuthHeader, "stripped")
				return http.DefaultClient.Do(req)
			}
			dispatchURL := fmt.Sprintf("http://%s/%s", s.clusters[1].Host().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: tc.taskQueue}))
			nexusClient, err := nexus.NewHTTPClient(nexus.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service", HTTPCaller: caller})
			s.NoError(err)

			activeMetricsHandler, ok := s.clusters[0].Host().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			activeCapture := activeMetricsHandler.StartCapture()
			defer activeMetricsHandler.StopCapture(activeCapture)

			passiveMetricsHandler, ok := s.clusters[1].Host().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			passiveCapture := passiveMetricsHandler.StartCapture()
			defer passiveMetricsHandler.StopCapture(passiveCapture)

			ctx, cancel := context.WithCancel(testcore.NewContext())
			defer cancel()

			go s.nexusTaskPoller(ctx, s.clusters[0].FrontendClient(), ns, tc.taskQueue, tc.handler)

			startResult, err := nexus.StartOperation(ctx, nexusClient, op, "input", nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
				Header:      tc.header,
			})
			tc.assertion(t, startResult, err, activeCapture.Snapshot(), passiveCapture.Snapshot())
		})
	}
}

// Only tests dispatch by namespace+task_queue.
// TODO: Add test cases for dispatch by endpoint ID once endpoints support replication.
func (s *NexusRequestForwardingSuite) TestCancelOperationForwardedFromStandbyToActive() {
	// Custom auth logic used to verify that headers removed when processing the initial request are still forwarded.
	testAuthHeader := "internal-test-auth-header"
	onAuth := func(ctx context.Context, claims *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
		dispatchNexusRequest, ok := target.Request.(*matchingservice.DispatchNexusTaskRequest)
		if ok {
			if _, set := dispatchNexusRequest.Request.Header[testAuthHeader]; !set {
				return authorization.Result{}, errors.New("auth header not set")
			}
			delete(dispatchNexusRequest.Request.Header, testAuthHeader)
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	s.clusters[0].Host().SetOnAuthorize(onAuth)
	s.clusters[1].Host().SetOnAuthorize(onAuth)

	ns := s.createGlobalNamespace()

	testCases := []struct {
		name      string
		taskQueue string
		header    nexus.Header
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, error, map[string][]*metricstest.CapturedRecording, map[string][]*metricstest.CapturedRecording)
	}{
		{
			name:      "success",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				_, ok := res.Request.Header[testAuthHeader]
				s.Falsef(ok, "expected test auth header to be stripped")
				return &nexuspb.Response{
					Variant: &nexuspb.Response_CancelOperation{
						CancelOperation: &nexuspb.CancelOperationResponse{},
					},
				}, nil
			},
			assertion: func(t *testing.T, retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				require.NoError(t, retErr)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "CancelNexusOperation", "success")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelNexusOperation", "request_forwarded")
			},
		},
		{
			name:      "handler error",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				_, ok := res.Request.Header[testAuthHeader]
				s.Falsef(ok, "expected test auth header to be stripped")
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, retErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
				requireExpectedMetricsCaptured(t, activeSnap, ns, "CancelNexusOperation", "handler_error:INTERNAL")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelNexusOperation", "forwarded_request_error")
			},
		},
		{
			name:      "redirect disabled by header",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			header:    nexus.Header{"xdc-redirection": "false"},
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.FailNow("nexus task handler invoked when redirection should be disabled")
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "redirection should be disabled"},
				}
			},
			assertion: func(t *testing.T, retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, retErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
				require.Equal(t, "cluster inactive", handlerErr.Cause.Error())
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelNexusOperation", "namespace_inactive_forwarding_disabled")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			caller := func(req *http.Request) (*http.Response, error) {
				req.Header.Set(testAuthHeader, "stripped")
				return http.DefaultClient.Do(req)
			}
			dispatchURL := fmt.Sprintf("http://%s/%s", s.clusters[1].Host().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: tc.taskQueue}))
			nexusClient, err := nexus.NewHTTPClient(nexus.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service", HTTPCaller: caller})
			s.NoError(err)

			activeMetricsHandler, ok := s.clusters[0].Host().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			activeCapture := activeMetricsHandler.StartCapture()
			defer activeMetricsHandler.StopCapture(activeCapture)

			passiveMetricsHandler, ok := s.clusters[1].Host().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			passiveCapture := passiveMetricsHandler.StartCapture()
			defer passiveMetricsHandler.StopCapture(passiveCapture)

			ctx, cancel := context.WithCancel(testcore.NewContext())
			defer cancel()

			go s.nexusTaskPoller(ctx, s.clusters[0].FrontendClient(), ns, tc.taskQueue, tc.handler)

			handle, err := nexusClient.NewHandle("operation", "id")
			require.NoError(t, err)
			err = handle.Cancel(ctx, nexus.CancelOperationOptions{Header: tc.header})
			tc.assertion(t, err, activeCapture.Snapshot(), passiveCapture.Snapshot())
		})
	}
}

func (s *NexusRequestForwardingSuite) TestOperationCompletionForwardedFromStandbyToActive() {
	testCases := []struct {
		name                          string
		getCompletionFn               func() (nexus.OperationCompletion, error)
		assertHistoryAndGetCompleteWF func(*testing.T, []*historypb.HistoryEvent) *workflowservice.RespondWorkflowTaskCompletedRequest
		assertResult                  func(*testing.T, string)
	}{
		{
			name: "success",
			getCompletionFn: func() (nexus.OperationCompletion, error) {
				return nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccessfulOptions{
					Serializer: cnexus.PayloadSerializer,
				})
			},
			assertHistoryAndGetCompleteWF: func(t *testing.T, events []*historypb.HistoryEvent) *workflowservice.RespondWorkflowTaskCompletedRequest {
				completedEventIdx := slices.IndexFunc(events, func(e *historypb.HistoryEvent) bool {
					return e.GetNexusOperationCompletedEventAttributes() != nil
				})
				require.Greater(t, completedEventIdx, 0)
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Identity: "test",
					Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: &commonpb.Payloads{
									Payloads: []*commonpb.Payload{
										events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
									}}}}}},
				}
			},
			assertResult: func(t *testing.T, result string) {
				require.Equal(t, "result", result)
			},
		},
		{
			name: "operation error",
			getCompletionFn: func() (nexus.OperationCompletion, error) {
				f := nexus.Failure{Message: "intentional operation failure"}
				return nexus.NewOperationCompletionUnsuccessful(
					&nexus.OperationError{State: nexus.OperationStateFailed, Cause: &nexus.FailureError{Failure: f}},
					nexus.OperationCompletionUnsuccessfulOptions{})
			},
			assertHistoryAndGetCompleteWF: func(t *testing.T, events []*historypb.HistoryEvent) *workflowservice.RespondWorkflowTaskCompletedRequest {
				failedEventIdx := slices.IndexFunc(events, func(e *historypb.HistoryEvent) bool {
					return e.GetNexusOperationFailedEventAttributes() != nil
				})
				require.Greater(t, failedEventIdx, 0)
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Identity: "test",
					Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: &commonpb.Payloads{
									Payloads: []*commonpb.Payload{
										payload.EncodeString(events[failedEventIdx].GetNexusOperationFailedEventAttributes().GetFailure().Message),
									}}}}}},
				}
			},
			assertResult: func(t *testing.T, result string) {
				require.Equal(t, "intentional operation failure", result)
			},
		},
		{
			name: "canceled",
			getCompletionFn: func() (nexus.OperationCompletion, error) {
				f := nexus.Failure{Message: "operation canceled"}
				return nexus.NewOperationCompletionUnsuccessful(
					&nexus.OperationError{State: nexus.OperationStateCanceled, Cause: &nexus.FailureError{Failure: f}},
					nexus.OperationCompletionUnsuccessfulOptions{})
			},
			assertHistoryAndGetCompleteWF: func(t *testing.T, events []*historypb.HistoryEvent) *workflowservice.RespondWorkflowTaskCompletedRequest {
				canceledEventIdx := slices.IndexFunc(events, func(e *historypb.HistoryEvent) bool {
					return e.GetNexusOperationCanceledEventAttributes() != nil
				})
				require.Greater(t, canceledEventIdx, 0)
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Identity: "test",
					Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: &commonpb.Payloads{
									Payloads: []*commonpb.Payload{
										payload.EncodeString(events[canceledEventIdx].GetNexusOperationCanceledEventAttributes().GetFailure().Message),
									}}}}}},
				}
			},
			assertResult: func(t *testing.T, result string) {
				require.Equal(t, "operation canceled", result)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			// Override templates to always return passive cluster in callback URL
			s.clusters[0].OverrideDynamicConfig(
				s.T(),
				nexusoperations.CallbackURLTemplate,
				"http://"+s.clusters[1].Host().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")
			s.clusters[1].OverrideDynamicConfig(
				s.T(),
				nexusoperations.CallbackURLTemplate,
				"http://"+s.clusters[1].Host().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

			ctx := testcore.NewContext()
			ns := s.createGlobalNamespace()
			taskQueue := fmt.Sprintf("%v-%v", "test-task-queue", uuid.New())
			endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

			var callbackToken, publicCallbackUrl string

			h := nexustest.Handler{
				OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
					callbackToken = options.CallbackHeader.Get(cnexus.CallbackTokenHeader)
					publicCallbackUrl = options.CallbackURL
					return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
				},
			}
			listenAddr := nexustest.AllocListenAddress()
			nexustest.NewNexusServer(s.T(), listenAddr, h)

			createEndpointReq := &operatorservice.CreateNexusEndpointRequest{
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
			}

			_, err := s.clusters[0].OperatorClient().CreateNexusEndpoint(ctx, createEndpointReq)
			s.NoError(err)

			_, err = s.clusters[1].OperatorClient().CreateNexusEndpoint(ctx, createEndpointReq)
			s.NoError(err)

			activeSDKClient, err := client.Dial(client.Options{
				HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
				Namespace: ns,
				Logger:    log.NewSdkLogger(s.logger),
			})
			s.NoError(err)

			run, err := activeSDKClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				TaskQueue: taskQueue,
			}, "workflow")
			s.NoError(err)

			feClient0 := s.clusters[0].FrontendClient()
			feClient1 := s.clusters[1].FrontendClient()

			pollResp, err := feClient0.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: ns,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: "test",
			})
			s.NoError(err)
			_, err = feClient0.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
				Identity:  "test",
				TaskToken: pollResp.TaskToken,
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
						Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
							ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
								Endpoint:  endpointName,
								Service:   "service",
								Operation: "operation",
								Input:     s.mustToPayload("input"),
							},
						},
					},
				},
			})
			s.NoError(err)

			// Poll and verify that the "started" event was recorded.
			pollResp, err = feClient0.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: ns,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: "test",
			})
			s.NoError(err)
			_, err = feClient0.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
				Identity:  "test",
				TaskToken: pollResp.TaskToken,
			})
			s.NoError(err)

			startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
				return e.GetNexusOperationStartedEventAttributes() != nil
			})
			s.Greater(startedEventIdx, 0)

			// Wait for Nexus operation to be replicated
			s.Eventually(func() bool {
				resp, err := feClient1.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: ns,
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: run.GetID(),
						RunId:      run.GetRunID(),
					},
				})
				return err == nil && len(resp.PendingNexusOperations) > 0
			}, 5*time.Second, 500*time.Millisecond)

			completion, err := tc.getCompletionFn()
			s.NoError(err)
			res, snap := s.sendNexusCompletionRequest(ctx, s.T(), s.clusters[1], publicCallbackUrl, completion, callbackToken)
			s.Equal(http.StatusOK, res.StatusCode)
			s.Equal(1, len(snap["nexus_completion_requests"]))
			s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": ns, "outcome": "request_forwarded"})

			// Ensure that CompleteOperation request is tracked as part of normal service telemetry metrics
			s.Condition(func() bool {
				for _, m := range snap["service_requests"] {
					if opTag, ok := m.Tags["operation"]; ok && opTag == "CompleteNexusOperation" {
						return true
					}
				}
				return false
			})

			// Resend the request and verify we get a not found error since the operation has already completed.
			res, snap = s.sendNexusCompletionRequest(ctx, s.T(), s.clusters[0], publicCallbackUrl, completion, callbackToken)
			s.Equal(http.StatusNotFound, res.StatusCode)
			s.Equal(1, len(snap["nexus_completion_requests"]))
			s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": ns, "outcome": "error_not_found"})

			// Poll active cluster and verify the completion is recorded and triggers workflow progress.
			pollResp, err = feClient0.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: ns,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: "test",
			})
			s.NoError(err)
			completeWfReq := tc.assertHistoryAndGetCompleteWF(t, pollResp.History.Events)
			completeWfReq.TaskToken = pollResp.TaskToken
			_, err = feClient0.RespondWorkflowTaskCompleted(ctx, completeWfReq)
			s.NoError(err)
			var result string
			s.NoError(run.Get(ctx, &result))
		})
	}
}

func (s *NexusRequestForwardingSuite) nexusTaskPoller(ctx context.Context, frontendClient workflowservice.WorkflowServiceClient, ns string, taskQueue string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	res, err := frontendClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: ns,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	})
	if ctx.Err() != nil {
		// Test doesn't expect poll to get unblocked.
		return
	}
	s.NoError(err)

	response, handlerErr := handler(res)

	if handlerErr != nil {
		_, err = frontendClient.RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: ns,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Error:     handlerErr,
		})
	} else if response != nil {
		_, err = frontendClient.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: ns,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Response:  response,
		})
	}

	if err != nil && ctx.Err() == nil {
		s.FailNow("received unexpected error responding to Nexus task", err)
	}
}

func (s *NexusRequestForwardingSuite) sendNexusCompletionRequest(
	ctx context.Context,
	t *testing.T,
	testCluster *testcore.TestCluster,
	url string,
	completion nexus.OperationCompletion,
	callbackToken string,
) (*http.Response, map[string][]*metricstest.CapturedRecording) {
	metricsHandler, ok := testCluster.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
	s.True(ok)
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	req, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	require.NoError(t, err)
	if callbackToken != "" {
		req.Header.Add(cnexus.CallbackTokenHeader, callbackToken)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	defer res.Body.Close()
	return res, capture.Snapshot()
}

func requireExpectedMetricsCaptured(t *testing.T, snap map[string][]*metricstest.CapturedRecording, ns string, method string, expectedOutcome string) {
	require.Equal(t, 1, len(snap["nexus_requests"]))
	require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": ns, "method": method, "outcome": expectedOutcome})
	require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
	require.Equal(t, metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)
	require.Equal(t, 1, len(snap["nexus_latency"]))
	require.Subset(t, snap["nexus_latency"][0].Tags, map[string]string{"namespace": ns, "method": method, "outcome": expectedOutcome})
}

func (s *NexusRequestForwardingSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}
