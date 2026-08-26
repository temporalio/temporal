package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type nexusHTTPSpan struct {
	TraceID      int
	SpanID       int
	ParentSpanID int
	Name         string
	ServiceName  string
	Kind         oteltrace.SpanKind
	URLPath      string
	Status       codes.Code
	NexusAttrs   map[string]any
}

type NexusOTELSuite struct {
	parallelsuite.Suite[*NexusOTELSuite]
}

func TestNexusOTELSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusOTELSuite{})
}

func (s *NexusOTELSuite) newTestEnv(exporter sdktrace.SpanExporter) *NexusTestEnv {
	return newNexusTestEnv(s.T(), true,
		testcore.WithSpanExporter(exporter),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
	)
}

// Verifies production callback wiring propagates trace context and stored headers end to end.
func (s *NexusOTELSuite) TestCallback() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)

	requestHeaders := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	s.T().Cleanup(server.Close)

	callbackWorker := worker.New(env.SdkClient(), env.Tv().TaskQueue().GetName(), worker.Options{})
	callbackWorker.RegisterWorkflowWithOptions(
		func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: env.Tv().WorkflowType().GetName()},
	)
	s.NoError(callbackWorker.Start())
	s.T().Cleanup(callbackWorker.Stop)

	callbackHeaderValue := env.Tv().Any().String()
	startResponse, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          env.Tv().RequestID(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         env.Tv().WorkflowID(),
		WorkflowType:       env.Tv().WorkflowType(),
		TaskQueue:          env.Tv().TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute),
		Identity:           env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: server.URL,
					Header: map[string]string{
						"X-Callback-Header": callbackHeaderValue,
					},
				},
			},
		}},
	})
	s.NoError(err)
	s.NoError(env.SdkClient().GetWorkflow(s.Context(), env.Tv().WorkflowID(), startResponse.RunId).Get(s.Context(), nil))

	headers := await.Rcv(s.T(), requestHeaders)
	s.Equal(callbackHeaderValue, headers.Get("X-Callback-Header"))
	httpSpans := s.requireNexusHTTPSpans(exporter, []nexusHTTPSpan{{
		TraceID:     1,
		SpanID:      1,
		Name:        "HTTP POST",
		ServiceName: "io.temporal.history",
		Kind:        oteltrace.SpanKindClient,
	}})
	spanContext := oteltrace.SpanContextFromContext(
		propagation.TraceContext{}.Extract(s.Context(), propagation.HeaderCarrier(headers)),
	)
	s.Require().True(spanContext.IsValid())
	s.Require().Equal(spanContext.TraceID(), httpSpans[0].SpanContext.TraceID())
	s.Require().Equal(spanContext.SpanID(), httpSpans[0].SpanContext.SpanID())
}

// Verifies asynchronous start and cancellation connect real History client and Frontend server spans.
func (s *NexusOTELSuite) TestOperation() {
	exporter := tracetest.NewInMemoryExporter()
	callerEnv := s.newTestEnv(exporter)
	handlerEnv := s.newTestEnv(exporter)
	tv := callerEnv.Tv()
	handlerTaskQueue := handlerEnv.Tv().TaskQueue().GetName()

	handlerWorkflow := func(ctx workflow.Context, _ nexus.NoValue) (nexus.NoValue, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return nil, nil
	}
	requestIDs := make(chan string, 1)
	operation := temporalnexus.NewWorkflowRunOperation(
		"test-operation",
		handlerWorkflow,
		func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			select {
			case requestIDs <- options.RequestID:
			default:
			}
			return client.StartWorkflowOptions{
				ID:        options.RequestID,
				TaskQueue: handlerTaskQueue,
			}, nil
		},
	)
	service := nexus.NewService("test-service")
	service.MustRegister(operation)
	handlerWorker := worker.New(handlerEnv.SdkClient(), handlerTaskQueue, worker.Options{})
	handlerWorker.RegisterWorkflow(handlerWorkflow)
	handlerWorker.RegisterNexusService(service)
	s.NoError(handlerWorker.Start())
	s.T().Cleanup(handlerWorker.Stop)

	handlerWorkerEndpoint := handlerEnv.createNexusEndpoint(s.Context(), s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), handlerTaskQueue)
	callerExternalEndpoint := callerEnv.createExternalNexusEndpoint(s.Context(), s.T(), handlerEnv.dispatchByEndpointURL(handlerWorkerEndpoint.Id))
	operationID := tv.Any().String()
	startResponse, err := callerEnv.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              callerEnv.Namespace().String(),
		OperationId:            operationID,
		Endpoint:               callerExternalEndpoint,
		Service:                service.Name,
		Operation:              operation.Name(),
		RequestId:              tv.RequestID(),
		ScheduleToCloseTimeout: durationpb.New(time.Minute),
	})
	s.NoError(err)
	nexusRequestID := await.Rcv(s.T(), requestIDs)

	pollResponse, err := callerEnv.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   callerEnv.Namespace().String(),
		OperationId: operationID,
		RunId:       startResponse.RunId,
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
	})
	s.NoError(err)

	s.Require().Equal(enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED, pollResponse.GetWaitStage())
	_, err = callerEnv.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
		Namespace:   callerEnv.Namespace().String(),
		OperationId: operationID,
		RunId:       startResponse.RunId,
		Reason:      tv.Any().String(),
	})
	s.NoError(err)

	operationURLPath := "/nexus/endpoints/" + handlerWorkerEndpoint.Id + "/services/" + service.Name + "/" + operation.Name()
	s.requireNexusHTTPSpans(exporter, []nexusHTTPSpan{
		{
			TraceID:     1,
			SpanID:      1,
			Name:        "HTTP POST",
			ServiceName: "io.temporal.history",
			Kind:        oteltrace.SpanKindClient,
			URLPath:     operationURLPath,
			NexusAttrs: map[string]any{
				"nexus.request_id": nexusRequestID,
			},
		},
		{
			TraceID:      1,
			SpanID:       2,
			ParentSpanID: 1,
			Name:         "temporal.api.nexusservice.v1.NexusService/DispatchByEndpoint",
			ServiceName:  "io.temporal.frontend",
			Kind:         oteltrace.SpanKindServer,
			URLPath:      operationURLPath,
			NexusAttrs: map[string]any{
				"nexus.endpoint":   handlerWorkerEndpoint.GetSpec().GetName(),
				"nexus.operation":  operation.Name(),
				"nexus.request_id": nexusRequestID,
				"nexus.service":    service.Name,
			},
		},
		{
			TraceID:     2,
			SpanID:      1,
			Name:        "HTTP POST",
			ServiceName: "io.temporal.history",
			Kind:        oteltrace.SpanKindClient,
			URLPath:     operationURLPath + "/cancel",
		},
		{
			TraceID:      2,
			SpanID:       2,
			ParentSpanID: 1,
			Name:         "temporal.api.nexusservice.v1.NexusService/DispatchByEndpoint",
			ServiceName:  "io.temporal.frontend",
			Kind:         oteltrace.SpanKindServer,
			URLPath:      operationURLPath + "/cancel",
			NexusAttrs: map[string]any{
				"nexus.endpoint":  handlerWorkerEndpoint.GetSpec().GetName(),
				"nexus.operation": operation.Name(),
				"nexus.service":   service.Name,
			},
		},
	})
}

// Verifies worker-target Nexus operations connect local Frontend client and server spans.
func (s *NexusOTELSuite) TestWorkerOperation() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)
	tv := env.Tv().WithTaskQueue(env.WorkerTaskQueue())

	type nexusRequest struct {
		header    nexus.Header
		requestID string
	}
	requests := make(chan nexusRequest, 1)
	service := nexus.NewService("test-service")
	operation := nexus.NewSyncOperation("test-operation", func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (string, error) {
		requests <- nexusRequest{header: options.Header, requestID: options.RequestID}
		return tv.Any().String(), nil
	})
	service.MustRegister(operation)

	nexusWorker := worker.New(env.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
	nexusWorker.RegisterNexusService(service)
	s.NoError(nexusWorker.Start())
	s.T().Cleanup(nexusWorker.Stop)

	endpoint := env.createNexusEndpoint(s.Context(), s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), tv.TaskQueue().GetName())
	_, err := env.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              env.Namespace().String(),
		OperationId:            tv.Any().String(),
		Endpoint:               endpoint.GetSpec().GetName(),
		Service:                service.Name,
		Operation:              operation.Name(),
		RequestId:              tv.RequestID(),
		ScheduleToCloseTimeout: durationpb.New(time.Minute),
	})
	s.NoError(err)

	request := await.Rcv(s.T(), requests)
	operationURLPath := "/nexus/endpoints/" + endpoint.Id + "/services/" + service.Name + "/" + operation.Name()
	httpSpans := s.requireNexusHTTPSpans(exporter, []nexusHTTPSpan{
		{
			TraceID:     1,
			SpanID:      1,
			Name:        "HTTP POST",
			ServiceName: "io.temporal.history",
			Kind:        oteltrace.SpanKindClient,
			URLPath:     operationURLPath,
			NexusAttrs: map[string]any{
				"nexus.namespace":  env.Namespace().String(),
				"nexus.request_id": request.requestID,
			},
		},
		{
			TraceID:      1,
			SpanID:       2,
			ParentSpanID: 1,
			Name:         "temporal.api.nexusservice.v1.NexusService/DispatchByEndpoint",
			ServiceName:  "io.temporal.frontend",
			Kind:         oteltrace.SpanKindServer,
			URLPath:      operationURLPath,
			NexusAttrs: map[string]any{
				"nexus.endpoint":   endpoint.GetSpec().GetName(),
				"nexus.operation":  operation.Name(),
				"nexus.request_id": request.requestID,
				"nexus.service":    service.Name,
			},
		},
	})
	spanContext := oteltrace.SpanContextFromContext(
		propagation.TraceContext{}.Extract(s.Context(), propagation.MapCarrier(request.header)),
	)
	s.Require().True(spanContext.IsValid())
	s.Require().Equal(spanContext.TraceID(), httpSpans[0].SpanContext.TraceID())
	s.Require().Equal(spanContext.SpanID(), httpSpans[0].SpanContext.SpanID())
}

// Verifies the namespace and task queue route propagates tracing and records handler failures without forwarding.
func (s *NexusOTELSuite) TestNamespaceAndTaskQueueDispatch() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)
	taskQueue := env.Tv().TaskQueue().GetName()
	pollerErrCh := env.nexusTaskPoller(s.Context(), s.T(), taskQueue, func(
		*testing.T,
		*workflowservice.PollNexusTaskQueueResponse,
	) (*nexusTaskResponse, error) {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "deliberate test failure")
	})
	dispatchURL, err := url.Parse(env.dispatchByTaskQueueURL(taskQueue))
	s.NoError(err)
	nexusClient, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		BaseURL: dispatchURL.String(),
		Service: "test-service",
	})
	s.NoError(err)

	// Inject a fixed traceparent so this test exercises the server independently of client instrumentation.
	// Nexus API tests separately verify that frontend request headers reach workers.
	const (
		traceID      = "4bf92f3577b34da6a3ce929d0e0e4736"
		parentSpanID = "00f067aa0ba902b7"
	)
	requestID := env.Tv().RequestID()
	_, err = nexusrpc.StartOperation(s.Context(), nexusClient, op, env.Tv().Any().String(), nexus.StartOperationOptions{
		Header:    nexus.Header{"traceparent": "00-" + traceID + "-" + parentSpanID + "-01"},
		RequestID: requestID,
	})
	var handlerErr *nexus.HandlerError
	s.Require().ErrorAs(err, &handlerErr)
	s.NoError(await.Rcv(s.T(), pollerErrCh))

	httpSpans := s.requireNexusHTTPSpans(exporter, []nexusHTTPSpan{{
		TraceID:     1,
		SpanID:      1,
		Name:        "temporal.api.nexusservice.v1.NexusService/DispatchByNamespaceAndTaskQueue",
		ServiceName: "io.temporal.frontend",
		Kind:        oteltrace.SpanKindServer,
		URLPath:     dispatchURL.Path + "/test-service/my-operation",
		Status:      codes.Error,
		NexusAttrs: map[string]any{
			"nexus.operation":  "my-operation",
			"nexus.request_id": requestID,
			"nexus.service":    "test-service",
		},
	}})
	s.Require().Equal(traceID, httpSpans[0].SpanContext.TraceID().String())
	s.Require().Equal(parentSpanID, httpSpans[0].Parent.SpanID().String())
}

// requireNexusHTTPSpans compares all exported HTTP spans and their Nexus attributes after
// assigning stable local IDs, then returns the raw spans for context propagation assertions.
func (s *NexusOTELSuite) requireNexusHTTPSpans(
	exporter *tracetest.InMemoryExporter,
	expected []nexusHTTPSpan,
) tracetest.SpanStubs {
	s.T().Helper()
	var httpSpans tracetest.SpanStubs
	requireExportedSpans(s, exporter, expected, func(spans tracetest.SpanStubs) []nexusHTTPSpan {
		var actual []nexusHTTPSpan
		actual, httpSpans = s.nexusHTTPSpans(spans)
		return actual
	})
	return httpSpans
}

func (s *NexusOTELSuite) nexusHTTPSpans(
	spans tracetest.SpanStubs,
) ([]nexusHTTPSpan, tracetest.SpanStubs) {
	httpSpans := testtelemetry.FilterSpans(spans, func(span tracetest.SpanStub) bool {
		return span.InstrumentationScope.Name == otelhttp.ScopeName
	})
	slices.SortFunc(httpSpans, func(a, b tracetest.SpanStub) int {
		return a.StartTime.Compare(b.StartTime)
	})
	localIDs := testtelemetry.LocalSpanIDs(httpSpans)

	result := make([]nexusHTTPSpan, 0, len(httpSpans))
	for i, span := range httpSpans {
		var serviceName string
		if span.Resource != nil {
			if value, ok := span.Resource.Set().Value(semconv.ServiceNameKey); ok {
				serviceName = value.AsString()
			}
		}
		var urlPath string
		var nexusAttrs map[string]any
		for _, attr := range span.Attributes {
			key := string(attr.Key)
			if strings.HasPrefix(key, "nexus.") {
				if nexusAttrs == nil {
					nexusAttrs = make(map[string]any)
				}
				nexusAttrs[key] = attr.Value.AsInterface()
			}
			if attr.Key == semconv.URLPathKey {
				urlPath = attr.Value.AsString()
			}
			if urlPath == "" && attr.Key == semconv.URLFullKey {
				if parsedURL, err := url.Parse(attr.Value.AsString()); err == nil {
					urlPath = parsedURL.Path
				}
			}
		}
		result = append(result, nexusHTTPSpan{
			TraceID:      localIDs[i].Trace,
			SpanID:       localIDs[i].Span,
			ParentSpanID: localIDs[i].Parent,
			Name:         span.Name,
			ServiceName:  serviceName,
			Kind:         span.SpanKind,
			URLPath:      urlPath,
			Status:       span.Status.Code,
			NexusAttrs:   nexusAttrs,
		})
	}
	return result, httpSpans
}

func requireExportedSpans[T any](
	s *NexusOTELSuite,
	exporter *tracetest.InMemoryExporter,
	expected []T,
	project func(tracetest.SpanStubs) []T,
) {
	s.T().Helper()
	s.Await(func(s *NexusOTELSuite) {
		actual := project(exporter.GetSpans())
		s.Require().Len(actual, len(expected))
		s.Require().Equal(expected, actual)
	}, 10*time.Second, 100*time.Millisecond)
}
