package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type nexusHTTPSpan struct {
	TraceID     int
	SpanID      int
	Name        string
	ServiceName string
	Kind        oteltrace.SpanKind
	URLPath     string
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

	var headers http.Header
	select {
	case headers = <-requestHeaders:
	case <-s.Context().Done():
		s.FailNow("timed out waiting for Nexus callback", s.Context().Err().Error())
	}
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

// Verifies asynchronous start and cancellation use the instrumented external Nexus HTTP client.
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
	operation := temporalnexus.NewWorkflowRunOperation(
		"test-operation",
		handlerWorkflow,
		func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
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
		},
		{
			TraceID:     2,
			SpanID:      1,
			Name:        "HTTP POST",
			ServiceName: "io.temporal.history",
			Kind:        oteltrace.SpanKindClient,
			URLPath:     operationURLPath + "/cancel",
		},
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
	s.requireExportedNexusHTTPSpanPairs(callerExporter, handlerExporter, 2)
}

// Verifies the namespace and task queue dispatch route is instrumented independently of forwarding.
func (s *NexusOTELSuite) TestNamespaceAndTaskQueueDispatch() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)

	requestHeaders := make(chan nexus.Header, 1)
	service := nexus.NewService("test-service")
	operation := nexus.NewSyncOperation("test-operation", func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (string, error) {
		requestHeaders <- options.Header
		return tv.Any().String(), nil
	})
	s.NoError(err)

	requestHeaders := nexus.Header{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	_, err = nexusrpc.StartOperation(s.Context(), nexusClient, op, env.Tv().Any().String(), nexus.StartOperationOptions{
		Header: requestHeaders,
	})
	s.NoError(err)
	s.NoError(<-pollerErrCh)
	s.requireExportedServerSpan(
		exporter,
		requestHeaders,
		"temporal.api.nexusservice.v1.NexusService/DispatchByNamespaceAndTaskQueue",
		"io.temporal.frontend",
	)
}

func (s *NexusOTELSuite) requireExportedNexusHTTPSpanPairs(
	callerExporter *tracetest.InMemoryExporter,
	handlerExporter *tracetest.InMemoryExporter,
	expected int,
) {
	s.Await(func(s *NexusOTELSuite) {
		pairs := 0
		for _, serverSpan := range handlerExporter.GetSpans() {
			if serverSpan.Name != "temporal.api.nexusservice.v1.NexusService/DispatchByEndpoint" ||
				serverSpan.SpanKind != oteltrace.SpanKindServer ||
				spanServiceName(serverSpan) != "io.temporal.frontend" {
				continue
			}
			for _, clientSpan := range callerExporter.GetSpans() {
				if clientSpan.SpanKind == oteltrace.SpanKindClient &&
					spanServiceName(clientSpan) == "io.temporal.history" &&
					clientSpan.SpanContext.TraceID() == serverSpan.SpanContext.TraceID() &&
					clientSpan.SpanContext.SpanID() == serverSpan.Parent.SpanID() {
					pairs++
				}
			}
		}
		_, err = nexusrpc.StartOperation(s.Context(), client, op, tv.Any().String(), nexus.StartOperationOptions{
			Header: requestHeaders,
		})
		s.NoError(err)
		s.NoError(<-pollerErrCh)
		s.requireExportedServerSpan(exporter, requestHeaders, "DispatchNexusTaskByNamespaceAndTaskQueue", "io.temporal.frontend")
	})
	s.NoError(err)

	var headers nexus.Header
	select {
	case headers = <-requestHeaders:
	case <-s.Context().Done():
		s.FailNow("timed out waiting for Nexus operation", s.Context().Err().Error())
	}
	operationURLPath := "/nexus/endpoints/" + endpoint.Id + "/services/" + service.Name + "/" + operation.Name()
	httpSpans := s.requireNexusHTTPSpans(exporter, []nexusHTTPSpan{{
		TraceID:     1,
		SpanID:      1,
		Name:        "HTTP POST",
		ServiceName: "io.temporal.history",
		Kind:        oteltrace.SpanKindClient,
		URLPath:     operationURLPath,
	}})
	spanContext := oteltrace.SpanContextFromContext(
		propagation.TraceContext{}.Extract(s.Context(), propagation.MapCarrier(headers)),
	)
	s.Require().True(spanContext.IsValid())
	s.Require().Equal(spanContext.TraceID(), httpSpans[0].SpanContext.TraceID())
	s.Require().Equal(spanContext.SpanID(), httpSpans[0].SpanContext.SpanID())
}

// requireNexusHTTPSpans compares all exported HTTP spans after assigning stable local IDs
// and returns the matching raw spans for context propagation assertions.
func (s *NexusOTELSuite) requireNexusHTTPSpans(
	exporter *tracetest.InMemoryExporter,
	expected []nexusHTTPSpan,
) tracetest.SpanStubs {
	s.T().Helper()
	var httpSpans tracetest.SpanStubs
	s.Await(func(s *NexusOTELSuite) {
		var actual []nexusHTTPSpan
		actual, httpSpans = s.nexusHTTPSpans(exporter.GetSpans())
		s.Require().Equal(expected, actual)
	}, 10*time.Second, 100*time.Millisecond)
	return httpSpans
}

func (s *NexusOTELSuite) nexusHTTPSpans(
	spans tracetest.SpanStubs,
) ([]nexusHTTPSpan, tracetest.SpanStubs) {
	httpSpans := slices.DeleteFunc(spans, func(span tracetest.SpanStub) bool {
		return span.InstrumentationScope.Name != otelhttp.ScopeName
	})
	slices.SortFunc(httpSpans, func(a, b tracetest.SpanStub) int {
		return a.StartTime.Compare(b.StartTime)
	})

	traceIDs := make(map[oteltrace.TraceID]int)
	spanIDs := make(map[oteltrace.TraceID]map[oteltrace.SpanID]int)
	for _, span := range httpSpans {
		traceID := span.SpanContext.TraceID()
		if _, ok := traceIDs[traceID]; !ok {
			traceIDs[traceID] = len(traceIDs) + 1
			spanIDs[traceID] = make(map[oteltrace.SpanID]int)
		}
		spanIDs[traceID][span.SpanContext.SpanID()] = len(spanIDs[traceID]) + 1
	}

	result := make([]nexusHTTPSpan, 0, len(httpSpans))
	for _, span := range httpSpans {
		traceID := span.SpanContext.TraceID()
		var serviceName string
		if span.Resource != nil {
			if value, ok := span.Resource.Set().Value(semconv.ServiceNameKey); ok {
				serviceName = value.AsString()
			}
		}
		var urlPath string
		for _, attr := range span.Attributes {
			if attr.Key == semconv.URLPathKey {
				urlPath = attr.Value.AsString()
				break
			}
			if attr.Key == semconv.URLFullKey {
				if parsedURL, err := url.Parse(attr.Value.AsString()); err == nil {
					urlPath = parsedURL.Path
				}
			}
		}
		result = append(result, nexusHTTPSpan{
			TraceID:     traceIDs[traceID],
			SpanID:      spanIDs[traceID][span.SpanContext.SpanID()],
			Name:        span.Name,
			ServiceName: serviceName,
			Kind:        span.SpanKind,
			URLPath:     urlPath,
		})
	}
	return result, httpSpans
}

func (s *NexusOTELSuite) requireExportedServerSpan(
	exporter *tracetest.InMemoryExporter,
	headers headerGetter,
	operation string,
	serviceName string,
) {
	traceID, clientSpanID := s.requireTraceContext(headers)
	var exportedSpan tracetest.SpanStub
	s.Await(func(s *NexusOTELSuite) {
		spans := exporter.GetSpans()
		for _, span := range spans {
			if span.Name == operation &&
				span.SpanKind == oteltrace.SpanKindServer &&
				span.SpanContext.TraceID() == traceID &&
				span.Parent.SpanID() == clientSpanID {
				exportedSpan = span
				return
			}
		}
		s.Require().Fail("matching server span not found", "exported spans: %v", spans)
	}, 10*time.Second, 100*time.Millisecond)
	s.requireSpanServiceName(exportedSpan, serviceName)
}

func (s *NexusOTELSuite) requireSpanServiceName(span tracetest.SpanStub, expected string) {
	s.Require().NotNil(span.Resource)
	serviceName, ok := span.Resource.Set().Value(semconv.ServiceNameKey)
	s.Require().True(ok)
	s.Require().Equal(expected, serviceName.AsString())
}

func spanServiceName(span tracetest.SpanStub) string {
	if span.Resource == nil {
		return ""
	}
	serviceName, ok := span.Resource.Set().Value(semconv.ServiceNameKey)
	if !ok {
		return ""
	}
	return serviceName.AsString()
}

func (s *NexusOTELSuite) requireTraceContext(headers headerGetter) (oteltrace.TraceID, oteltrace.SpanID) {
	// Extract trace context from headers.
	traceparent := strings.Split(headers.Get("traceparent"), "-")
	s.Len(traceparent, 4)
	traceID, err := oteltrace.TraceIDFromHex(traceparent[1])
	s.NoError(err)
	spanID, err := oteltrace.SpanIDFromHex(traceparent[2])
	s.NoError(err)
	return traceID, spanID
}
