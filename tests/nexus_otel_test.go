package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type headerGetter interface {
	Get(string) string
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

	requestHeaders := make(chan headerGetter, 1)
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

	// Wait for the Nexus callback.
	headers := s.requireExportedClientSpan(exporter, requestHeaders)
	s.Equal(callbackHeaderValue, headers.Get("X-Callback-Header"))
}

// Verifies external Nexus operations use the instrumented production HTTP client.
func (s *NexusOTELSuite) TestExternalOperation() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)

	requestHeaders := make(chan headerGetter, 1)
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			requestHeaders <- options.Header
			return &nexus.HandlerStartOperationResultSync[any]{Value: env.Tv().Any().String()}, nil
		},
	})

	_, err := env.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              env.Namespace().String(),
		OperationId:            env.Tv().Any().String(),
		Endpoint:               endpointName,
		Service:                env.Tv().Service(),
		Operation:              env.Tv().Operation(),
		RequestId:              env.Tv().RequestID(),
		ScheduleToCloseTimeout: durationpb.New(time.Minute),
	})
	s.NoError(err)
	s.requireExportedClientSpan(exporter, requestHeaders)
}

// Verifies worker-target Nexus operations trace requests routed through the local frontend client.
func (s *NexusOTELSuite) TestWorkerOperation() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)
	tv := env.Tv().WithTaskQueue(env.WorkerTaskQueue())

	requestHeaders := make(chan headerGetter, 1)
	service := nexus.NewService("test-service")
	operation := nexus.NewSyncOperation("test-operation", func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (string, error) {
		requestHeaders <- options.Header
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
	s.requireExportedClientSpan(exporter, requestHeaders)
}

func (s *NexusOTELSuite) requireExportedClientSpan(
	exporter *tracetest.InMemoryExporter,
	requestHeaders <-chan headerGetter,
) headerGetter {
	var headers headerGetter
	select {
	case headers = <-requestHeaders:
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for Nexus request")
		return nil
	}

	// Extract trace context from headers.
	traceparent := strings.Split(headers.Get("traceparent"), "-")
	s.Len(traceparent, 4)
	traceID, err := oteltrace.TraceIDFromHex(traceparent[1])
	s.NoError(err)
	spanID, err := oteltrace.SpanIDFromHex(traceparent[2])
	s.NoError(err)

	// Verify the trace context.
	s.AwaitTrue(func() bool {
		for _, span := range exporter.GetSpans() {
			if span.SpanKind == oteltrace.SpanKindClient &&
				span.SpanContext.TraceID() == traceID &&
				span.SpanContext.SpanID() == spanID {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
	return headers
}
