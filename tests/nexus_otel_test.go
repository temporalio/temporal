package tests

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusOTELSuite struct {
	parallelsuite.Suite[*NexusOTELSuite]
}

func TestNexusOTELSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusOTELSuite{})
}

// Verifies production callback wiring propagates trace context and stored headers end to end.
func (s *NexusOTELSuite) TestCallback() {
	t := s.T()
	exporter := tracetest.NewInMemoryExporter()
	env := newNexusTestEnv(t, true,
		testcore.WithSpanExporter(exporter),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
	)
	tv := env.Tv().WithTaskQueue(env.WorkerTaskQueue())

	requestHeaders := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: tv.WorkflowType().GetName()},
	)

	ctx := s.Context()
	callbackHeaderValue := tv.Any().String()
	startResponse, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.RequestID(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute),
		Identity:           tv.Any().String(),
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
	s.NoError(env.SdkClient().GetWorkflow(ctx, tv.WorkflowID(), startResponse.RunId).Get(ctx, nil))

	var headers http.Header
	select {
	case headers = <-requestHeaders:
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for Nexus callback")
	}
	s.Equal(callbackHeaderValue, headers.Get("X-Callback-Header"))

	traceparent := strings.Split(headers.Get("traceparent"), "-")
	s.Len(traceparent, 4)
	traceID, err := oteltrace.TraceIDFromHex(traceparent[1])
	s.NoError(err)
	spanID, err := oteltrace.SpanIDFromHex(traceparent[2])
	s.NoError(err)
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
}
