package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCallbacksOTELIntegration(t *testing.T) {
	parallelsuite.Run(t, &CallbacksOTELSuite{})
}

// CallbacksOTELSuite covers OTEL trace context propagation on outbound Nexus completion callbacks.
type CallbacksOTELSuite struct {
	parallelsuite.Suite[*CallbacksOTELSuite]
}

// newEnv builds a test environment with an OTEL-enabled History service.
//
// chasmCallbacks toggles dynamicconfig.EnableCHASMCallbacks (NOT dynamicconfig.EnableChasm,
// which is the broader CHASM-feature switch). Callback routing specifically hinges on
// EnableCHASMCallbacks: false → HSM-backed components/callbacks; true → chasm/lib/callback.
func (s *CallbacksOTELSuite) newEnv(tp trace.TracerProvider, chasmCallbacks bool) *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		// Allow insecure callback URLs.
		testcore.WithDynamicConfig(callbacks.AllowedAddresses, []any{
			map[string]any{"Pattern": "*", "AllowInsecure": true},
		}),

		// Route callbacks through CHASM vs HSM.
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmCallbacks),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmCallbacks),

		// Inject a real TracerProvider into the History service so the otelhttp-wrapped
		// callback transport produces spans with a valid SpanContext, which is what
		// the W3C TraceContext propagator requires to inject the traceparent header.
		// Without this, the default NoopTracerProvider produces invalid contexts and
		// propagation.TraceContext.Inject bails before writing the header.
		testcore.WithServiceFxOptions(primitives.HistoryService,
			fx.Decorate(func(trace.TracerProvider) trace.TracerProvider { return tp }),
		),
	)
}

// runTraceparentTest runs a workflow with a completion callback pointing at a captive
// httptest.Server and asserts the outbound callback carries a W3C traceparent header.
// The shared body is used by both the HSM and CHASM variants — the only differing input
// is the callback-routing dynamic config that newEnv applies.
func (s *CallbacksOTELSuite) runTraceparentTest(chasmCallbacks bool) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	s.T().Cleanup(func() { _ = tp.Shutdown(s.T().Context()) })

	env := s.newEnv(tp, chasmCallbacks)
	ctx := env.Context()

	const (
		workflowID      = "run-workflow-test"
		workflowTimeout = 5 * time.Second
		workflowType    = "otel-test-workflow"
	)
	taskQueue := testcore.RandomizeStr(s.T().Name())

	// Create a faux service listening for callbacks.
	//
	// Buffered so the callback server can return the HTTP response without
	// blocking on the channel being read.
	receivedCallbackRequests := make(chan *http.Request, 4)
	callbackHandlerSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case receivedCallbackRequests <- r:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackHandlerSvr.Close()

	// Workflow. Does nothing, so the callback can fire quickly.
	const wantWfResult = 123456
	completingWorkflow := func(ctx workflow.Context) (int, error) {
		return wantWfResult, nil
	}

	// Construct and start the workflow.
	wfWorker := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	wfWorker.RegisterWorkflowWithOptions(completingWorkflow, workflow.RegisterOptions{Name: workflowType})
	s.NoError(wfWorker.Start())
	defer wfWorker.Stop()

	// Start the workflow and wait for completion.
	// We don't use the SDK's StartWorkflowExecution() so we can supply our own
	// CompletionCallback, which is only settable by the SDK if using higher-level methods.
	startWfReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		Input:               nil,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowTaskTimeout: durationpb.New(workflowTimeout),
		WorkflowRunTimeout:  durationpb.New(workflowTimeout),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{
			// Attach our custom callback to be invoked when the workflow completes.
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: callbackHandlerSvr.URL + "/completion",
					},
				},
			},
		},
	}
	startWfResp, err := env.FrontendClient().StartWorkflowExecution(ctx, startWfReq)
	s.NoError(err)

	// Wait for the Workflow to complete.
	wfRun := env.SdkClient().GetWorkflow(ctx, workflowID, startWfResp.RunId)
	var result int
	err = wfRun.Get(ctx, &result)
	s.NoError(err)
	s.Equal(result, wantWfResult)

	// With the workflow started, we now wait for the workflow to complete successfully,
	// and then confirm that we received traces from the callback handler.

	select {
	case gotReq := <-receivedCallbackRequests:
		tp := gotReq.Header.Get("traceparent")
		s.NotEmptyf(tp, "outbound callback should include W3C traceparent header. Got: %v", gotReq.Header)
	case <-time.After(10 * time.Second):
		s.Fail("callback never fired within timeout")
	}
}

// TestNexusCallbackPropagatesTraceparent_HSM exercises the legacy HSM-backed callback
// path (components/callbacks). End-to-end propagation runs through the History callback
// executor → otelhttp-wrapped transport → outbound HTTP boundary.
func (s *CallbacksOTELSuite) TestNexusCallbackPropagatesTraceparent_HSM() {
	s.runTraceparentTest(false)
}

// TestNexusCallbackPropagatesTraceparent_CHASM exercises the CHASM-backed callback
// path (chasm/lib/callback). Same end-to-end assertion as the HSM variant; gated on
// dynamicconfig.EnableCHASMCallbacks=true.
func (s *CallbacksOTELSuite) TestNexusCallbackPropagatesTraceparent_CHASM() {
	s.runTraceparentTest(true)
}
