package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCallbacksOTELIntegration(t *testing.T) {
	parallelsuite.Run(t, &CallbacksOTELSuite{})
}

// CallbacksOTELSuite covers OTEL trace context propagation on outbound Nexus completion callbacks.
type CallbacksOTELSuite struct {
	parallelsuite.Suite[*CallbacksOTELSuite]
}

func (s *CallbacksOTELSuite) newEnv() *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		// Allow insecure callback URLs.
		testcore.WithDynamicConfig(callbacks.AllowedAddresses, []any{
			map[string]any{"Pattern": "*", "AllowInsecure": true},
		}),
	)
}

// TestNexusCallback_PropagatesTraceparent runs a workflow that completes immediately with
// a Nexus completion callback attached. The callback target is a raw httptest.Server that
// captures the inbound traceparent header. The test asserts the header is present and
// non-empty, demonstrating end-to-end OTEL propagation through the History callback
// executor → otelhttp-wrapped transport → outbound HTTP boundary.
func (s *CallbacksOTELSuite) TestNexusCallbackPropagatesTraceparent() {
	env := s.newEnv()
	ctx := env.Context()

	const (
		workflowID      = "run-workflow-test"
		workflowTimeout = 20 * time.Second
		workflowType    = "otel-test-workflow"
		taskQueue       = "task-queue"
	)

	// Create a faux service listening for callbacks.
	receivedTraceparent := make(chan string, 4)
	callbackHandlerSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case receivedTraceparent <- r.Header.Get("traceparent"):
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
	wfWorker.RegisterWorkflow(completingWorkflow)
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
	case tp := <-receivedTraceparent:
		s.NotEmpty(tp, "outbound callback should include W3C traceparent header")
	case <-time.After(10 * time.Second):
		s.Fail("callback never fired within timeout")
	}
}
