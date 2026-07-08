package nexusworkercallbacks

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"

	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	operatorservicepb "go.temporal.io/api/operatorservice/v1"

	"go.temporal.io/server/tests/nexusworkercallbacks/caller"
	"go.temporal.io/server/tests/nexusworkercallbacks/fauxsdk"
	"go.temporal.io/server/tests/nexusworkercallbacks/handler"
)

// Test cases for "worker callbacks" being invoked in the calling namespace whenever a SANO-based
// operation reaches a terminal state.

// Namespace names for tests.
const (
	callerNamespace  = "caller-ns"
	handlerNamespace = "handler-ns"
)

type NexusWorkerCallbacksSuite struct {
	parallelsuite.Suite[*NexusWorkerCallbacksSuite]
}

func TestNexusWorkerCallbacksSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusWorkerCallbacksSuite{})
}

func (s *NexusWorkerCallbacksSuite) newEnv() *testcore.TestEnv {
	env := testcore.NewEnv(s.T())

	// Dynamic configuration values. We need to apply these to both the handler and caller namespaces.
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: handlerNamespace}, Value: value},
			{Constraints: dynamicconfig.Constraints{Namespace: callerNamespace}, Value: value},
		}
	}
	dcSettings := []dynamicconfig.NamespaceTypedSetting[bool]{
		dynamicconfig.EnableChasm,
		dynamicconfig.EnableCHASMCallbacks,
		nexusoperation.Enabled,
		nexusoperation.EnableChasmWorkflowOperations,
		// TODO: Add a new dynamicconfig to control if worker callbacks are enabled for a namespace.
	}

	cluster := env.GetTestCluster()
	for _, dcSetting := range dcSettings {
		cluster.OverrideDynamicConfig(s.T(), dcSetting, nsValues(true))
	}

	return env
}

func (s *NexusWorkerCallbacksSuite) registerNexusEndpoint(ctx context.Context, env *testcore.TestEnv, endpoint, namespace, taskQueue string) error {
	createHandlerSvcReq := &operatorservicepb.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpoint,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: namespace,
						TaskQueue: taskQueue,
					},
				},
			},
		},
	}
	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, createHandlerSvcReq)
	return err
}

// Run the test via:
// % go test ./tests/nexusworkercallbacks -v -count=1 -tags=test_dep
func (s *NexusWorkerCallbacksSuite) TestBasicExample() {
	env := s.newEnv()
	ctx := s.Context()

	// Register the Temporal namespaces.
	retentionDays := int32(1)
	state := enumspb.ARCHIVAL_STATE_DISABLED
	uri := "uri://"
	_, err := env.RegisterNamespace(handlerNamespace, retentionDays, state, uri, uri)
	s.NoError(err, "registering handler namespace")
	_, err = env.RegisterNamespace(callerNamespace, retentionDays, state, uri, uri)
	s.NoError(err, "registering caller namespace")

	// Handler Worker
	//
	// Register the handler workflows and Nexus service.
	handlerClient := env.SdkClientForNamespace(handlerNamespace)
	handlerWorker, err := handler.NewWorker(handlerClient)
	s.NoError(err, "creating Handler worker")
	err = handlerWorker.Start()
	s.NoError(err, "starting Handler worker")
	defer handlerWorker.Stop()
	// Register the Nexus endpoint.
	handlerNexusEndpointName := "handler-nexus-endpoint"
	err = s.registerNexusEndpoint(ctx, env, handlerNexusEndpointName, handlerNamespace, handler.HandlerTaskQueue)
	s.NoError(err, "creating Nexus endpoint for handler")

	// Caller Worker
	//
	// The caller registers a worker callback handler.
	callerClient := env.SdkClientForNamespace(callerNamespace)
	callerWorker, err := caller.NewWorker(callerClient)
	s.NoError(err, "creating Caller worker")
	err = callerWorker.Start()
	s.NoError(err, "starting Caller worker")
	defer callerWorker.Stop()
	callerNexusEndpointName := "caller-nexus-endpoint"
	err = s.registerNexusEndpoint(ctx, env, callerNexusEndpointName, callerNamespace, caller.CallerTaskQueue)
	s.NoError(err, "creating Nexus endpoint for caller")

	// Reset the global variable, confirming the worker callback was invoked.
	caller.ResetTimesWorkerCallbackCalled()

	// Starter
	//
	// We call the Nexus operation via SANO.
	nexusClient, err := callerClient.NewNexusClient(client.NexusClientOptions{
		Endpoint: handlerNexusEndpointName,
		Service:  handler.NexusServiceName,
	})
	s.NoError(err, "creating Nexus client")

	callInput := handler.AddInput{
		A: 100,
		B: 100,
	}
	callOpts := client.StartNexusOperationOptions{
		ID:                     "add-100-to-100",
		ScheduleToCloseTimeout: 5 * time.Second,
	}

	// Attach the worker callback. The completion is dispatched to the caller's task queue, where the
	// completion Nexus service is registered.
	callbackRef := fauxsdk.CallbackRef{
		TaskQueueName: caller.CallerTaskQueue,
	}
	callCtx := caller.OnCompleteCallContext{
		Message: s.T().Name(),
	}
	fauxsdk.AttachWorkerCallback(&callOpts, callbackRef, callCtx)

	callHandle, err := nexusClient.ExecuteOperation(ctx, handler.AddOperationName, callInput, callOpts)
	s.NoError(err, "calling Nexus operation")

	var callOutput handler.AddOutput
	err = callHandle.Get(ctx, &callOutput)
	s.NoError(err, "getting Nexus operation result")

	// Confirm the result is correct.
	s.Equal(int8(-56), callOutput.Sum)
	s.True(callOutput.Overflow)

	// Confirm the worker callback was invoked.
	s.Eventually(func() bool {
		return caller.TimesWorkerCallbackCalled() > 0
	}, 10*time.Second, 200*time.Millisecond, "nexus-worker-callback was never executed")

	// Confirm the worker callback received the expected data.
	s.Equal(1, caller.TimesWorkerCallbackCalled())
	receivedCallback := caller.MustGetWorkerCallbackResult(0)
	gotCompletionInput := receivedCallback.Input

	// Confirm the source operation's successful outcome was delivered.
	s.NotNil(gotCompletionInput.GetOutcome().GetSuccess(), "expected a successful outcome")
	var gotResult handler.AddOutput
	err = payload.Decode(gotCompletionInput.GetOutcome().GetSuccess(), &gotResult)
	s.NoError(err, "decoding completion outcome")
	s.Equal(int8(-56), gotResult.Sum)
	s.True(gotResult.Overflow)

	// Confirm the user-supplied source context round-tripped through the callback.
	s.NotNil(gotCompletionInput.GetSourceContext(), "expected a source context")
	var gotCallCtx caller.OnCompleteCallContext
	err = payload.Decode(gotCompletionInput.GetSourceContext(), &gotCallCtx)
	s.NoError(err, "decoding source context")
	s.Equal(s.T().Name(), gotCallCtx.Message)
}
