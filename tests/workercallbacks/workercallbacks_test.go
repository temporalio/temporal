package workercallbacks

import (
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"

	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	operatorservicepb "go.temporal.io/api/operatorservice/v1"

	"go.temporal.io/server/tests/workercallbacks/caller"
	"go.temporal.io/server/tests/workercallbacks/handler"
)

// Basic example of calling a SANO-based operation and having a user-supplied callback get
// invoked when the Nexus operation completes.

type WorkerCallbacksSuite struct {
	parallelsuite.Suite[*WorkerCallbacksSuite]
}

func TestWorkerCallbacksSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkerCallbacksSuite{})
}

func (s *WorkerCallbacksSuite) newEnv() *testcore.TestEnv {
	env := testcore.NewEnv(s.T())

	// Enable Dynamic Configuration values for both the caller and handler namespace.
	// (And not just the default s.nsName.)
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: "handler-ns"}, Value: value},
			{Constraints: dynamicconfig.Constraints{Namespace: "caller-ns"}, Value: value},
		}
	}

	cluster := env.GetTestCluster()
	// Enable CHASM
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableCHASMCallbacks, nsValues(true))
	// Enable SAA
	cluster.OverrideDynamicConfig(s.T(), activity.Enabled, nsValues(true))
	// Enable SANO
	cluster.OverrideDynamicConfig(s.T(), nexusoperation.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), nexusoperation.EnableChasmWorkflowOperations, nsValues(true))

	return env
}

// Run the test via:
// % go test ./tests/workercallbacks -v -count=1
func (s *WorkerCallbacksSuite) TestBasicExample() {
	env := s.newEnv()
	ctx := s.Context()

	const (
		handlerNamespace = "handler-ns"
		callerNamespace  = "caller-ns"
	)

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
	nexusEndpointName := "nexus-endpoint"
	createSvcReq := &operatorservicepb.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: nexusEndpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: handlerNamespace,
						TaskQueue: handler.HandlerTaskQueue,
					},
				},
			},
		},
	}
	_, err = env.OperatorClient().CreateNexusEndpoint(ctx, createSvcReq)
	s.NoError(err, "creating Nexus endpoint")

	// Caller Worker
	//
	// The caller registers a worker callback handler.
	callerClient := env.SdkClientForNamespace(callerNamespace)
	callerWorker, err := caller.NewWorker(callerClient)
	s.NoError(err, "creating Caller worker")
	err = callerWorker.Start()
	s.NoError(err, "starting Caller worker")
	defer callerWorker.Stop()

	// Starter
	//
	// We call the Nexus operation via SANO.
	nexusClient, err := callerClient.NewNexusClient(client.NexusClientOptions{
		Endpoint: nexusEndpointName,
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
	callHandle, err := nexusClient.ExecuteOperation(ctx, handler.AddOperationName, callInput, callOpts)
	s.NoError(err, "calling Nexus operation")

	var callOutput handler.AddOutput
	err = callHandle.Get(ctx, &callOutput)
	s.NoError(err, "getting Nexus operation result")

	// Confirm the result is correct.
	s.Equal(int8(-56), callOutput.Sum)
	s.True(callOutput.Overflow)
}
