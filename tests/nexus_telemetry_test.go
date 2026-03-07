package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
)

func TestNexusTelemetrySuite(t *testing.T) {
	testcore.MustRunSequential(t, "uses process-global OTEL env vars")
	collector := testtelemetry.SetupMemoryCollector(t)

	t.Run("NexusSyncOperation", func(t *testing.T) {
		const nexusEndpointName = "test-nexus-sync-endpoint"
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up Nexus callback URL.
		s.OverrideDynamicConfig(
			nexusoperations.CallbackURLTemplate,
			"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

		// Create SDK client.
		sdkClient, err := client.Dial(client.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name

		// Create Nexus endpoint.
		_, err = s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: nexusEndpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: s.Namespace().String(),
							TaskQueue: taskQueue + "-handler",
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Register Nexus handler worker with a sync echo operation.
		echoOp := nexus.NewSyncOperation("echo", func(ctx context.Context, input string, options nexus.StartOperationOptions) (string, error) {
			return input, nil
		})
		svc := nexus.NewService("test-service")
		require.NoError(t, svc.Register(echoOp))
		hw := worker.New(sdkClient, taskQueue+"-handler", worker.Options{})
		hw.RegisterNexusService(svc)
		require.NoError(t, hw.Start())
		defer hw.Stop()

		// Register caller workflow worker.
		callerWf := func(ctx workflow.Context, msg string) (string, error) {
			c := workflow.NewNexusClient(nexusEndpointName, svc.Name)
			fut := c.ExecuteOperation(ctx, echoOp.Name(), msg, workflow.NexusOperationOptions{})
			var result string
			if err := fut.Get(ctx, &result); err != nil {
				return "", err
			}
			return result, nil
		}
		cw := worker.New(sdkClient, taskQueue+"-caller", worker.Options{})
		cw.RegisterWorkflow(callerWf)
		require.NoError(t, cw.Start())
		defer cw.Stop()

		// Run workflow.
		run, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: taskQueue + "-caller",
		}, callerWf, "hello")
		require.NoError(t, err)

		var result string
		require.NoError(t, run.Get(ctx, &result))
		assert.Equal(t, "hello", result)

		callerWfID := run.GetID()
		testtelemetry.RequireSpans(t, collector,
			// PollNexusTaskQueue trace is separate; caller WF ID links it to the caller.
			testtelemetry.SpanPattern{"rpc.method": "PollNexusTaskQueue", telemetry.WorkflowIDKey: callerWfID},
			// RespondNexusTaskCompleted: sync op, connected via OTEL propagation.
			testtelemetry.SpanPattern{"rpc.method": "RespondNexusTaskCompleted"},
		)
	})

	t.Run("NexusAsyncOperationLinks", func(t *testing.T) {
		const nexusEndpointName = "test-nexus-async-endpoint"
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up Nexus callback URL.
		s.OverrideDynamicConfig(
			nexusoperations.CallbackURLTemplate,
			"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

		// Create SDK client.
		sdkClient, err := client.Dial(client.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		handlerWfID := s.Tv().WorkflowID() + "-handler"

		// Create Nexus endpoint.
		_, err = s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: nexusEndpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: s.Namespace().String(),
							TaskQueue: taskQueue + "-handler",
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Register Nexus handler worker with an async workflow-backed operation.
		handlerWf := func(_ workflow.Context, name string) (string, error) {
			return fmt.Sprintf("Hello %s!", name), nil
		}
		helloOp := temporalnexus.NewWorkflowRunOperation("say-hello", handlerWf, func(ctx context.Context, name string, options nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			return client.StartWorkflowOptions{
				ID: handlerWfID,
			}, nil
		})
		svc := nexus.NewService("test-service")
		require.NoError(t, svc.Register(helloOp))
		hw := worker.New(sdkClient, taskQueue+"-handler", worker.Options{})
		hw.RegisterNexusService(svc)
		hw.RegisterWorkflow(handlerWf)
		require.NoError(t, hw.Start())
		defer hw.Stop()

		// Register caller workflow worker.
		callerWf := func(ctx workflow.Context, name string) (string, error) {
			c := workflow.NewNexusClient(nexusEndpointName, svc.Name)
			fut := c.ExecuteOperation(ctx, helloOp.Name(), name, workflow.NexusOperationOptions{})
			var result string
			if err := fut.Get(ctx, &result); err != nil {
				return "", err
			}
			return result, nil
		}
		cw := worker.New(sdkClient, taskQueue+"-caller", worker.Options{})
		cw.RegisterWorkflow(callerWf)
		require.NoError(t, cw.Start())
		defer cw.Stop()

		// Run workflow.
		run, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: taskQueue + "-caller",
		}, callerWf, "World")
		require.NoError(t, err)

		var result string
		require.NoError(t, run.Get(ctx, &result))
		assert.Equal(t, "Hello World!", result)

		callerWfID := run.GetID()
		testtelemetry.RequireSpans(t, collector,
			// PollNexusTaskQueue trace is separate; caller WF ID links it to the caller.
			testtelemetry.SpanPattern{"rpc.method": "PollNexusTaskQueue", telemetry.WorkflowIDKey: callerWfID},
			// RespondNexusTaskCompleted: handler WF ID links this span to the handler WF.
			testtelemetry.SpanPattern{"rpc.method": "RespondNexusTaskCompleted", telemetry.WorkflowIDKey: handlerWfID},
			// Handler workflow round-trips share the caller's OTEL trace via propagation.
			testtelemetry.SpanPattern{"rpc.method": "PollWorkflowTaskQueue", telemetry.WorkflowIDKey: handlerWfID},
			testtelemetry.SpanPattern{"rpc.method": "RespondWorkflowTaskCompleted", telemetry.WorkflowIDKey: handlerWfID},
		)
	})
}
