// Functional test for the experimental-API incubator. Imports both the stable
// go.temporal.io/api module and the experimental github.com/temporalio/api-go/experimental
// module. No build tags required — the experimental module is a plain Go dependency.
package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	expenums "github.com/temporalio/api-go/experimental/enums/v1"
	expworkflowservice "github.com/temporalio/api-go/experimental/workflowservice/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestExperimentalApi_Example(t *testing.T) {
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.FrontendAPIVariant, "example"))

	ctx := env.Context()
	frontend := env.ExperimentalFrontend()
	exampleClient := expworkflowservice.NewExampleWorkflowServiceClient(frontend.Workflow())

	t.Run("echo responds", func(t *testing.T) {
		resp, err := exampleClient.Echo(ctx, &expworkflowservice.EchoRequest{
			Namespace: env.Namespace().String(),
			Payload:   "hello",
		})
		require.NoError(t, err)
		require.Equal(t, "hello", resp.GetPayload())
	})

	t.Run("stable RPCs still work", func(t *testing.T) {
		_, err := env.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
	})

	t.Run("experimental enum value handled by server", func(t *testing.T) {
		_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                env.Namespace().String(),
			WorkflowId:               "test-experimental-enum",
			WorkflowType:             &commonpb.WorkflowType{Name: "test"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "test"},
			WorkflowIdConflictPolicy: expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO,
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("experimental overlay field handled by server", func(t *testing.T) {
		req := &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   "test-experimental-overlay",
			WorkflowType: &commonpb.WorkflowType{Name: "test"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "test"},
		}
		require.NoError(t, expworkflowservice.SetStartWorkflowExecutionRequestOverlay(req,
			&expworkflowservice.StartWorkflowExecutionRequestOverlay{FooText: "hello overlay"}))
		_, err := env.FrontendClient().StartWorkflowExecution(ctx, req)
		require.NotEqual(t, codes.Unimplemented, status.Code(err))
	})
}

func TestExperimentalApi_Stable(t *testing.T) {
	env := testcore.NewEnv(t)

	ctx := env.Context()
	frontend := env.ExperimentalFrontend()
	exampleClient := expworkflowservice.NewExampleWorkflowServiceClient(frontend.Workflow())

	t.Run("echo not available", func(t *testing.T) {
		_, err := exampleClient.Echo(ctx, &expworkflowservice.EchoRequest{})
		require.Error(t, err)
		require.Equal(t, codes.Unimplemented, serviceerror.ToStatus(err).Code())
	})

	t.Run("stable RPCs work", func(t *testing.T) {
		_, err := env.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: env.Namespace().String(),
		})
		if err != nil {
			var namespaceNotFound *serviceerror.NamespaceNotFound
			require.NotEqual(t, codes.Unimplemented, status.Code(err))
			require.ErrorAs(t, err, &namespaceNotFound)
		}
	})
}

func TestExperimentalApi_MessageOverlay(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{}

	overlay := &expworkflowservice.StartWorkflowExecutionRequestOverlay{
		FooText: "hello overlay",
	}
	require.NoError(t, expworkflowservice.SetStartWorkflowExecutionRequestOverlay(req, overlay))

	got, ok, err := expworkflowservice.GetStartWorkflowExecutionRequestOverlay(req)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "hello overlay", got.GetFooText())

	payload, err := proto.Marshal(req)
	require.NoError(t, err)
	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))

	got, ok, err = expworkflowservice.GetStartWorkflowExecutionRequestOverlay(roundTrip)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "hello overlay", got.GetFooText())

	require.NoError(t, expworkflowservice.ClearStartWorkflowExecutionRequestOverlay(roundTrip))
	got, ok, err = expworkflowservice.GetStartWorkflowExecutionRequestOverlay(roundTrip)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, got.GetFooText())
}

func TestExperimentalApi_EnumValue(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowIdConflictPolicy: expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO,
	}

	require.Equal(t, expenums.WorkflowIdConflictPolicy(1000), req.GetWorkflowIdConflictPolicy())

	payload, err := proto.Marshal(req)
	require.NoError(t, err)

	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	require.Equal(t, expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO, roundTrip.GetWorkflowIdConflictPolicy())
}
