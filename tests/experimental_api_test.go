// Functional test for the experimental-API incubator. This file is NOT
// build-tag-gated: it always compiles, importing types only from the stable
// go.temporal.io/api module (the experimental symbols are gated there by
// //go:build experimental, so they appear in the test binary via ALL_TEST_TAGS).
//
// The Makefile passes `experimental` for all test invocations by default,
// so `make test` Just Works. To explicitly verify the production path:
//
//	go test -run TestExperimentalApi_Stable ./tests/...
//
// (The Stable subtest works regardless of the tag because no variant is
// requested.)
package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
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
	exampleClient := workflowservice.NewExampleWorkflowServiceClient(frontend.Workflow())

	t.Run("echo responds", func(t *testing.T) {
		resp, err := exampleClient.Echo(ctx, &workflowservice.EchoRequest{
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
}

func TestExperimentalApi_Stable(t *testing.T) {
	env := testcore.NewEnv(t)

	ctx := env.Context()
	frontend := env.ExperimentalFrontend()
	exampleClient := workflowservice.NewExampleWorkflowServiceClient(frontend.Workflow())

	t.Run("echo not available", func(t *testing.T) {
		_, err := exampleClient.Echo(ctx, &workflowservice.EchoRequest{})
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

// TestExperimentalApi_MessageOverlay is skipped until experimental_field
// annotations are added to StartWorkflowExecutionRequest in the proto,
// which will generate the overlay types into go.temporal.io/api/workflowservice/v1.
func TestExperimentalApi_MessageOverlay(t *testing.T) {
	t.Skip("requires experimental_field annotations on StartWorkflowExecutionRequest — not yet added")
}

func TestExperimentalApi_EnumValue(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{
		// enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO is generated into enums/v1 under //go:build experimental
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO,
	}

	require.Equal(t, enumspb.WorkflowIdConflictPolicy(1000), req.GetWorkflowIdConflictPolicy())

	payload, err := proto.Marshal(req)
	require.NoError(t, err)

	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO, roundTrip.GetWorkflowIdConflictPolicy())
}
