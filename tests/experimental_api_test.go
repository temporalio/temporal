// Functional test for the experimental-API incubator. This file is NOT
// build-tag-gated: it always compiles, importing the experimental api-go
// module directly (that module is unconditionally listed in go.mod).
//
// Without `-tags experimental`, the test binary builds but the cluster's
// frontend doesn't compile in the experimental registry, so a cluster that
// requests variant=example hits Fatal at startup and the test fails.
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
	expenums "github.com/temporalio/api-go/experimental/example/enums/v1"
	expexample "github.com/temporalio/api-go/experimental/example/workflowservice/v1"
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
	exampleClient := expexample.NewWorkflowServiceClient(frontend.Workflow())

	t.Run("echo responds", func(t *testing.T) {
		resp, err := exampleClient.Echo(ctx, &expexample.EchoRequest{
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
	exampleClient := expexample.NewWorkflowServiceClient(frontend.Workflow())

	t.Run("echo not available", func(t *testing.T) {
		_, err := exampleClient.Echo(ctx, &expexample.EchoRequest{})
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

	overlay := &expexample.StartWorkflowExecutionRequestOverlay{
		Foo:           &expexample.Foo{Value: "bar"},
		FooText:       "text",
		FooCount:      42,
		FooEnabled:    true,
		FooPayload:    []byte("payload"),
		FooTags:       []string{"a", "b"},
		FooNumbers:    []int64{1, 2},
		FooById:       map[string]*expexample.Foo{"one": &expexample.Foo{Value: "baz"}},
		FooCountsById: map[string]int64{"one": 7},
		FooPolicy:     expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO,
	}
	require.NoError(t, expexample.SetStartWorkflowExecutionRequestOverlay(req, overlay))

	got, ok, err := expexample.GetStartWorkflowExecutionRequestOverlay(req)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, proto.Equal(overlay, got))

	payload, err := proto.Marshal(req)
	require.NoError(t, err)

	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))

	got, ok, err = expexample.GetStartWorkflowExecutionRequestOverlay(roundTrip)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, proto.Equal(overlay, got))

	require.NoError(t, expexample.ClearStartWorkflowExecutionRequestOverlay(roundTrip))
	got, ok, err = expexample.GetStartWorkflowExecutionRequestOverlay(roundTrip)
	require.NoError(t, err)
	require.False(t, ok)
	require.NotNil(t, got)
	require.Nil(t, got.GetFoo())
}

func TestExperimentalApi_EnumValue(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowIdConflictPolicy: expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO,
	}

	require.Equal(t, enumspb.WorkflowIdConflictPolicy(1000), req.GetWorkflowIdConflictPolicy())

	payload, err := proto.Marshal(req)
	require.NoError(t, err)

	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	require.Equal(t, expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO, roundTrip.GetWorkflowIdConflictPolicy())
}
