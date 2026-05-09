// Functional test for the api_next incubator. go.temporal.io/api is replaced
// with api_next in this branch, so experimental fields and RPCs are available
// on the regular generated API packages.
package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestExperimentalApi_Server(t *testing.T) {
	env := testcore.NewEnv(t)

	ctx := env.Context()

	t.Run("experimental RPC is exposed but not implemented", func(t *testing.T) {
		_, err := env.FrontendClient().Echo(ctx, &workflowservice.EchoRequest{})
		require.Error(t, err)
		require.Contains(t, []codes.Code{codes.InvalidArgument, codes.Unimplemented}, serviceerror.ToStatus(err).Code())
	})

	t.Run("stable RPCs work", func(t *testing.T) {
		_, err := env.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
	})

	t.Run("experimental enum value reaches server", func(t *testing.T) {
		_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   "test-experimental-enum",
			WorkflowType: &commonpb.WorkflowType{Name: "test"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "test"},
			// temporal:allow-experimental-api example -- verifies the incubating enum is wired through the server path.
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO,
		})
		require.NotEqual(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("experimental field reaches server", func(t *testing.T) {
		req := &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   "test-experimental-overlay",
			WorkflowType: &commonpb.WorkflowType{Name: "test"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "test"},
		}
		cleanup := env.OverrideDynamicConfig(dynamicconfig.FrontendEnableExperimentalAPIExample, true)
		defer cleanup()
		// temporal:allow-experimental-api example -- verifies the incubating field is accepted by generated types.
		req.FooText = "hello api_next"
		_, err := env.FrontendClient().StartWorkflowExecution(ctx, req)
		require.NotEqual(t, codes.Unimplemented, status.Code(err))
	})
}

func TestExperimentalApi_MessageField(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{}
	// temporal:allow-experimental-api example -- verifies protobuf round-tripping for the incubating field.
	req.FooText = "hello api_next"

	payload, err := proto.Marshal(req)
	require.NoError(t, err)
	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	// temporal:allow-experimental-api example -- verifies protobuf round-tripping for the incubating getter.
	require.Equal(t, "hello api_next", roundTrip.GetFooText())
}

func TestExperimentalApi_EnumValue(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{
		// temporal:allow-experimental-api example -- verifies protobuf round-tripping for the incubating enum.
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO,
	}

	require.Equal(t, enumspb.WorkflowIdConflictPolicy(1000), req.GetWorkflowIdConflictPolicy())

	payload, err := proto.Marshal(req)
	require.NoError(t, err)

	roundTrip := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	// temporal:allow-experimental-api example -- verifies protobuf round-tripping for the incubating enum.
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO, roundTrip.GetWorkflowIdConflictPolicy())
}
