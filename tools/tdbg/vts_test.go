package tdbg_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type vtsAdminClient struct {
	adminservice.AdminServiceClient
	describeResponse *adminservice.DescribeMutableStateResponse
	disableResponse  *adminservice.DisableTimeSkippingResponse
	describeRequest  *adminservice.DescribeMutableStateRequest
	disableRequest   *adminservice.DisableTimeSkippingRequest
}

func (c *vtsAdminClient) DescribeMutableState(
	_ context.Context,
	request *adminservice.DescribeMutableStateRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	c.describeRequest = request
	return c.describeResponse, nil
}

func (c *vtsAdminClient) DisableTimeSkipping(
	_ context.Context,
	request *adminservice.DisableTimeSkippingRequest,
	_ ...grpc.CallOption,
) (*adminservice.DisableTimeSkippingResponse, error) {
	c.disableRequest = request
	return c.disableResponse, nil
}

type vtsClientFactory struct {
	admin adminservice.AdminServiceClient
}

func (f vtsClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.admin
}

func (vtsClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return nil
}

func runVTS(t *testing.T, client adminservice.AdminServiceClient, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = vtsClientFactory{admin: client}
		params.Writer = &output
	})
	err := app.Run(append([]string{"tdbg", "--namespace", "test-namespace", "--yes", "vts"}, args...))
	return output.String(), err
}

func TestVTSGet(t *testing.T) {
	client := &vtsAdminClient{
		describeResponse: &adminservice.DescribeMutableStateResponse{
			DatabaseMutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
						Config: &commonpb.TimeSkippingConfig{Enabled: false},
					},
				},
			},
		},
	}
	output, err := runVTS(t, client, "get", "--workflow-id", "business-id", "--run-id", "run-id", "--archetype", "example.Archetype")
	require.NoError(t, err)
	require.Contains(t, output, `"config": {}`)
	require.NotContains(t, output, "databaseMutableState")
	require.Equal(t, &adminservice.DescribeMutableStateRequest{
		Namespace: "test-namespace",
		Execution: &commonpb.WorkflowExecution{WorkflowId: "business-id", RunId: "run-id"},
		Archetype: "example.Archetype",
	}, client.describeRequest)
}

func TestVTSGetNotConfigured(t *testing.T) {
	client := &vtsAdminClient{
		describeResponse: &adminservice.DescribeMutableStateResponse{
			DatabaseMutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
			},
		},
	}
	output, err := runVTS(t, client, "get", "--workflow-id", "business-id")
	require.NoError(t, err)
	require.Equal(t, "time skipping not enabled\n", output)
}

func TestVTSDisable(t *testing.T) {
	client := &vtsAdminClient{disableResponse: &adminservice.DisableTimeSkippingResponse{Disabled: true}}
	output, err := runVTS(t, client, "disable", "--workflow-id", "business-id", "--archetype-id", "42")
	require.NoError(t, err)
	require.Equal(t, "time skipping disabled\n", output)
	require.Equal(t, &adminservice.DisableTimeSkippingRequest{
		Namespace:   "test-namespace",
		Execution:   &commonpb.WorkflowExecution{WorkflowId: "business-id"},
		ArchetypeId: chasm.ArchetypeID(42),
	}, client.disableRequest)
}

func TestVTSDisableNoop(t *testing.T) {
	client := &vtsAdminClient{disableResponse: &adminservice.DisableTimeSkippingResponse{}}
	output, err := runVTS(t, client, "disable", "--workflow-id", "business-id")
	require.NoError(t, err)
	require.Equal(t, "time skipping not enabled\n", output)
}
