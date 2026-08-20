package tdbg

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

type dynamicConfigAdminClient struct {
	adminservice.AdminServiceClient
	request *adminservice.GetDynamicConfigValueRequest
}

func (c *dynamicConfigAdminClient) GetDynamicConfigValue(
	_ context.Context,
	request *adminservice.GetDynamicConfigValueRequest,
	_ ...grpc.CallOption,
) (*adminservice.GetDynamicConfigValueResponse, error) {
	c.request = request
	return &adminservice.GetDynamicConfigValueResponse{Value: []byte("true")}, nil
}

type dynamicConfigClientFactory struct {
	ClientFactory
	adminClient adminservice.AdminServiceClient
}

func (f dynamicConfigClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.adminClient
}

func TestGetDynamicConfigValue(t *testing.T) {
	adminClient := &dynamicConfigAdminClient{}
	var output bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
	})

	err := app.Run([]string{
		"tdbg",
		"--namespace", "A",
		"dc", "get",
		"--key", "frontend.WorkflowTimeSkippingEnabled",
	})
	require.NoError(t, err)
	require.Equal(t, "frontend.WorkflowTimeSkippingEnabled", adminClient.request.GetKey())
	require.Equal(t, "A", adminClient.request.GetNamespace())
	require.Equal(t, "true\n", output.String())
}
