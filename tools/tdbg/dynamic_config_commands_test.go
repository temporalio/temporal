package tdbg

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

type dynamicConfigAdminClient struct {
	adminservice.AdminServiceClient
	request    *adminservice.GetDynamicConfigValueRequest
	dumpCalled bool
}

func (c *dynamicConfigAdminClient) DumpDynamicConfigValues(
	_ context.Context,
	_ *adminservice.DumpDynamicConfigValuesRequest,
	_ ...grpc.CallOption,
) (*adminservice.DumpDynamicConfigValuesResponse, error) {
	c.dumpCalled = true
	return &adminservice.DumpDynamicConfigValuesResponse{
		Values: []byte(`{"frontend.workflowtimeskippingenabled":[{"constraints":{"namespace":"A"},"value":true}]}`),
	}, nil
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

func TestDumpDynamicConfigValues(t *testing.T) {
	t.Chdir(t.TempDir())
	adminClient := &dynamicConfigAdminClient{}
	var output bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
	})

	err := app.Run([]string{"tdbg", "dc", "dump", "cvs"})
	require.NoError(t, err)
	require.True(t, adminClient.dumpCalled)
	filename := strings.TrimSpace(output.String())
	require.Regexp(t, `^tmp_dc_cvs_\d{8}T\d{6}Z\.json$`, filename)
	contents, err := os.ReadFile(filepath.Clean(filename))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"frontend.workflowtimeskippingenabled": [{
			"constraints": {"namespace": "A"},
			"value": true
		}]
	}`, string(contents))
}
