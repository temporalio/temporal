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
	dumpCalled  bool
	dumpOptions []grpc.CallOption
}

func (c *dynamicConfigAdminClient) DumpDynamicConfigValues(
	_ context.Context,
	_ *adminservice.DumpDynamicConfigValuesRequest,
	options ...grpc.CallOption,
) (*adminservice.DumpDynamicConfigValuesResponse, error) {
	c.dumpCalled = true
	c.dumpOptions = options
	return &adminservice.DumpDynamicConfigValuesResponse{
		Values: []byte(`{"frontend.workflowtimeskippingenabled":[{"constraints":{"namespace":"A"},"value":true}]}`),
	}, nil
}

type dynamicConfigClientFactory struct {
	ClientFactory
	adminClient adminservice.AdminServiceClient
}

func (f dynamicConfigClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.adminClient
}

func TestDumpDynamicConfigValues(t *testing.T) {
	t.Chdir(t.TempDir())
	adminClient := &dynamicConfigAdminClient{}
	var output bytes.Buffer
	var stderr bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
		params.ErrWriter = &stderr
	})

	err := app.Run([]string{"tdbg", "dc", "dump"})
	require.NoError(t, err)
	require.True(t, adminClient.dumpCalled)
	require.Len(t, adminClient.dumpOptions, 1)
	maxReceiveSize, ok := adminClient.dumpOptions[0].(grpc.MaxRecvMsgSizeCallOption)
	require.True(t, ok)
	require.Equal(t, dynamicConfigDumpMaxReceiveSize, maxReceiveSize.MaxRecvMsgSize)
	require.Equal(t, dynamicConfigDumpNote+"\n", stderr.String())
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

func TestDynamicConfigDumpHelp(t *testing.T) {
	var output bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.Writer = &output
	})

	err := app.Run([]string{"tdbg", "dc", "dump", "--help"})
	require.NoError(t, err)
	require.Contains(t, output.String(), "Dump all configured dynamic config values")
	require.Contains(t, output.String(), "tdbg dynamic-config dump [command options]")
	require.Contains(t, output.String(), "tdbg dc dump [command options]")
	require.NotContains(t, output.String(), "cvs")
}
