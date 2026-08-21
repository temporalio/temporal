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
	dumpCalled      bool
	dumpOptions     []grpc.CallOption
	request         *adminservice.GetDynamicConfigValueRequest
	getResponse     *adminservice.GetDynamicConfigValueResponse
	describeRequest *adminservice.DescribeDynamicConfigSettingRequest
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

func (c *dynamicConfigAdminClient) GetDynamicConfigValue(
	_ context.Context,
	request *adminservice.GetDynamicConfigValueRequest,
	_ ...grpc.CallOption,
) (*adminservice.GetDynamicConfigValueResponse, error) {
	c.request = request
	if c.getResponse != nil {
		return c.getResponse, nil
	}
	return &adminservice.GetDynamicConfigValueResponse{Value: []byte("true")}, nil
}

func (c *dynamicConfigAdminClient) DescribeDynamicConfigSetting(
	_ context.Context,
	request *adminservice.DescribeDynamicConfigSettingRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeDynamicConfigSettingResponse, error) {
	c.describeRequest = request
	return &adminservice.DescribeDynamicConfigSettingResponse{
		Key:                  request.GetKey(),
		ValueType:            "float64",
		Precedence:           "TaskQueue",
		SupportedConstraints: []string{"namespace", "taskQueueName", "taskQueueType"},
		ConstraintPrecedence: []*adminservice.DynamicConfigConstraintFields{
			{Fields: []string{"namespace", "taskQueueName", "taskQueueType"}},
			{Fields: []string{"namespace", "taskQueueName"}},
			{Fields: []string{"taskQueueName"}},
			{Fields: []string{"namespace"}},
			{},
		},
	}, nil
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
		"dc", "get",
		"--key", "frontend.WorkflowTimeSkippingEnabled",
		"--constraints", `{"namespace":"A"}`,
	})
	require.NoError(t, err)
	require.Equal(t, "frontend.WorkflowTimeSkippingEnabled", adminClient.request.GetKey())
	require.JSONEq(t, `{"namespace":"A"}`, adminClient.request.GetConstraints())
	require.False(t, adminClient.request.GetIncludeConstrainedValues())
	require.Equal(t, "true\n", output.String())
}

func TestGetDynamicConfigValueInvalidConstraints(t *testing.T) {
	adminClient := &dynamicConfigAdminClient{}
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
	})
	app.ExitErrHandler = func(*cli.Context, error) {}

	err := app.Run([]string{
		"tdbg",
		"dc", "get",
		"--key", "frontend.WorkflowTimeSkippingEnabled",
		"--constraints", `{"shardId":"one"}`,
	})
	require.ErrorContains(t, err, "invalid dynamic config constraints")
	require.Nil(t, adminClient.request)
}

func TestGetDynamicConfigValueVerbose(t *testing.T) {
	adminClient := &dynamicConfigAdminClient{}
	adminClient.getResponse = &adminservice.GetDynamicConfigValueResponse{
		Value: []byte("true"),
		ConstrainedValues: []byte(`[
			{"constraints":{"namespace":"A"},"value":true},
			{"constraints":{"namespace":"B"},"value":false},
			{"constraints":{},"value":false}
		]`),
	}
	var output bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
	})

	err := app.Run([]string{
		"tdbg",
		"dc", "get",
		"-k", "frontend.WorkflowTimeSkippingEnabled",
		"-c", `{"namespace":"A"}`,
		"-v",
	})
	require.NoError(t, err)
	require.True(t, adminClient.request.GetIncludeConstrainedValues())
	require.JSONEq(t, `{
		"key": "frontend.WorkflowTimeSkippingEnabled",
		"queryConstraints": {"namespace": "A"},
		"effectiveValue": true,
		"constrainedValues": [
			{"constraints":{"namespace":"A"},"value":true},
			{"constraints":{"namespace":"B"},"value":false},
			{"constraints":{},"value":false}
		]
	}`, output.String())
}

func TestDescribeDynamicConfigSetting(t *testing.T) {
	adminClient := &dynamicConfigAdminClient{}
	var output bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
	})

	err := app.Run([]string{
		"tdbg",
		"dc", "describe",
		"-k", "admin.matchingNamespaceTaskqueueToPartitionDispatchRate",
	})
	require.NoError(t, err)
	require.Equal(t, "admin.matchingNamespaceTaskqueueToPartitionDispatchRate", adminClient.describeRequest.GetKey())
	require.JSONEq(t, `{
		"key": "admin.matchingNamespaceTaskqueueToPartitionDispatchRate",
		"valueType": "float64",
		"precedence": "TaskQueue",
		"supportedConstraints": ["namespace", "taskQueueName", "taskQueueType"],
		"order": [
			{"namespace":"<namespace>","taskQueueName":"<taskQueueName>","taskQueueType":"<taskQueueType>"},
			{"namespace":"<namespace>","taskQueueName":"<taskQueueName>"},
			{"taskQueueName":"<taskQueueName>"},
			{"namespace":"<namespace>"},
			{}
		]
	}`, output.String())
}

func TestDynamicConfigHelpShowsAliasUsage(t *testing.T) {
	for _, args := range [][]string{
		{"tdbg", "dc", "--help"},
		{"tdbg", "dc", "get", "--help"},
		{"tdbg", "dc", "describe", "--help"},
		{"tdbg", "dc", "dump", "--help"},
	} {
		t.Run(strings.Join(args[1:], " "), func(t *testing.T) {
			var output bytes.Buffer
			app := NewCliApp(func(params *Params) {
				params.Writer = &output
			})

			err := app.Run(args)
			require.NoError(t, err)
			require.Contains(t, output.String(), "tdbg dynamic-config")
			require.Contains(t, output.String(), "tdbg dc")
		})
	}
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
