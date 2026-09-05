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
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type dynamicConfigAdminClient struct {
	adminservice.AdminServiceClient
	dumpCalled      bool
	dumpOptions     []grpc.CallOption
	dumpError       error
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
	if c.dumpError != nil {
		return nil, c.dumpError
	}
	return &adminservice.DumpDynamicConfigValuesResponse{
		Values: []byte(`frontend.workflowtimeskippingenabled:
  - constraints:
      namespace: A
    value: true
`),
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
		Key:                   request.GetKey(),
		ValueType:             "float64",
		ConstraintDescription: "[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, TaskQueueType: taskQueueType}, {}}",
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
	var stderr bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
		params.ErrWriter = &stderr
	})

	err := app.Run([]string{
		"tdbg",
		"dc", "get",
		"--key", "frontend.WorkflowTimeSkippingEnabled",
		"--constraints", `{namespace: A}`,
	})
	require.NoError(t, err)
	require.Equal(t, "frontend.WorkflowTimeSkippingEnabled", adminClient.request.GetKey())
	require.Equal(t, `{namespace: A}`, adminClient.request.GetConstraints())
	require.False(t, adminClient.request.GetIncludeConstrainedValues())
	require.Equal(t, "true\n", output.String())
	require.Equal(t, dynamicConfigGetNote+"\n", stderr.String())
}

func TestGetDynamicConfigValuePersistenceDynamicRateLimitingParams(t *testing.T) {
	value, err := dynamicconfig.MarshalValueYAML(dynamicconfig.DefaultDynamicRateLimitingParams)
	require.NoError(t, err)

	adminClient := &dynamicConfigAdminClient{
		getResponse: &adminservice.GetDynamicConfigValueResponse{Value: value},
	}
	var output bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
	})

	err = app.Run([]string{
		"tdbg",
		"dc", "get",
		"-k", "frontend.persistenceDynamicRateLimitingParams",
	})
	require.NoError(t, err)
	require.Equal(t, "frontend.persistenceDynamicRateLimitingParams", adminClient.request.GetKey())
	requireYAMLEq(t, `
		enabled: false
		refreshinterval: 10s
		latencythreshold: 0
		errorthreshold: 0
		ratebackoffstepsize: 0.3
		rateincreasestepsize: 0.1
		ratemultimin: 0.8
		ratemultimax: 1
	`, output.String())
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
		"--constraints", `{shardId: one}`,
	})
	require.ErrorContains(t, err, "invalid dynamic config constraints")
	require.Nil(t, adminClient.request)
}

func TestGetDynamicConfigValueVerbose(t *testing.T) {
	adminClient := &dynamicConfigAdminClient{}
	adminClient.getResponse = &adminservice.GetDynamicConfigValueResponse{
		Value:                 []byte("true"),
		ConstraintDescription: "[]Constraints{{Namespace: namespace}, {}}",
		ConstrainedValues: []byte(`
- constraints:
    namespace: A
  value: true
- constraints:
    namespace: B
  value: false
- constraints: {}
  value: false
`),
	}
	var output bytes.Buffer
	var stderr bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
		params.Writer = &output
		params.ErrWriter = &stderr
	})

	err := app.Run([]string{
		"tdbg",
		"dc", "get",
		"-k", "frontend.WorkflowTimeSkippingEnabled",
		"-c", `{namespace: A}`,
		"-v",
	})
	require.NoError(t, err)
	require.True(t, adminClient.request.GetIncludeConstrainedValues())
	require.Equal(t, dynamicConfigGetNote+"\n", stderr.String())
	requireYAMLEq(t, `
		key: frontend.WorkflowTimeSkippingEnabled
		queryConstraints:
		  namespace: A
		constraintDescription: '[]Constraints{{Namespace: namespace}, {}}'
		effectiveValue: true
		constrainedValues:
		  - constraints:
		      namespace: A
		    value: true
		  - constraints:
		      namespace: B
		    value: false
		  - constraints: {}
		    value: false
	`, output.String())
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
	requireYAMLEq(t, `
		key: admin.matchingNamespaceTaskqueueToPartitionDispatchRate
		valueType: float64
		constraintDescription: '[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, TaskQueueType: taskQueueType}, {}}'
	`, output.String())
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
	require.Regexp(t, `^tmp_dc_cvs_\d{8}T\d{6}Z\.yaml$`, filename)
	contents, err := os.ReadFile(filepath.Clean(filename))
	require.NoError(t, err)
	loadedValues := dynamicconfig.LoadYamlFile(contents)
	require.Empty(t, loadedValues.Errors)
	require.Equal(t, []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: "A"},
		Value:       true,
	}}, loadedValues.Map[dynamicconfig.MakeKey("frontend.workflowtimeskippingenabled")])
}

func TestDumpDynamicConfigValuesReturnsServerError(t *testing.T) {
	t.Chdir(t.TempDir())
	adminClient := &dynamicConfigAdminClient{
		dumpError: status.Error(
			codes.Internal,
			`unable to encode dynamic config values: dynamic config key "invalid" constrained value at index 0: cannot marshal type: chan struct {}`,
		),
	}
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = dynamicConfigClientFactory{adminClient: adminClient}
	})
	app.ExitErrHandler = func(*cli.Context, error) {}

	err := app.Run([]string{"tdbg", "dc", "dump"})
	require.EqualError(
		t,
		err,
		`unable to dump dynamic config values: rpc error: code = Internal desc = unable to encode dynamic config values: dynamic config key "invalid" constrained value at index 0: cannot marshal type: chan struct {}`,
	)
	files, readErr := os.ReadDir(".")
	require.NoError(t, readErr)
	require.Empty(t, files)
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

func requireYAMLEq(t *testing.T, expected string, actual string) {
	t.Helper()
	var expectedValue any
	require.NoError(t, yaml.Unmarshal([]byte(strings.ReplaceAll(expected, "\t", "")), &expectedValue))
	var actualValue any
	require.NoError(t, yaml.Unmarshal([]byte(actual), &actualValue))
	require.Equal(t, expectedValue, actualValue)
}
