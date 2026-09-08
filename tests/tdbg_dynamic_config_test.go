package tests

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"gopkg.in/yaml.v3"
)

var tdbgDCSettings = []dynamicconfig.GenericSetting{
	dynamicconfig.NewTaskQueueIntSetting("test.tdbg.task-queue", 0, ""),
	dynamicconfig.NewTaskTypeIntSetting("test.tdbg.history-task-type", 0, ""),
}

func TestTDBGDynamicConfigGetVerboseTaskTypePrecedences(t *testing.T) {
	contents, err := os.ReadFile("testdata/tdbg_dc_test.yaml")
	require.NoError(t, err)
	config := dynamicconfig.LoadYamlFile(contents)
	require.Empty(t, config.Errors)
	require.Empty(t, config.Warnings)
	require.Len(t, config.Map, len(tdbgDCSettings))

	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	for _, setting := range tdbgDCSettings {
		configuredValues := config.Map[setting.Key()]
		require.NotEmpty(t, configuredValues, setting.Key())
		env.OverrideDynamicConfig(setting, configuredValues)
	}

	testCases := []struct {
		name                  string
		key                   string
		constraints           string
		constraintDescription string
		effectiveValue        int
	}{
		{
			name:                  "task queue namespace name and type",
			key:                   "test.tdbg.task-queue",
			constraints:           `{"namespace":"namespace-a","taskQueueName":"task-queue-a","taskType":"Workflow"}`,
			constraintDescription: "[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, taskType: taskQueueType}, {Namespace: namespace, TaskQueueName: taskQueue}, {TaskQueueName: taskQueue}, {Namespace: namespace}, {}}",
			effectiveValue:        405,
		},
		{
			name:                  "task queue namespace and name",
			key:                   "test.tdbg.task-queue",
			constraints:           `{"namespace":"namespace-a","taskQueueName":"task-queue-a","taskType":"Activity"}`,
			constraintDescription: "[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, taskType: taskQueueType}, {Namespace: namespace, TaskQueueName: taskQueue}, {TaskQueueName: taskQueue}, {Namespace: namespace}, {}}",
			effectiveValue:        404,
		},
		{
			name:                  "task queue name",
			key:                   "test.tdbg.task-queue",
			constraints:           `{"namespace":"namespace-b","taskQueueName":"task-queue-a","taskType":"Activity"}`,
			constraintDescription: "[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, taskType: taskQueueType}, {Namespace: namespace, TaskQueueName: taskQueue}, {TaskQueueName: taskQueue}, {Namespace: namespace}, {}}",
			effectiveValue:        403,
		},
		{
			name:                  "task queue namespace",
			key:                   "test.tdbg.task-queue",
			constraints:           `{"namespace":"namespace-a","taskQueueName":"task-queue-b","taskType":"Activity"}`,
			constraintDescription: "[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, taskType: taskQueueType}, {Namespace: namespace, TaskQueueName: taskQueue}, {TaskQueueName: taskQueue}, {Namespace: namespace}, {}}",
			effectiveValue:        402,
		},
		{
			name:                  "task queue global fallback",
			key:                   "test.tdbg.task-queue",
			constraints:           `{"namespace":"namespace-b","taskQueueName":"task-queue-b","taskType":"Activity"}`,
			constraintDescription: "[]Constraints{{Namespace: namespace, TaskQueueName: taskQueue, taskType: taskQueueType}, {Namespace: namespace, TaskQueueName: taskQueue}, {TaskQueueName: taskQueue}, {Namespace: namespace}, {}}",
			effectiveValue:        401,
		},
		{
			name:                  "history task type exact",
			key:                   "test.tdbg.history-task-type",
			constraints:           `{"historyTaskType":"TransferWorkflowTask"}`,
			constraintDescription: "[]Constraints{{historyTaskType: taskType}, {}}",
			effectiveValue:        601,
		},
		{
			name:                  "history task type global fallback",
			key:                   "test.tdbg.history-task-type",
			constraints:           `{"historyTaskType":"ActivityRetryTimer"}`,
			constraintDescription: "[]Constraints{{historyTaskType: taskType}, {}}",
			effectiveValue:        600,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
				params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(env.FrontendGRPCAddress()))
				params.Writer = &stdout
				params.ErrWriter = &stderr
			})
			err := app.RunContext(t.Context(), []string{
				"tdbg",
				"--color", "never",
				"dc", "get",
				"--key", testCase.key,
				"--constraints", testCase.constraints,
				"--verbose",
			})
			require.NoError(t, err)
			require.Equal(t, `
Note: Constraint aliases: "taskType" -> runtime "TaskQueueType"; "historyTaskType" -> runtime "TaskType".
Note: Constraints not used by this setting are ignored. Use --verbose to inspect the constraint description and configured constrained values.

`, stderr.String())

			var queryConstraints any
			require.NoError(t, yaml.Unmarshal([]byte(testCase.constraints), &queryConstraints))
			expectedConstrainedValues, err := dynamicconfig.MarshalConstrainedValuesYAML(
				dynamicconfig.MakeKey(testCase.key),
				config.Map[dynamicconfig.MakeKey(testCase.key)],
			)
			require.NoError(t, err)
			var constrainedValues any
			require.NoError(t, yaml.Unmarshal(expectedConstrainedValues, &constrainedValues))

			requireYAMLEqual(t, mustMarshalYAML(t, map[string]any{
				"key":                   testCase.key,
				"queryConstraints":      queryConstraints,
				"constraintDescription": testCase.constraintDescription,
				"effectiveValue":        testCase.effectiveValue,
				"constrainedValues":     constrainedValues,
			}), stdout.String())
		})
	}
}

func mustMarshalYAML(t *testing.T, value any) string {
	t.Helper()
	data, err := yaml.Marshal(value)
	require.NoError(t, err)
	return string(data)
}

func requireYAMLEqual(t *testing.T, expected string, actual string) {
	t.Helper()
	var expectedValue any
	require.NoError(t, yaml.Unmarshal([]byte(expected), &expectedValue))
	var actualValue any
	require.NoError(t, yaml.Unmarshal([]byte(actual), &actualValue))
	require.Equal(t, expectedValue, actualValue)
}
