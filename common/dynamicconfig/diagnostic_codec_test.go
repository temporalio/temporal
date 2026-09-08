package dynamicconfig

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
)

func TestParseAliasedConstraintsJSON(t *testing.T) {
	t.Parallel()

	validCases := []struct {
		name     string
		input    string
		expected Constraints
	}{
		{name: "empty", input: "", expected: Constraints{}},
		{name: "empty mapping", input: "{}", expected: Constraints{}},
		{
			name: "all fields",
			input: `{
  "namespace": "namespace-a",
  "namespaceId": "namespace-id-a",
  "taskQueueName": "queue-a",
  "destination": "cluster-a",
  "chasmTaskType": "chasm-task-a",
  "taskType": "Activity",
  "shardId": 12,
  "historyTaskType": "TransferWorkflowTask"
}`,
			expected: Constraints{
				Namespace:     "namespace-a",
				NamespaceID:   "namespace-id-a",
				TaskQueueName: "queue-a",
				Destination:   "cluster-a",
				ChasmTaskType: "chasm-task-a",
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
				ShardID:       12,
				TaskType:      enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
			},
		},
		{
			name:  "numeric enum aliases",
			input: `{"taskType": 1, "historyTaskType": 4}`,
			expected: Constraints{
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				TaskType:      enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
			},
		},
	}
	for _, testCase := range validCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			actual, err := ParseAliasedConstraintsJSON(testCase.input)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, actual)
		})
	}

	for _, testCase := range []struct {
		name  string
		input string
	}{
		{name: "malformed JSON", input: `{"namespace": [`},
		{name: "YAML is rejected", input: `namespace: value`},
		{name: "unknown field", input: `{"unknown": "value"}`},
		{name: "invalid value", input: `{"shardId": "one"}`},
		{name: "multiple objects", input: `{} {}`},
		{name: "not a mapping", input: `null`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseAliasedConstraintsJSON(testCase.input)
			require.Error(t, err)
		})
	}
}

func TestMarshalValueYAML(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		duration time.Duration
		yaml     string
	}{
		{name: "days", duration: 48 * time.Hour, yaml: "48h0m0s\n"},
		{name: "hours", duration: time.Hour, yaml: "1h0m0s\n"},
		{name: "minutes", duration: time.Minute, yaml: "1m0s\n"},
		{name: "seconds", duration: 10 * time.Second, yaml: "10s\n"},
		{name: "milliseconds", duration: time.Millisecond, yaml: "1ms\n"},
		{name: "microseconds", duration: time.Microsecond, yaml: "1µs\n"},
		{name: "nanoseconds", duration: time.Nanosecond, yaml: "1ns\n"},
		{name: "combined", duration: 10*time.Hour + 4*time.Minute + 5*time.Second, yaml: "10h4m5s\n"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			encodedValue, err := MarshalValueYAML(testCase.duration)
			require.NoError(t, err)
			require.Equal(t, testCase.yaml, string(encodedValue))

			parsedValue, err := timestamp.ParseDurationDefaultSeconds(strings.TrimSpace(string(encodedValue)))
			require.NoError(t, err)
			require.Equal(t, testCase.duration, parsedValue)
		})
	}
}

func TestMarshalConfigValueMapYAML(t *testing.T) {
	t.Run("constraints round trip", func(t *testing.T) {
		key := MakeKey("test.all-constraints")
		expectedConstraints := Constraints{
			Namespace:     "namespace-a",
			NamespaceID:   "namespace-id-a",
			TaskQueueName: "queue-a",
			Destination:   "cluster-a",
			ChasmTaskType: "chasm-task-a",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			ShardID:       12,
			TaskType:      enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		}

		encodedValues, err := MarshalConfigValueMapYAML(ConfigValueMap{
			key: {{Constraints: expectedConstraints, Value: 1500 * time.Millisecond}},
		})
		require.NoError(t, err)
		require.Contains(t, string(encodedValues), "taskType: 2")
		require.Contains(t, string(encodedValues), "historyTaskType: 4")

		loadedValues := LoadYamlFile(encodedValues)
		require.Empty(t, loadedValues.Errors)
		require.Equal(t, expectedConstraints, loadedValues.Map[key][0].Constraints)
		require.Equal(t, "1.5s", loadedValues.Map[key][0].Value)
	})

	t.Run("nested duration round trip", func(t *testing.T) {
		key := FrontendPersistenceDynamicRateLimitingParams.Key()
		encodedValues, err := MarshalConfigValueMapYAML(ConfigValueMap{
			key: {{Value: DefaultDynamicRateLimitingParams}},
		})
		require.NoError(t, err)
		require.Contains(t, string(encodedValues), "refreshinterval: 10s")

		loadedValues := LoadYamlFile(encodedValues)
		require.Empty(t, loadedValues.Errors)
		require.Empty(t, loadedValues.Warnings)

		collection := NewCollection(
			StaticClient{key: loadedValues.Map[key]},
			log.NewNoopLogger(),
		)
		require.Equal(t, DefaultDynamicRateLimitingParams, FrontendPersistenceDynamicRateLimitingParams.Get(collection)())
	})

	t.Run("repository config round trip", func(t *testing.T) {
		contents, err := os.ReadFile("config/testConfig.yaml")
		require.NoError(t, err)

		original := LoadYamlFile(contents)
		require.Empty(t, original.Errors)
		require.NotEmpty(t, original.Map)

		dumped, err := MarshalConfigValueMapYAML(original.Map)
		require.NoError(t, err)
		reloaded := LoadYamlFile(dumped)
		require.Empty(t, reloaded.Errors)
		require.Len(t, reloaded.Map, len(original.Map))
		for key, expectedValues := range original.Map {
			require.Equal(t, expectedValues, reloaded.Map[key], key.String())
		}
	})

	t.Run("unsupported value", func(t *testing.T) {
		_, err := MarshalConfigValueMapYAML(ConfigValueMap{
			MakeKey("invalid"): {{Value: make(chan struct{})}},
		})
		require.ErrorContains(t, err, `dynamic config key "invalid" constrained value at index 0`)
		require.ErrorContains(t, err, "cannot marshal type: chan struct {}")
	})
}
