package dynamicconfig

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
)

func TestParseConstraintsYAML(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    string
		expected Constraints
	}{
		{name: "empty", input: "", expected: Constraints{}},
		{name: "empty mapping", input: "{}", expected: Constraints{}},
		{
			name:     "JSON syntax",
			input:    `{"namespace":"namespace-a"}`,
			expected: Constraints{Namespace: "namespace-a"},
		},
		{
			name: "all fields",
			input: `namespace: namespace-a
namespaceId: namespace-id-a
taskQueueName: queue-a
destination: cluster-a
chasmTaskType: chasm-task-a
taskType: 2
shardId: 12
historyTaskType: 4
`,
			expected: Constraints{
				Namespace:     "namespace-a",
				NamespaceID:   "namespace-id-a",
				TaskQueueName: "queue-a",
				Destination:   "cluster-a",
				ChasmTaskType: "chasm-task-a",
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
				ShardID:       12,
				TaskType:      enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			actual, err := ParseConstraintsYAML(testCase.input)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestParseConstraintsYAMLInvalid(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		`namespace: [`,
		`unknown: value`,
		`shardId: one`,
		"{}\n---\n{}",
		`null`,
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := ParseConstraintsYAML(input)
			require.Error(t, err)
		})
	}
}

func TestMarshalValueYAMLDuration(t *testing.T) {
	encodedValue, err := MarshalValueYAML(90 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "1m30s\n", string(encodedValue))
}

func TestMarshalConfigValueMapYAMLRoundTripsConstraints(t *testing.T) {
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
	require.Contains(t, string(encodedValues), "value: 1.5s")

	loadedValues := LoadYamlFile(encodedValues)
	require.Empty(t, loadedValues.Errors)
	require.Equal(t, expectedConstraints, loadedValues.Map[key][0].Constraints)
	require.Equal(t, "1.5s", loadedValues.Map[key][0].Value)
}

func TestMarshalConfigValueMapYAMLRoundTripsTypedValueWithNestedDuration(t *testing.T) {
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
}

func TestMarshalConfigValueMapYAMLRoundTripsRepositoryConfig(t *testing.T) {
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
}

func TestMarshalConfigValueMapYAMLReturnsKeyAndIndexForUnsupportedValue(t *testing.T) {
	_, err := MarshalConfigValueMapYAML(ConfigValueMap{
		MakeKey("invalid"): {{Value: make(chan struct{})}},
	})
	require.ErrorContains(t, err, `dynamic config key "invalid" constrained value at index 0`)
	require.ErrorContains(t, err, "cannot marshal type: chan struct {}")
}
