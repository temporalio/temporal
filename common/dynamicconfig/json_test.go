package dynamicconfig

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

func TestParseConstraintsJSON(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    string
		expected Constraints
	}{
		{name: "empty", input: "", expected: Constraints{}},
		{name: "empty object", input: `{}`, expected: Constraints{}},
		{
			name:     "namespace",
			input:    `{"namespace":"namespace-a"}`,
			expected: Constraints{Namespace: "namespace-a"},
		},
		{
			name:  "all field types",
			input: `{"namespace":"namespace-a","taskQueueType":1,"shardId":2,"taskType":3}`,
			expected: Constraints{
				Namespace:     "namespace-a",
				TaskQueueType: 1,
				ShardID:       2,
				TaskType:      3,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			actual, err := ParseConstraintsJSON(testCase.input)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestParseConstraintsJSONInvalid(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		`{"namespace":`,
		`{"unknown":"value"}`,
		`{"shardId":"one"}`,
		`{} {}`,
		`null`,
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := ParseConstraintsJSON(input)
			require.Error(t, err)
		})
	}
}

func TestMarshalValueDurationRoundTripsThroughDurationConverter(t *testing.T) {
	encodedValue, err := MarshalValue(90 * time.Second)
	require.NoError(t, err)

	var serializedDuration string
	require.NoError(t, json.Unmarshal(encodedValue, &serializedDuration))
	parsedDuration, err := convertDuration(serializedDuration)
	require.NoError(t, err)
	require.Equal(t, 90*time.Second, parsedDuration)
}

func TestMarshalConfigValueMapPreservesSupportedValuesAndAllConstraints(t *testing.T) {
	key := MakeKey("test.all-types")
	encodedValues, err := MarshalConfigValueMap(ConfigValueMap{
		key: {
			{Value: -7},
			{Value: int64(9_007_199_254_740_993)},
			{Value: 1.25},
			{Value: true},
			{Value: "value"},
			{Value: map[string]any{
				"duration": "3s",
				"list":     []any{"item", 2, "500ms"},
			}},
			{
				Constraints: Constraints{
					Namespace:     "namespace-a",
					NamespaceID:   "namespace-id-a",
					TaskQueueName: "queue-a",
					Destination:   "cluster-a",
					ChasmTaskType: "chasm-task-a",
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
					ShardID:       12,
					TaskType:      enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
				},
				Value: 1500 * time.Millisecond,
			},
		},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"test.all-types": [
			{"constraints": {}, "value": -7},
			{"constraints": {}, "value": 9007199254740993},
			{"constraints": {}, "value": 1.25},
			{"constraints": {}, "value": true},
			{"constraints": {}, "value": "value"},
			{
				"constraints": {},
				"value": {
					"duration": "3s",
					"list": ["item", 2, "500ms"]
				}
			},
			{
				"constraints": {
					"namespace": "namespace-a",
					"namespaceId": "namespace-id-a",
					"taskQueueName": "queue-a",
					"destination": "cluster-a",
					"chasmTaskType": "chasm-task-a",
					"taskQueueType": 2,
					"shardId": 12,
					"taskType": 4
				},
				"value": "1.5s"
			}
		]
	}`, string(encodedValues))
	require.Contains(t, string(encodedValues), `"value":9007199254740993`)
}

func TestMarshalConfigValueMapPreservesRealConfiguredValueShapes(t *testing.T) {
	meteringKey := MakeKey("metering.storage.ResetV3TimeByNamespaceIDAndArchetype")
	computeProvidersKey := MakeKey("workercontroller.compute_providers.enabled")
	encodedValues, err := MarshalConfigValueMap(ConfigValueMap{
		MaxLocalParentWorkflowVerificationDuration.Key(): {
			{Value: "24h"},
		},
		HistoryPersistenceGlobalMaxQPS.Key(): {
			{Value: 480000},
		},
		StandbyTaskMissingEventsDiscardDelay.Key(): {
			{Value: "1h"},
			{Constraints: Constraints{TaskType: enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK}, Value: "15m"},
			{Constraints: Constraints{TaskType: enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK}, Value: "15m"},
			{Constraints: Constraints{TaskType: enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER}, Value: "15m"},
		},
		MatchingEnableWorkerDeploymentVersionDemotionSignal.Key(): {
			{Value: true},
		},
		meteringKey: {
			{Value: map[string]any{
				"activity.activity":   "2026-04-23T20:00:00Z",
				"scheduler.scheduler": "2026-04-23T20:00:00Z",
			}},
		},
		computeProvidersKey: {
			{Value: []any{"aws-lambda"}},
		},
	})
	require.NoError(t, err)

	type dumpedValue struct {
		Constraints Constraints     `json:"constraints"`
		Value       json.RawMessage `json:"value"`
	}
	var valuesByKey map[string][]dumpedValue
	require.NoError(t, json.Unmarshal(encodedValues, &valuesByKey))
	require.Len(t, valuesByKey, 6)

	require.JSONEq(
		t,
		`"24h"`,
		string(valuesByKey[MaxLocalParentWorkflowVerificationDuration.Key().String()][0].Value),
	)
	require.JSONEq(
		t,
		`480000`,
		string(valuesByKey[HistoryPersistenceGlobalMaxQPS.Key().String()][0].Value),
	)
	standbyValues := valuesByKey[StandbyTaskMissingEventsDiscardDelay.Key().String()]
	require.Len(t, standbyValues, 4)
	require.Empty(t, standbyValues[0].Constraints)
	require.JSONEq(t, `"1h"`, string(standbyValues[0].Value))
	for i, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
	} {
		require.Equal(t, Constraints{TaskType: taskType}, standbyValues[i+1].Constraints)
		require.JSONEq(t, `"15m"`, string(standbyValues[i+1].Value))
	}
	require.JSONEq(
		t,
		`true`,
		string(valuesByKey[MatchingEnableWorkerDeploymentVersionDemotionSignal.Key().String()][0].Value),
	)
	require.JSONEq(
		t,
		`{
			"activity.activity": "2026-04-23T20:00:00Z",
			"scheduler.scheduler": "2026-04-23T20:00:00Z"
		}`,
		string(valuesByKey[meteringKey.String()][0].Value),
	)
	require.JSONEq(t, `["aws-lambda"]`, string(valuesByKey[computeProvidersKey.String()][0].Value))
}

func TestMarshalConfigValueMapReturnsKeyAndIndexForUnsupportedValues(t *testing.T) {
	cyclicMap := make(map[string]any)
	cyclicMap["self"] = cyclicMap
	cyclicList := make([]any, 1)
	cyclicList[0] = cyclicList

	testCases := []struct {
		name          string
		value         any
		expectedError string
	}{
		{name: "channel", value: make(chan struct{}), expectedError: "json: unsupported type: chan struct {}"},
		{name: "function", value: func() {}, expectedError: "json: unsupported type: func()"},
		{name: "complex number", value: complex(1, 2), expectedError: "json: unsupported type: complex128"},
		{name: "NaN", value: math.NaN(), expectedError: "json: unsupported value: NaN"},
		{name: "positive infinity", value: math.Inf(1), expectedError: "json: unsupported value: +Inf"},
		{name: "cyclic map", value: cyclicMap, expectedError: "encountered a cycle"},
		{name: "cyclic list", value: cyclicList, expectedError: "encountered a cycle"},
		{
			name:          "nested channel",
			value:         map[string]any{"nested": make(chan struct{})},
			expectedError: "json: unsupported type: chan struct {}",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := MarshalConfigValueMap(ConfigValueMap{
				MakeKey("invalid"): {{Value: testCase.value}},
			})
			require.ErrorContains(t, err, `dynamic config key "invalid" constrained value at index 0`)
			require.ErrorContains(t, err, testCase.expectedError)
		})
	}
}
