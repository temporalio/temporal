package dynamicconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestParseConstraintsJSON(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    string
		expected dynamicconfig.Constraints
	}{
		{name: "empty", input: "", expected: dynamicconfig.Constraints{}},
		{name: "empty object", input: `{}`, expected: dynamicconfig.Constraints{}},
		{
			name:     "namespace",
			input:    `{"namespace":"namespace-a"}`,
			expected: dynamicconfig.Constraints{Namespace: "namespace-a"},
		},
		{
			name:  "all field types",
			input: `{"namespace":"namespace-a","taskQueueType":1,"shardId":2,"taskType":3}`,
			expected: dynamicconfig.Constraints{
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
			actual, err := dynamicconfig.ParseConstraintsJSON(testCase.input)
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
			_, err := dynamicconfig.ParseConstraintsJSON(input)
			require.Error(t, err)
		})
	}
}

func TestParseConstraintsJSONWithFields(t *testing.T) {
	t.Parallel()

	constraints, fields, err := dynamicconfig.ParseConstraintsJSONWithFields(
		`{"taskQueueName":"queue-a","namespace":"namespace-a"}`,
	)
	require.NoError(t, err)
	require.Equal(t, dynamicconfig.Constraints{
		Namespace:     "namespace-a",
		TaskQueueName: "queue-a",
	}, constraints)
	require.Equal(t, []string{"namespace", "taskQueueName"}, fields)

	constraints, fields, err = dynamicconfig.ParseConstraintsJSONWithFields(`{"taskQueueName":""}`)
	require.NoError(t, err)
	require.Equal(t, dynamicconfig.Constraints{}, constraints)
	require.Equal(t, []string{"taskQueueName"}, fields)
}
