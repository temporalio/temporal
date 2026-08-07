package mixedbrain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricSchemaProblems(t *testing.T) {
	t.Parallel()

	current, err := parseMetricSchemas(strings.NewReader(`
# TYPE task_requests counter
task_requests{archetype="workflow",namespace="test",operation="Transfer",task_priority="high",task_type="Transfer"} 1
# TYPE new_metric counter
new_metric{outcome="success"} 1
`))
	require.NoError(t, err)
	release, err := parseMetricSchemas(strings.NewReader(`
# TYPE task_requests counter
task_requests{namespace="test",operation="Transfer",task_priority="high",task_type="Transfer"} 1
`))
	require.NoError(t, err)

	require.Equal(t, []string{
		`metric "task_requests" has incompatible labels: current={archetype, namespace, operation, task_priority, task_type} release={namespace, operation, task_priority, task_type}`,
	}, metricSchemaProblems(current, release))
}

func TestMetricSchemaProblems_InconsistentWithinVersion(t *testing.T) {
	t.Parallel()

	current, err := parseMetricSchemas(strings.NewReader(`
# TYPE task_requests counter
task_requests{namespace="test",operation="Transfer"} 1
task_requests{namespace="test",operation="Transfer",task_priority="high"} 1
`))
	require.NoError(t, err)

	require.Equal(t, []string{
		`current metric "task_requests" has inconsistent label sets: {namespace, operation, task_priority}, {namespace, operation}`,
	}, metricSchemaProblems(current, nil))
}

func TestMetricSchemaProblems_Compatible(t *testing.T) {
	t.Parallel()

	current, err := parseMetricSchemas(strings.NewReader(`
# TYPE task_requests counter
task_requests{namespace="current",operation="Transfer"} 1
# TYPE current_only counter
current_only 1
`))
	require.NoError(t, err)
	release, err := parseMetricSchemas(strings.NewReader(`
# TYPE task_requests counter
task_requests{namespace="release",operation="Transfer"} 1
# TYPE release_only counter
release_only{source="release"} 1
`))
	require.NoError(t, err)

	require.Empty(t, metricSchemaProblems(current, release))
}

func TestMetricSchemaProblems_AllowedExistingAddition(t *testing.T) {
	t.Parallel()

	current, err := parseMetricSchemas(strings.NewReader(`
# TYPE mutable_state_size gauge
mutable_state_size{archetype="workflow",namespace="test",operation="Update"} 1
`))
	require.NoError(t, err)
	release, err := parseMetricSchemas(strings.NewReader(`
# TYPE mutable_state_size gauge
mutable_state_size{namespace="test",operation="Update"} 1
`))
	require.NoError(t, err)

	require.Empty(t, metricSchemaProblems(current, release))
}

func TestMetricSchemaProblems_AllowedExistingAdditionDoesNotHideNewLabel(t *testing.T) {
	t.Parallel()

	current, err := parseMetricSchemas(strings.NewReader(`
# TYPE mutable_state_size gauge
mutable_state_size{archetype="workflow",namespace="test",operation="Update",new_label="value"} 1
`))
	require.NoError(t, err)
	release, err := parseMetricSchemas(strings.NewReader(`
# TYPE mutable_state_size gauge
mutable_state_size{namespace="test",operation="Update"} 1
`))
	require.NoError(t, err)

	require.Equal(t, []string{
		`metric "mutable_state_size" has incompatible labels: current={archetype, namespace, new_label, operation} release={namespace, operation}`,
	}, metricSchemaProblems(current, release))
}
