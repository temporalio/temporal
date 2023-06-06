package metrics

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/internal"
)

func TestGetPrometheusRegistry(t *testing.T) {
	t.Parallel()

	registry, err := BuildPrometheusRegistry()
	require.NoError(t, err)

	var b bytes.Buffer
	err = internal.PrintMetricsPage(registry, &b)
	require.NoError(t, err)
	assert.Contains(
		t,
		b.String(),
		"# HELP wf_too_many_pending_child_workflows The number of pending child workflows exceeds",
	)
}
