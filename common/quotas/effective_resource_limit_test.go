package quotas_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/quotastest"
)

func TestEffectiveResourceLimit(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5.0, quotas.CalculateEffectiveResourceLimit(
		quotastest.NewFakeInstanceCounter(4),
		quotas.Limits{
			InstanceLimit: 10,
			ClusterLimit:  20,
		},
	))
}

func TestEffectiveResourceLimit_NoPerClusterLimit(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10.0, quotas.CalculateEffectiveResourceLimit(
		quotastest.NewFakeInstanceCounter(4),
		quotas.Limits{
			InstanceLimit: 10,
			ClusterLimit:  0,
		},
	))
}

func TestEffectiveResourceLimit_NoHosts(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10.0, quotas.CalculateEffectiveResourceLimit(
		quotastest.NewFakeInstanceCounter(0),
		quotas.Limits{
			InstanceLimit: 10,
			ClusterLimit:  20,
		},
	))
}

func TestEffectiveResourceLimit_NilInstanceCounter(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10.0, quotas.CalculateEffectiveResourceLimit(nil, quotas.Limits{
		InstanceLimit: 10,
		ClusterLimit:  20,
	}))
}
