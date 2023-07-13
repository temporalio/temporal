package quotas_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/quotas"
)

type memberCounter struct {
	numHosts int
}

func (c memberCounter) MemberCount() int {
	return c.numHosts
}

func TestEffectiveResourceLimit(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5.0, quotas.CalculateEffectiveResourceLimit(memberCounter{4}, quotas.Limits{
		InstanceLimit: 10,
		ClusterLimit:  20,
	}))
}

func TestEffectiveResourceLimit_NoPerClusterLimit(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10.0, quotas.CalculateEffectiveResourceLimit(memberCounter{numHosts: 4}, quotas.Limits{
		InstanceLimit: 10,
		ClusterLimit:  0,
	}))
}

func TestEffectiveResourceLimit_NoHosts(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10.0, quotas.CalculateEffectiveResourceLimit(memberCounter{0}, quotas.Limits{
		InstanceLimit: 10,
		ClusterLimit:  20,
	}))
}

func TestEffectiveResourceLimit_NilMemberCounter(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10.0, quotas.CalculateEffectiveResourceLimit(nil, quotas.Limits{
		InstanceLimit: 10,
		ClusterLimit:  20,
	}))
}
