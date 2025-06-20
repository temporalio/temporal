package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryBuildCatalog_Ok(t *testing.T) {
	t.Parallel()

	r := registry{}
	r.register(newMetricDefinition("foo", WithDescription("foo description")))
	r.register(newMetricDefinition("bar", WithDescription("bar description")))
	c, err := r.buildCatalog()
	require.Nil(t, err)
	require.Equal(t, 2, len(c))
	require.Equal(t, "foo description", c["foo"].description)
	require.Equal(t, "bar description", c["bar"].description)
}

func TestRegistryBuildCatalog_ErrMetricAlreadyExists(t *testing.T) {
	t.Parallel()

	b := registry{}
	b.register(newMetricDefinition("foo", WithDescription("foo description")))
	b.register(newMetricDefinition("foo", WithDescription("bar description")))
	_, err := b.buildCatalog()
	assert.ErrorIs(t, err, errMetricAlreadyExists)
	assert.ErrorContains(t, err, "foo")
}
