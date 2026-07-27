package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCassandraConfigUsesScyllaConnectionEnv(t *testing.T) {
	t.Setenv(testCassandraMaxConnsEnv, "12")
	t.Setenv(testCassandraMaxExcessShardConnectionsEnv, "0")

	cfg := NewCassandraConfig()

	require.Equal(t, 12, cfg.MaxConns)
	require.NotNil(t, cfg.MaxExcessShardConnectionsRate)
	require.Equal(t, float32(0), *cfg.MaxExcessShardConnectionsRate)
}
