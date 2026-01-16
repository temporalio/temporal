package mongodb_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/mongodb"
)

func TestFactoryConnectionFailure(t *testing.T) {
	cfg := config.MongoDB{
		Hosts:          []string{"invalid-host:99999"},
		DatabaseName:   "test",
		MaxConns:       5,
		ConnectTimeout: 2 * time.Second,
	}

	logger := log.NewTestLogger()
	_, err := mongodb.NewFactory(cfg, "test-cluster", logger, metrics.NoopMetricsHandler)
	require.Error(t, err)
}

func TestFactoryReplicaSet(t *testing.T) {
	cfg := config.MongoDB{
		Hosts:          []string{"localhost:27017", "localhost:27018", "localhost:27019"},
		DatabaseName:   "temporal_test",
		ReplicaSet:     "rs0",
		MaxConns:       10,
		ConnectTimeout: 10 * time.Second,
	}

	logger := log.NewTestLogger()
	factory, err := mongodb.NewFactory(cfg, "test-cluster", logger, metrics.NoopMetricsHandler)
	if err != nil {
		t.Logf("Replica set test skipped (containers not running): %v", err)
		return
	}

	require.NoError(t, err)
	require.NotNil(t, factory)
	defer factory.Close()
}
