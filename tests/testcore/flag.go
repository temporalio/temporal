package testcore

import (
	"context"
	"flag"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	rpcfaultinjection "go.temporal.io/server/common/rpc/faultinjection"
)

// cliFlags contains the feature flags for functional tests.
// Fault injection flags share a -fault prefix and are namespaced by layer: -faultPersistence
// for the persistence layer and -faultRPC* for the gRPC layer.
var cliFlags struct {
	persistenceType   string
	persistenceDriver string

	faultPersistence string

	faultPersistenceLatency         string
	faultPersistenceLatencyRate     float64
	faultPersistenceLatencySeed     int64
	faultPersistenceLatencyMeanMs   float64
	faultPersistenceLatencyStddevMs float64
	faultPersistenceLatencyMaxMs    float64

	faultRPC              string
	faultRPCRate          float64
	faultRPCErrorRate     float64
	faultRPCSeed          int64
	faultRPCLatencyMeanMs float64
	faultRPCLatencyStddev float64
	faultRPCLatencyMaxMs  float64
}

func init() {
	flag.StringVar(&cliFlags.persistenceType, "persistenceType", "sql", "type of persistence - [nosql or sql]")
	flag.StringVar(&cliFlags.persistenceDriver, "persistenceDriver", "sqlite", "driver of nosql/sql - [cassandra, mysql8, postgres12, sqlite]")

	flag.StringVar(&cliFlags.faultPersistence, "faultPersistence", "", "enable global persistence fault injection (error faults)")

	flag.StringVar(&cliFlags.faultPersistenceLatency, "faultPersistenceLatency", "", "enable the persistence Latency fault on the test cluster")
	flag.Float64Var(&cliFlags.faultPersistenceLatencyRate, "faultPersistenceRate", 1.0, "probability (0..1) that a persistence call receives an injected fault")
	flag.Int64Var(&cliFlags.faultPersistenceLatencySeed, "faultPersistenceSeed", 0, "seed for the persistence fault injector RNG (0 = time-based)")
	flag.Float64Var(&cliFlags.faultPersistenceLatencyMeanMs, "faultPersistenceLatencyMeanMs", 25, "mean latency in ms for the persistence Latency fault")
	flag.Float64Var(&cliFlags.faultPersistenceLatencyStddevMs, "faultPersistenceLatencyStddevMs", 15, "stddev latency in ms for the persistence Latency fault")
	flag.Float64Var(&cliFlags.faultPersistenceLatencyMaxMs, "faultPersistenceLatencyMaxMs", 250, "max latency clamp in ms for the persistence Latency fault (0 = unbounded)")

	flag.StringVar(&cliFlags.faultRPC, "faultRPC", "", "enable the gRPC Latency fault on the test cluster")
	flag.Float64Var(&cliFlags.faultRPCRate, "faultRPCRate", 1.0, "probability (0..1) that an RPC receives a Latency fault")
	flag.Float64Var(&cliFlags.faultRPCErrorRate, "faultRPCErrorRate", 0, "probability (0..1) that an RPC fails with a transient retryable error (independent of -faultRPC)")
	flag.Int64Var(&cliFlags.faultRPCSeed, "faultRPCSeed", 0, "seed for the RPC fault injector RNG (0 = time-based)")
	flag.Float64Var(&cliFlags.faultRPCLatencyMeanMs, "faultRPCLatencyMeanMs", 25, "mean latency in ms for the Latency fault")
	flag.Float64Var(&cliFlags.faultRPCLatencyStddev, "faultRPCLatencyStddevMs", 15, "stddev latency in ms for the Latency fault")
	flag.Float64Var(&cliFlags.faultRPCLatencyMaxMs, "faultRPCLatencyMaxMs", 250, "max latency clamp in ms for the Latency fault (0 = unbounded)")
}

// RPCFaultInjectionConfig returns the gRPC fault injection config derived from CLI flags.
// The Latency fault is enabled by -faultRPC; the transient Error fault is enabled independently
// by -faultRPCErrorRate. The result is disabled (zero value) unless at least one is set.
func RPCFaultInjectionConfig() rpcfaultinjection.Config {
	cfg := rpcfaultinjection.Config{
		Seed:      cliFlags.faultRPCSeed,
		ErrorRate: cliFlags.faultRPCErrorRate,
	}
	if cliFlags.faultRPC != "" {
		cfg.Rate = cliFlags.faultRPCRate
		cfg.Latency = rpcfaultinjection.LatencyConfig{
			MeanMs:   cliFlags.faultRPCLatencyMeanMs,
			StddevMs: cliFlags.faultRPCLatencyStddev,
			MaxMs:    cliFlags.faultRPCLatencyMaxMs,
		}
	}
	return cfg
}

// PersistenceLatencyInjector returns a runtime persistence fault injector that sleeps for a
// sampled latency before each store call, or nil if the persistence Latency fault is disabled.
// It reuses the rpcfaultinjection sampler so the gRPC and persistence Latency faults share the
// same distribution machinery.
func PersistenceLatencyInjector() config.FaultInjector {
	if cliFlags.faultPersistenceLatency == "" {
		return nil
	}
	inj := rpcfaultinjection.New(rpcfaultinjection.Config{
		Rate: cliFlags.faultPersistenceLatencyRate,
		Seed: cliFlags.faultPersistenceLatencySeed,
		Latency: rpcfaultinjection.LatencyConfig{
			MeanMs:   cliFlags.faultPersistenceLatencyMeanMs,
			StddevMs: cliFlags.faultPersistenceLatencyStddevMs,
			MaxMs:    cliFlags.faultPersistenceLatencyMaxMs,
		},
	})
	return func(target config.FaultInjectionTarget) error {
		// Background context: the injected latency is unconditional, mimicking a slow store.
		_ = inj.Delay(context.Background(), string(target.Store)+"/"+target.Method)
		return nil
	}
}

func UseSQLVisibility() bool {
	switch cliFlags.persistenceDriver {
	case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
		return true
	// If the main storage is Cassandra, Elasticsearch is used for visibility.
	default:
		return false
	}
}

func UseCassandraPersistence() bool {
	return cliFlags.persistenceType == config.StoreTypeNoSQL && cliFlags.persistenceDriver == "cassandra"
}
