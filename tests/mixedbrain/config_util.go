package mixedbrain

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/freeport"
	"gopkg.in/yaml.v3"
)

var (
	// Fixed ports for CI to avoid exceeding the cluster_membership.rpc_port
	// SMALLINT column (max 32767). Linux ephemeral ports start at 32768.
	portSetA = newPortSet(7230)
	portSetB = newPortSet(8240)
)

type portSet struct {
	FrontendGRPC       int
	FrontendMembership int
	FrontendHTTP       int
	HistoryGRPC        int
	HistoryMembership  int
	MatchingGRPC       int
	MatchingMembership int
	WorkerGRPC         int
	WorkerMembership   int
}

func newPortSet(base int) portSet {
	return portSet{
		FrontendGRPC:       base,
		FrontendMembership: base + 1,
		FrontendHTTP:       base + 2,
		HistoryGRPC:        base + 3,
		HistoryMembership:  base + 4,
		MatchingGRPC:       base + 5,
		MatchingMembership: base + 6,
		WorkerGRPC:         base + 7,
		WorkerMembership:   base + 8,
	}
}

func newRandPortSet() portSet {
	return portSet{
		FrontendGRPC:       freeport.MustGetFreePort(),
		FrontendMembership: freeport.MustGetFreePort(),
		FrontendHTTP:       freeport.MustGetFreePort(),
		HistoryGRPC:        freeport.MustGetFreePort(),
		HistoryMembership:  freeport.MustGetFreePort(),
		MatchingGRPC:       freeport.MustGetFreePort(),
		MatchingMembership: freeport.MustGetFreePort(),
		WorkerGRPC:         freeport.MustGetFreePort(),
		WorkerMembership:   freeport.MustGetFreePort(),
	}
}

func (p portSet) frontendAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", p.FrontendGRPC)
}

func (p portSet) membershipPorts() []int {
	return []int{
		p.FrontendMembership,
		p.HistoryMembership,
		p.MatchingMembership,
		p.WorkerMembership,
	}
}

func generateConfig(t *testing.T, tmpDir string, ports portSet, activePorts portSet) string {
	t.Helper()

	configDir := filepath.Join(tmpDir, fmt.Sprintf("config-%d", ports.FrontendGRPC))
	require.NoError(t, os.MkdirAll(configDir, 0755))

	dynConfigPath := filepath.Join(sourceRoot(), "config", "dynamicconfig", "development-sql.yaml")

	driver := os.Getenv("PERSISTENCE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}

	var dataStores map[string]config.DataStore
	switch driver {
	case "sqlite":
		newStore := func(dbName string) config.DataStore {
			return config.DataStore{
				SQL: &config.SQL{
					PluginName:      "sqlite",
					DatabaseName:    filepath.Join(tmpDir, dbName),
					ConnectAddr:     "localhost",
					ConnectProtocol: "tcp",
					ConnectAttributes: map[string]string{
						"cache":        "private",
						"setup":        "true",
						"journal_mode": "wal",
						"synchronous":  "2",
						"busy_timeout": "10000",
					},
					MaxConns:     1,
					MaxIdleConns: 1,
				},
			}
		}
		dataStores = map[string]config.DataStore{
			"default":    newStore("temporal_default.db"),
			"visibility": newStore("temporal_visibility.db"),
		}
	case "postgres12", "postgres12_pgx":
		newStore := func(dbName string) config.DataStore {
			return config.DataStore{
				SQL: &config.SQL{
					PluginName:      driver,
					DatabaseName:    dbName,
					ConnectAddr:     "127.0.0.1:5432",
					ConnectProtocol: "tcp",
					User:            "temporal",
					Password:        "temporal",
					MaxConns:        20,
					MaxIdleConns:    20,
					MaxConnLifetime: time.Hour,
				},
			}
		}
		dataStores = map[string]config.DataStore{
			"default":    newStore("temporal"),
			"visibility": newStore("temporal_visibility"),
		}
	default:
		t.Fatalf("unsupported persistence driver: %s", driver)
	}

	cfg := config.Config{
		Log: log.Config{
			Stdout: true,
			Level:  "info",
		},
		Persistence: config.Persistence{
			DefaultStore:     "default",
			VisibilityStore:  "visibility",
			NumHistoryShards: 4,
			DataStores:       dataStores,
		},
		Global: config.Global{
			Membership: config.Membership{
				MaxJoinDuration:  30 * time.Second,
				BroadcastAddress: "127.0.0.1",
			},
		},
		Services: map[string]config.Service{
			string(primitives.FrontendService): {
				RPC: config.RPC{
					GRPCPort:        ports.FrontendGRPC,
					MembershipPort:  ports.FrontendMembership,
					BindOnLocalHost: true,
					HTTPPort:        ports.FrontendHTTP,
				}},
			string(primitives.MatchingService): {
				RPC: config.RPC{
					GRPCPort:        ports.MatchingGRPC,
					MembershipPort:  ports.MatchingMembership,
					BindOnLocalHost: true,
				}},
			string(primitives.HistoryService): {
				RPC: config.RPC{
					GRPCPort:        ports.HistoryGRPC,
					MembershipPort:  ports.HistoryMembership,
					BindOnLocalHost: true,
				}},
			string(primitives.WorkerService): {
				RPC: config.RPC{
					GRPCPort:        ports.WorkerGRPC,
					MembershipPort:  ports.WorkerMembership,
					BindOnLocalHost: true,
				}},
		},
		ClusterMetadata: &cluster.Config{
			FailoverVersionIncrement: 10,
			MasterClusterName:        "active",
			CurrentClusterName:       "active",
			ClusterInformation: map[string]cluster.ClusterInformation{
				"active": {
					Enabled:                true,
					InitialFailoverVersion: 1,
					RPCAddress:             fmt.Sprintf("localhost:%d", activePorts.FrontendGRPC),
					HTTPAddress:            fmt.Sprintf("localhost:%d", activePorts.FrontendHTTP),
				},
			},
		},
		DynamicConfigClient: &dynamicconfig.FileBasedClientConfig{
			Filepath:     dynConfigPath,
			PollInterval: 10 * time.Second,
		},
	}

	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(configDir, "development.yaml"), data, 0644)
	require.NoError(t, err)
	return configDir
}
