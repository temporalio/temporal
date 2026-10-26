package testcore

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership/static"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/primitives"
)

type persistentClusterOptions struct {
	initialize bool
	grpcHosts  map[primitives.ServiceName]static.Hosts
	httpHost   string
}

type persistentDatabase struct {
	persistencetests.PersistenceTestCluster
	initialize bool
}

func (db *persistentDatabase) SetupTestDatabase() {
	if db.initialize {
		db.PersistenceTestCluster.SetupTestDatabase()
	}
}

func (*persistentDatabase) TearDownTestDatabase() {}

// NewPersistentTestCluster keeps a file-backed SQLite database across test guest
// incarnations. Only its first incarnation initializes schema and cluster metadata.
func NewPersistentTestCluster(
	t *testing.T,
	cfg *TestClusterConfig,
	logger log.Logger,
	initialize bool,
	grpcHosts map[primitives.ServiceName]static.Hosts,
	httpHost string,
) (*TestCluster, error) {
	if cfg.Persistence.SQLDBPluginName != "sqlite" || !filepath.IsAbs(cfg.Persistence.DBName) || filepath.Clean(cfg.Persistence.DBName) != cfg.Persistence.DBName || cfg.Persistence.DBName == "/" || cfg.Persistence.ConnectAttributes["mode"] == "memory" {
		return nil, errors.New("persistent test cluster requires file-backed SQLite")
	}
	if !initialize {
		if info, err := os.Stat(cfg.Persistence.DBName); err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("persistent test cluster requires an existing database on reopen")
		}
	} else if _, err := os.Stat(cfg.Persistence.DBName); !os.IsNotExist(err) {
		return nil, errors.New("persistent test cluster initialization requires a new database")
	}
	hosts := make(map[primitives.ServiceName]static.Hosts, 4)
	for _, service := range []primitives.ServiceName{primitives.FrontendService, primitives.HistoryService, primitives.MatchingService, primitives.WorkerService} {
		host := grpcHosts[service]
		if len(host.All) != 1 {
			return nil, fmt.Errorf("persistent test cluster requires one %s address", service)
		}
		if err := validatePersistentAddress(host.All[0]); err != nil {
			return nil, fmt.Errorf("persistent test cluster %s address: %w", service, err)
		}
		hosts[service] = static.Hosts{All: []string{host.All[0]}, Self: host.All[0]}
	}
	if err := validatePersistentAddress(httpHost); err != nil {
		return nil, fmt.Errorf("persistent test cluster HTTP address: %w", err)
	}
	return newClusterWithPersistenceOptions(t, cfg, logger, &defaultPersistenceTestBaseFactory{}, &persistentClusterOptions{
		initialize: initialize, grpcHosts: hosts, httpHost: httpHost,
	})
}

func validatePersistentAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || net.ParseIP(host) == nil {
		return errors.New("address must contain an IP and a stable nonzero port")
	}
	return nil
}
