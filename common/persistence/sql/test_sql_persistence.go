package sql

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/tests/testutils"
)

// TestCluster allows executing SQL operations in testing.
type TestCluster struct {
	dbName          string
	schemaDir       string
	cfg             config.SQL
	faultInjection  *config.FaultInjection
	skipSchemaSetup bool
	logger          log.Logger
}

// NewTestCluster returns a new SQL test cluster
func NewTestCluster(
	pluginName string,
	dbName string,
	username string,
	password string,
	host string,
	port int,
	connectAttributes map[string]string,
	schemaDir string,
	faultInjection *config.FaultInjection,
	skipSchemaSetup bool,
	logger log.Logger,
) *TestCluster {
	var result TestCluster
	result.logger = logger
	result.dbName = dbName
	result.schemaDir = schemaDir
	result.cfg = config.SQL{
		User:               username,
		Password:           password,
		ConnectAddr:        fmt.Sprintf("%v:%v", host, port),
		ConnectProtocol:    "tcp",
		PluginName:         pluginName,
		DatabaseName:       dbName,
		TaskScanPartitions: 4,
		ConnectAttributes:  connectAttributes,
	}
	result.faultInjection = faultInjection
	result.skipSchemaSetup = skipSchemaSetup
	return &result
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.dbName
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase() {
	if s.skipSchemaSetup {
		return
	}
	s.createDatabase(s.dbName)

	schemaDir := s.resolveSchemaDir()
	if schemaDir == "" {
		return
	}
	s.loadSchema(s.dbName, path.Join(schemaDir, "temporal", "schema.sql"))
	s.loadSchema(s.dbName, path.Join(schemaDir, "visibility", "schema.sql"))
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	cfg := s.cfg
	return config.Persistence{
		DefaultStore:         "test",
		VisibilityStore:      "test",
		DataStores:           map[string]config.DataStore{"test": {SQL: &cfg, FaultInjection: s.faultInjection}},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	if !s.skipSchemaSetup {
		s.dropDatabase(s.dbName)
	}
}

func (s *TestCluster) createDatabase(dbName string) {
	cfg2 := s.cfg
	// NOTE need to connect with empty name to create new database
	if cfg2.PluginName != "sqlite" {
		cfg2.DatabaseName = ""
	}
	var db sqlplugin.AdminDB
	var err error
	err = backoff.ThrottleRetry(
		func() error {
			db, err = NewSQLAdminDB(sqlplugin.DbKindUnknown, &cfg2, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
			return err
		},
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()
	if err = db.CreateDatabase(dbName); err != nil {
		panic(err)
	}
	s.logger.Info("created database", tag.String("database", dbName))
}

func (s *TestCluster) dropDatabase(dbName string) {
	cfg2 := s.cfg
	if cfg2.PluginName == "sqlite" && dbName != ":memory:" && cfg2.ConnectAttributes["mode"] != "memory" {
		if len(dbName) > 3 { // 3 should mean not ., .., empty, or /
			_ = os.Remove(dbName)
			_ = os.Remove(dbName + "-wal")
			_ = os.Remove(dbName + "-shm")
		}
		return
	}
	// NOTE need to connect with empty name to drop the database
	cfg2.DatabaseName = ""
	db, err := NewSQLAdminDB(sqlplugin.DbKindUnknown, &cfg2, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()
	if err = db.DropDatabase(dbName); err != nil {
		panic(err)
	}
	s.logger.Info("dropped database", tag.String("database", dbName))
}

func (s *TestCluster) loadSchema(dbName, schemaFile string) {
	statements, err := p.LoadAndSplitQuery([]string{schemaFile})
	if err != nil {
		s.logger.Fatal("LoadSchema", tag.Error(err))
	}
	cfg2 := s.cfg
	cfg2.DatabaseName = dbName
	var db sqlplugin.AdminDB
	err = backoff.ThrottleRetry(
		func() error {
			db, err = NewSQLAdminDB(sqlplugin.DbKindUnknown, &cfg2, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
			return err
		},
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()
	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			s.logger.Fatal("LoadSchema", tag.Error(err))
		}
	}
	s.logger.Info("loaded schema", tag.String("database", dbName))
}

func (s *TestCluster) resolveSchemaDir() string {
	if s.schemaDir == "" {
		s.logger.Info("No schema directory provided, skipping schema setup")
		return ""
	}
	schemaDir := s.schemaDir + "/"
	if !strings.HasPrefix(schemaDir, "/") && !strings.HasPrefix(schemaDir, "../") {
		temporalPackageDir := testutils.GetRepoRootDirectory()
		schemaDir = path.Join(temporalPackageDir, schemaDir)
	}
	return schemaDir
}
