package cassandra

import (
	"context"
	"path"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	cassandraschema "go.temporal.io/server/schema/cassandra"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tests/testutils"
)

const (
	testSchemaDir = "schema/cassandra/"

	createSchemaVersionTableCQL = `CREATE TABLE IF NOT EXISTS schema_version(keyspace_name text PRIMARY KEY, ` +
		`creation_time timestamp, ` +
		`curr_version text, ` +
		`min_compatible_version text);`

	createSchemaUpdateHistoryTableCQL = `CREATE TABLE IF NOT EXISTS schema_update_history(` +
		`year int, ` +
		`month int, ` +
		`update_time timestamp, ` +
		`description text, ` +
		`manifest_md5 text, ` +
		`new_version text, ` +
		`old_version text, ` +
		`PRIMARY KEY ((year, month), update_time));`

	writeSchemaVersionCQL = `INSERT into schema_version(keyspace_name, creation_time, curr_version, min_compatible_version) VALUES (?,?,?,?)`

	writeSchemaUpdateHistoryCQL = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(?,?,?,?,?,?,?)`
)

// TestCluster allows executing cassandra operations in testing.
type TestCluster struct {
	keyspace       string
	schemaDir      string
	session        commongocql.Session
	cfg            config.Cassandra
	faultInjection *config.FaultInjection
	logger         log.Logger
}

// NewTestCluster returns a new cassandra test cluster
func NewTestCluster(keyspace, username, password, host string, port int, schemaDir string, faultInjection *config.FaultInjection, logger log.Logger) *TestCluster {
	var result TestCluster
	result.logger = logger
	result.keyspace = keyspace
	if port == 0 {
		port = environment.GetCassandraPort()
	}
	if schemaDir == "" {
		schemaDir = testSchemaDir
	}
	if host == "" {
		host = environment.GetCassandraAddress()
	}
	result.schemaDir = schemaDir
	result.cfg = config.Cassandra{
		User:           username,
		Password:       password,
		Hosts:          host,
		Port:           port,
		MaxConns:       2,
		ConnectTimeout: 30 * time.Second * debug.TimeoutMultiplier,
		Keyspace:       keyspace,
	}
	result.faultInjection = faultInjection
	return &result
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	cfg := s.cfg
	return config.Persistence{
		DefaultStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {Cassandra: &cfg, FaultInjection: s.faultInjection},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.keyspace
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase() {
	s.CreateSession("system")
	s.CreateDatabase()
	s.CreateSession(s.DatabaseName())
	schemaDir := s.schemaDir + "/"

	if !strings.HasPrefix(schemaDir, "/") && !strings.HasPrefix(schemaDir, "../") {
		temporalPackageDir := testutils.GetRepoRootDirectory()
		schemaDir = path.Join(temporalPackageDir, schemaDir)
	}

	s.LoadSchema(path.Join(schemaDir, "temporal", "schema.cql"))
	s.loadSchemaVersion()
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.DropDatabase()
	s.session.Close()
}

// CreateSession from PersistenceTestCluster interface
func (s *TestCluster) CreateSession(
	keyspace string,
) {
	if s.session != nil {
		s.session.Close()
	}

	var err error
	op := func() error {
		session, err := commongocql.NewSession(
			func() (*gocql.ClusterConfig, error) {
				return commongocql.NewCassandraCluster(
					config.Cassandra{
						Hosts:    s.cfg.Hosts,
						Port:     s.cfg.Port,
						User:     s.cfg.User,
						Password: s.cfg.Password,
						Keyspace: keyspace,
						Consistency: &config.CassandraStoreConsistency{
							Default: &config.CassandraConsistencySettings{
								Consistency: "ONE",
							},
						},
						ConnectTimeout: s.cfg.ConnectTimeout,
					},
					resolver.NewNoopResolver(),
				)
			},
			log.NewNoopLogger(),
			metrics.NoopMetricsHandler,
		)
		if err == nil {
			s.session = session
		}
		return err
	}
	err = backoff.ThrottleRetry(
		op,
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		s.logger.Fatal("CreateSession", tag.Error(err))
	}
	s.logger.Debug("created session", tag.String("keyspace", keyspace))
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	err := CreateCassandraKeyspace(s.session, s.DatabaseName(), 1, true, s.logger)
	if err != nil {
		s.logger.Fatal("CreateCassandraKeyspace", tag.Error(err))
	}
	s.logger.Info("created database", tag.String("database", s.DatabaseName()))
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	err := DropCassandraKeyspace(s.session, s.DatabaseName(), s.logger)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		s.logger.Fatal("DropCassandraKeyspace", tag.Error(err))
	}
	s.logger.Info("dropped database", tag.String("database", s.DatabaseName()))
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(schemaFile string) {
	statements, err := p.LoadAndSplitQuery([]string{schemaFile})
	if err != nil {
		s.logger.Fatal("LoadSchema", tag.Error(err))
	}
	for _, stmt := range statements {
		if err = s.session.Query(stmt).Exec(context.Background()); err != nil {
			s.logger.Fatal("LoadSchema", tag.Error(err))
		}
	}
	s.logger.Info("loaded schema")
}

func (s *TestCluster) loadSchemaVersion() {
	s.createSchemaVersionTables()
	s.updateSchemaVersion(cassandraschema.Version, cassandraschema.Version)
	s.writeSchemaUpdateLog("0", cassandraschema.Version, "", "initial version")
	s.logger.Info("loaded schema version", tag.String("version", cassandraschema.Version))
}

func (s *TestCluster) createSchemaVersionTables() {
	s.execSchemaVersionQuery(createSchemaVersionTableCQL)
	s.execSchemaVersionQuery(createSchemaUpdateHistoryTableCQL)
}

func (s *TestCluster) updateSchemaVersion(newVersion string, minCompatibleVersion string) {
	now := time.Now().UTC()
	s.execSchemaVersionQuery(
		writeSchemaVersionCQL,
		s.keyspace, now, newVersion, minCompatibleVersion)
}

func (s *TestCluster) writeSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, description string) {
	now := time.Now().UTC()
	s.execSchemaVersionQuery(
		writeSchemaUpdateHistoryCQL,
		now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, description)
}

func (s *TestCluster) execSchemaVersionQuery(stmt string, args ...any) {
	if err := s.session.Query(stmt, args...).Exec(); err != nil {
		s.logger.Fatal("loadSchemaVersion", tag.Error(err))
	}
}

func (s *TestCluster) GetSession() commongocql.Session {
	return s.session
}
