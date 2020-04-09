package cassandra

import (
	"os"
	"strings"
	"time"

	"github.com/temporalio/temporal/common/cassandra"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/environment"
)

const (
	testSchemaDir = "schema/cassandra/"
)

// TestCluster allows executing cassandra operations in testing.
type TestCluster struct {
	keyspace  string
	schemaDir string
	cluster   *gocql.ClusterConfig
	session   *gocql.Session
	cfg       config.Cassandra
}

// NewTestCluster returns a new cassandra test cluster
func NewTestCluster(keyspace, username, password, host string, port int, schemaDir string) *TestCluster {
	var result TestCluster
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
		User:     username,
		Password: password,
		Hosts:    host,
		Port:     port,
		MaxConns: 2,
		Keyspace: keyspace,
	}
	return &result
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	cfg := s.cfg
	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {Cassandra: &cfg},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit),
	}
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.keyspace
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase() {
	s.CreateSession()
	s.CreateDatabase()
	schemaDir := s.schemaDir + "/"

	if !strings.HasPrefix(schemaDir, "/") && !strings.HasPrefix(schemaDir, "../") {
		cadencePackageDir, err := getCadencePackageDir()
		if err != nil {
			log.Fatal(err)
		}
		schemaDir = cadencePackageDir + schemaDir
	}

	s.LoadSchema([]string{"schema.cql"}, schemaDir)
	s.LoadVisibilitySchema([]string{"schema.cql"}, schemaDir)
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.DropDatabase()
	s.session.Close()
}

// CreateSession from PersistenceTestCluster interface
func (s *TestCluster) CreateSession() {
	s.cluster = cassandra.NewCassandraCluster(config.Cassandra{
		Hosts:    s.cfg.Hosts,
		Port:     s.cfg.Port,
		User:     s.cfg.User,
		Password: s.cfg.Password,
	})
	s.cluster.Consistency = gocql.Consistency(1)
	s.cluster.Keyspace = "system"
	s.cluster.Timeout = 40 * time.Second
	var err error
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		log.Fatal(`CreateSession`, err)
	}
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	err := CreateCassandraKeyspace(s.session, s.DatabaseName(), 1, true)
	if err != nil {
		log.Fatal(err)
	}

	s.cluster.Keyspace = s.DatabaseName()
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	err := DropCassandraKeyspace(s.session, s.DatabaseName())
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := schemaDir + "/temporal"
	err := loadCassandraSchema(workflowSchemaDir, fileNames, s.cluster.Hosts, s.cluster.Port, s.DatabaseName(), true, nil)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
}

// LoadVisibilitySchema from PersistenceTestCluster interface
func (s *TestCluster) LoadVisibilitySchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := schemaDir + "visibility"
	err := loadCassandraSchema(workflowSchemaDir, fileNames, s.cluster.Hosts, s.cluster.Port, s.DatabaseName(), false, nil)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
}

func getCadencePackageDir() (string, error) {
	cadencePackageDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cadenceIndex := strings.LastIndex(cadencePackageDir, "/temporal/")
	cadencePackageDir = cadencePackageDir[:cadenceIndex+len("/temporal/")]
	if err != nil {
		panic(err)
	}
	return cadencePackageDir, err
}
