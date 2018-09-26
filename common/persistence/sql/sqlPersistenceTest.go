// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sql

import (
	"github.com/jmoiron/sqlx"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/persistence-tests"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
	testPort                 = 3306
	testUser                 = "uber"
	testPassword             = "uber"
	testSchemaDir            = "schema/mysql/"
)

// TestCluster allows executing cassandra operations in testing.
type TestCluster struct {
	Options *persistencetests.TestBaseOptions
	dbName  string
	db      *sqlx.DB
}

// InitTestSuite initializes test suite to use cassandra
func InitTestSuite(tb *persistencetests.TestBase) {
	options := &persistencetests.TestBaseOptions{
		SchemaDir:          testSchemaDir,
		DBHost:             testWorkflowClusterHosts,
		DBPort:             testPort,
		DBUser:             testUser,
		DBPassword:         testPassword,
		DropDatabase:       true,
		EnableGlobalDomain: false,
		Datacenter:         "foo",
	}
	InitTestSuiteWithOptions(tb, options)
}

// InitTestSuiteWithOptions initializes test suite to use cassandra given options
func InitTestSuiteWithOptions(tb *persistencetests.TestBase, options *persistencetests.TestBaseOptions) {
	InitTestSuiteWithMetadata(tb, options, cluster.GetTestClusterMetadata(
		options.EnableGlobalDomain,
		options.IsMasterCluster,
	))
}

// InitTestSuiteWithMetadata initializes test suite to use cassandra given options and metadata
func InitTestSuiteWithMetadata(tb *persistencetests.TestBase, options *persistencetests.TestBaseOptions, metadata cluster.Metadata) {
	if metadata == nil {
		panic("nil metadata")
	}
	if options.SchemaDir == "" {
		options.SchemaDir = testSchemaDir
	}
	log := bark.NewLoggerFromLogrus(log.New())
	tb.PersistenceTestCluster = &TestCluster{
		Options: options,
	}
	tb.ClusterMetadata = metadata
	currentClusterName := tb.ClusterMetadata.GetCurrentClusterName()
	// Setup Workflow keyspace and deploy schema for tests
	tb.PersistenceTestCluster.SetupTestDatabase(options)
	shardID := 0
	databaseName := tb.PersistenceTestCluster.DatabaseName()
	var err error
	tb.ShardMgr, err = NewShardPersistence(options.DBHost, options.DBPort, options.DBUser,
		options.DBPassword, databaseName, currentClusterName, log)
	if err != nil {
		log.Fatal(err)
	}
	tb.ExecutionMgrFactory, err = NewExecutionManagerFactory(options.DBHost, options.DBPort,
		options.DBUser, options.DBPassword, databaseName, options.Datacenter, log)
	if err != nil {
		log.Fatal(err)
	}
	// Create an ExecutionManager for the shard for use in unit tests
	tb.ExecutionManager, err = tb.ExecutionMgrFactory.CreateExecutionManager(shardID)
	if err != nil {
		log.Fatal(err)
	}
	tb.TaskMgr, err = NewTaskPersistence(options.DBHost, options.DBPort, options.DBUser, options.DBPassword,
		databaseName, log)
	if err != nil {
		log.Fatal(err)
	}
	tb.HistoryMgr, err = NewHistoryPersistence(options.DBHost, options.DBPort, options.DBUser,
		options.DBPassword, databaseName, log)
	if err != nil {
		log.Fatal(err)
	}
	tb.MetadataManager, err = NewMetadataPersistenceV2(options.DBHost, options.DBPort, options.DBUser,
		options.DBPassword, databaseName, currentClusterName, log)
	if err != nil {
		log.Fatal(err)
	}
	tb.MetadataProxy = tb.MetadataManager
	tb.MetadataManagerV2 = tb.MetadataManager
	//tb.VisibilityMgr, err = NewVisibilityPersistence(options.DBHost, options.DBPort,
	//	options.DBUser, options.DBPassword, options.Datacenter, databaseName, log)
	//if err != nil {
	//	log.Fatal(err)
	//}
	// Create a shard for test
	tb.ReadLevel = 0
	tb.ReplicationReadLevel = 0
	tb.ShardInfo = &p.ShardInfo{
		ShardID:                 shardID,
		RangeID:                 0,
		TransferAckLevel:        0,
		ReplicationAckLevel:     0,
		TimerAckLevel:           time.Time{},
		ClusterTimerAckLevel:    map[string]time.Time{currentClusterName: time.Time{}},
		ClusterTransferAckLevel: map[string]int64{currentClusterName: 0},
	}
	tb.TaskIDGenerator = &persistencetests.TestTransferTaskIDGenerator{}
	err1 := tb.ShardMgr.CreateShard(&p.CreateShardRequest{
		ShardInfo: tb.ShardInfo,
	})
	if err1 != nil {
		log.Fatal(err1)
	}
}

func getCadencePackageDir() (string, error) {
	cadencePackageDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cadenceIndex := strings.LastIndex(cadencePackageDir, "/cadence/")
	cadencePackageDir = cadencePackageDir[:cadenceIndex+len("/cadence/")]
	if err != nil {
		panic(err)
	}
	return cadencePackageDir, err
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.dbName
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase(options *persistencetests.TestBaseOptions) {
	s.dbName = options.DBName
	if s.dbName == "" {
		s.dbName = persistencetests.GenerateRandomDBName(10)
	}

	s.CreateDatabase(options.DropDatabase)
	s.CreateSession(options)
	cadencePackageDir, err := getCadencePackageDir()
	if err != nil {
		log.Fatal(err)
	}
	schemaDir := cadencePackageDir + options.SchemaDir + "/"
	s.LoadSchema([]string{"schema.sql"}, schemaDir)
	// TODO: Visibility
	//s.LoadVisibilitySchema([]string{"schema.sql"}, schemaDir)
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.DropDatabase()
	s.db.Close()
}

// CreateSession from PersistenceTestCluster interface
func (s *TestCluster) CreateSession(options *persistencetests.TestBaseOptions) {
	var err error
	s.db, err = newConnection(options.DBHost, options.DBPort, options.DBUser, options.DBPassword, s.dbName)
	if err != nil {
		log.WithField(logging.TagErr, err).Fatal(`CreateSession`)
	}
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase(overwrite bool) {
	err := createDatabase(s.Options.DBHost, s.Options.DBPort, s.Options.DBUser, s.Options.DBPassword, s.dbName, overwrite)
	if err != nil {
		log.Fatal(err)
	}
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	err := dropDatabase(s.db, s.dbName)
	if err != nil {
		log.Fatal(err)
	}
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := schemaDir + "/cadence"
	err := loadDatabaseSchema(workflowSchemaDir, fileNames, s.db, true)
	if err != nil {
		log.Fatal(err)
	}
}

// LoadVisibilitySchema from PersistenceTestCluster interface
func (s *TestCluster) LoadVisibilitySchema(fileNames []string, schemaDir string) {
	log.Fatal("LoadVisibilitySchema is not supportedy by SQL yet")
}
