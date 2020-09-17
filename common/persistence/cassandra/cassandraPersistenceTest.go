// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package cassandra

import (
	"os"
	"path"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cassandra"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/environment"
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
	logger    log.Logger
}

// NewTestCluster returns a new cassandra test cluster
func NewTestCluster(keyspace, username, password, host string, port int, schemaDir string, logger log.Logger) *TestCluster {
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
		ConnectTimeout: 600 * time.Millisecond,
		Keyspace:       keyspace,
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
		temporalPackageDir, err := getTemporalPackageDir()
		if err != nil {
			s.logger.Fatal("Unable to get package dir.", tag.Error(err))
		}
		schemaDir = path.Join(temporalPackageDir, schemaDir)
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
	var err error
	s.cluster, err = cassandra.NewCassandraCluster(config.Cassandra{
		Hosts:    s.cfg.Hosts,
		Port:     s.cfg.Port,
		User:     s.cfg.User,
		Password: s.cfg.Password,
	})
	if err != nil {
		s.logger.Fatal("CreateSession", tag.Error(err))
	}
	s.cluster.Consistency = gocql.Consistency(1)
	s.cluster.Keyspace = "system"
	s.cluster.Timeout = 40 * time.Second
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		s.logger.Fatal("CreateSession", tag.Error(err))
	}
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	err := CreateCassandraKeyspace(s.session, s.DatabaseName(), 1, true, s.logger)
	if err != nil {
		s.logger.Fatal("CreateCassandraKeyspace", tag.Error(err))
	}

	s.cluster.Keyspace = s.DatabaseName()
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	err := DropCassandraKeyspace(s.session, s.DatabaseName(), s.logger)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		s.logger.Fatal("DropCassandraKeyspace", tag.Error(err))
	}
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := path.Join(schemaDir, "temporal")
	err := loadCassandraSchema(workflowSchemaDir, fileNames, s.cluster.Hosts, s.cluster.Port, s.DatabaseName(), true, nil)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		s.logger.Fatal("loadCassandraSchema", tag.Error(err))
	}
}

// LoadVisibilitySchema from PersistenceTestCluster interface
func (s *TestCluster) LoadVisibilitySchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := path.Join(schemaDir, "visibility")
	err := loadCassandraSchema(workflowSchemaDir, fileNames, s.cluster.Hosts, s.cluster.Port, s.DatabaseName(), false, nil)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		s.logger.Fatal("loadCassandraVisibilitySchema", tag.Error(err))
	}
}

func getTemporalPackageDir() (string, error) {
	var err error
	temporalPackageDir := os.Getenv("TEMPORAL_ROOT")
	if temporalPackageDir == "" {
		temporalPackageDir, err = os.Getwd()
		if err != nil {
			panic(err)
		}
		temporalIndex := strings.LastIndex(temporalPackageDir, "/temporal/")
		if temporalIndex == -1 {
			panic("Unable to find repo path. Use env var TEMPORAL_ROOT or clone the repo into folder named 'temporal'")
		}
		temporalPackageDir = temporalPackageDir[:temporalIndex+len("/temporal/")]
		if err != nil {
			panic(err)
		}
	}
	return temporalPackageDir, err
}
