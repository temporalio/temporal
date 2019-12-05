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

package cassandra

import (
	"os"
	"strings"
	"time"

	"github.com/uber/cadence/common/cassandra"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/environment"
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
	workflowSchemaDir := schemaDir + "/cadence"
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
	cadenceIndex := strings.LastIndex(cadencePackageDir, "/cadence/")
	cadencePackageDir = cadencePackageDir[:cadenceIndex+len("/cadence/")]
	if err != nil {
		panic(err)
	}
	return cadencePackageDir, err
}
