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
	"path"
	"strings"
	"time"

	"github.com/gocql/gocql"
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
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tests/testutils"
)

const (
	testSchemaDir = "schema/cassandra/"
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
	s.logger.Debug("created session", tag.NewStringTag("keyspace", keyspace))
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	err := CreateCassandraKeyspace(s.session, s.DatabaseName(), 1, true, s.logger)
	if err != nil {
		s.logger.Fatal("CreateCassandraKeyspace", tag.Error(err))
	}
	s.logger.Info("created database", tag.NewStringTag("database", s.DatabaseName()))
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	err := DropCassandraKeyspace(s.session, s.DatabaseName(), s.logger)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		s.logger.Fatal("DropCassandraKeyspace", tag.Error(err))
	}
	s.logger.Info("dropped database", tag.NewStringTag("database", s.DatabaseName()))
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(schemaFile string) {
	statements, err := p.LoadAndSplitQuery([]string{schemaFile})
	if err != nil {
		s.logger.Fatal("LoadSchema", tag.Error(err))
	}
	for _, stmt := range statements {
		if err = s.session.Query(stmt).Exec(); err != nil {
			s.logger.Fatal("LoadSchema", tag.Error(err))
		}
	}
	s.logger.Info("loaded schema")
}

func (s *TestCluster) GetSession() commongocql.Session {
	return s.session
}
