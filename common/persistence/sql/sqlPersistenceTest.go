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
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"

	log "github.com/sirupsen/logrus"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/service/config"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
	testPort                 = 3306
	testUser                 = "uber"
	testPassword             = "uber"
	testSchemaDir            = "schema/mysql/v56"
)

// TestCluster allows executing cassandra operations in testing.
type TestCluster struct {
	dbName    string
	schemaDir string
	db        *sqlx.DB
	cfg       config.SQL
}

// NewTestCluster returns a new SQL test cluster
func NewTestCluster(port int, dbName string, schemaDir string, driverName string) *TestCluster {
	if schemaDir == "" {
		schemaDir = testSchemaDir
	}
	if port == 0 {
		port = testPort
	}
	if driverName == "" {
		driverName = defaultDriverName
	}
	var result TestCluster
	result.dbName = dbName
	result.schemaDir = schemaDir
	result.cfg = config.SQL{
		User:            testUser,
		Password:        testPassword,
		ConnectAddr:     fmt.Sprintf("%v:%v", testWorkflowClusterHosts, port),
		ConnectProtocol: "tcp",
		DriverName:      driverName,
		DatabaseName:    dbName,
		NumShards:       4,
	}
	return &result
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.dbName
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase() {
	s.CreateDatabase()
	s.CreateSession()

	schemaDir := s.schemaDir + "/"
	if !strings.HasPrefix(schemaDir, "/") && !strings.HasPrefix(schemaDir, "../") {
		cadencePackageDir, err := getCadencePackageDir()
		if err != nil {
			log.Fatal(err)
		}
		schemaDir = cadencePackageDir + schemaDir
	}
	s.LoadSchema([]string{"schema.sql"}, schemaDir)
	s.LoadVisibilitySchema([]string{"schema.sql"}, schemaDir)
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	cfg := s.cfg
	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {SQL: &cfg},
		},
	}
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.DropDatabase()
	s.db.Close()
}

// CreateSession from PersistenceTestCluster interface
func (s *TestCluster) CreateSession() {
	var err error
	s.db, err = newConnection(s.cfg)
	if err != nil {
		log.WithField(logging.TagErr, err).Fatal(`CreateSession`)
	}
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	err := createDatabase(s.cfg.DriverName, s.cfg.ConnectAddr, testUser, testPassword, s.dbName, true)
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
	workflowSchemaDir := schemaDir + "/visibility"
	err := loadDatabaseSchema(workflowSchemaDir, fileNames, s.db, true)
	if err != nil {
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
