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
	"io/ioutil"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/environment"
)

// TestCluster allows executing cassandra operations in testing.
type TestCluster struct {
	dbName    string
	schemaDir string
	cfg       config.SQL
}

// NewTestCluster returns a new SQL test cluster
func NewTestCluster(pluginName, dbName, username, password, host string, port int, schemaDir string) *TestCluster {
	var result TestCluster
	result.dbName = dbName
	if port == 0 {
		port = environment.GetMySQLPort()
	}
	if schemaDir == "" {
		panic("must provide schema dir")
	}
	result.schemaDir = schemaDir
	result.cfg = config.SQL{
		User:            username,
		Password:        password,
		ConnectAddr:     fmt.Sprintf("%v:%v", host, port),
		ConnectProtocol: "tcp",
		PluginName:      pluginName,
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
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit),
	}
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.DropDatabase()
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	cfg2 := s.cfg
	// NOTE need to connect with empty name to create new database
	cfg2.DatabaseName = ""
	db, err := NewSQLAdminDB(&cfg2)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()
	err = db.CreateDatabase(s.cfg.DatabaseName)
	if err != nil {
		panic(err)
	}
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	cfg2 := s.cfg
	// NOTE need to connect with empty name to drop the database
	cfg2.DatabaseName = ""
	db, err := NewSQLAdminDB(&cfg2)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()
	err = db.DropDatabase(s.cfg.DatabaseName)
	if err != nil {
		panic(err)
	}
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := schemaDir + "/cadence"
	err := s.loadDatabaseSchema(workflowSchemaDir, fileNames, true)
	if err != nil {
		log.Fatal(err)
	}
}

// LoadVisibilitySchema from PersistenceTestCluster interface
func (s *TestCluster) LoadVisibilitySchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := schemaDir + "/visibility"
	err := s.loadDatabaseSchema(workflowSchemaDir, fileNames, true)
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
	return cadencePackageDir, err
}

// loadDatabaseSchema loads the schema from the given .sql files on this database
func (s *TestCluster) loadDatabaseSchema(dir string, fileNames []string, override bool) (err error) {
	db, err := NewSQLAdminDB(&s.cfg)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

	for _, file := range fileNames {
		// This is only used in tests. Excluding it from security scanners
		// #nosec
		content, err := ioutil.ReadFile(dir + "/" + file)
		if err != nil {
			return fmt.Errorf("error reading contents of file %v:%v", file, err.Error())
		}
		err = db.Exec(string(content))
		if err != nil {
			return fmt.Errorf("error loading schema from %v: %v", file, err.Error())
		}
	}
	return nil
}
