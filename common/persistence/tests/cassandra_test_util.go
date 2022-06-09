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

package tests

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/blang/semver/v4"
	"go.uber.org/zap/zaptest"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"
)

const (
	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testCassandraExecutionSchema  = "../../../schema/cassandra/temporal/schema.cql"
	testCassandraVisibilitySchema = "../../../schema/cassandra/visibility/schema.cql"
)

// TODO merge the initialization with existing persistence setup
const (
	testCassandraClusterName = "temporal_cassandra_cluster"

	testCassandraUser               = "temporal"
	testCassandraPassword           = "temporal"
	testCassandraDatabaseNamePrefix = "test_"
	testCassandraDatabaseNameSuffix = "temporal_persistence"
)

type (
	CassandraTestData struct {
		Cfg     *config.Cassandra
		Factory *cassandra.Factory
		Logger  log.Logger
	}
)

func setUpCassandraTest(t *testing.T) (CassandraTestData, func()) {
	var testData CassandraTestData
	testData.Cfg = NewCassandraConfig()
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	SetUpCassandraDatabase(testData.Cfg, testData.Logger)
	SetUpCassandraSchema(testData.Cfg, testData.Logger)

	testData.Factory = cassandra.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		testData.Logger,
	)

	tearDown := func() {
		testData.Factory.Close()
		TearDownCassandraKeyspace(testData.Cfg)
	}

	return testData, tearDown
}

func SetUpCassandraDatabase(cfg *config.Cassandra, logger log.Logger) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := gocql.NewSession(adminCfg, resolver.NewNoopResolver(), logger)
	if err != nil {
		panic(fmt.Sprintf("unable to create Cassandra session: %v", err))
	}
	defer session.Close()

	if err := cassandra.CreateCassandraKeyspace(
		session,
		cfg.Keyspace,
		1,
		true,
		log.NewNoopLogger(),
	); err != nil {
		panic(fmt.Sprintf("unable to create Cassandra keyspace: %v", err))
	}
}

func SetUpCassandraSchema(cfg *config.Cassandra, logger log.Logger) {
	ApplySchemaUpdate(cfg, testCassandraExecutionSchema, logger)
	ApplySchemaUpdate(cfg, testCassandraVisibilitySchema, logger)
}

func ApplySchemaUpdate(cfg *config.Cassandra, schemaFile string, logger log.Logger) {
	session, err := gocql.NewSession(*cfg, resolver.NewNoopResolver(), logger)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	schemaPath, err := filepath.Abs(schemaFile)
	if err != nil {
		panic(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		panic(err)
	}

	for _, stmt := range statements {
		if err = session.Query(stmt).Exec(); err != nil {
			logger.Error(fmt.Sprintf("Unable to execute statement from file: %s\n  %s", schemaFile, stmt))
			panic(err)
		}
	}
}

func TearDownCassandraKeyspace(cfg *config.Cassandra) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := gocql.NewSession(adminCfg, resolver.NewNoopResolver(), log.NewNoopLogger())
	if err != nil {
		panic(fmt.Sprintf("unable to create Cassandra session: %v", err))
	}
	defer session.Close()

	if err := cassandra.DropCassandraKeyspace(
		session,
		cfg.Keyspace,
		log.NewNoopLogger(),
	); err != nil {
		panic(fmt.Sprintf("unable to drop Cassandra keyspace: %v", err))
	}
}

// GetSchemaFiles takes a root directory which contains subdirectories whose names are semantic versions and returns
// the .cql files within. E.g.: //schema/cassandra/temporal/versioned
// Subdirectories are ordered by semantic version, but files within the same subdirectory are in arbitrary order.
// All .cql files are returned regardless of whether they are named in manifest.json.
func GetSchemaFiles(schemaDir string, logger log.Logger) []string {
	var retVal []string

	versionDirPath := path.Join(schemaDir, "versioned")
	subDirs, err := os.ReadDir(versionDirPath)
	if err != nil {
		panic(err)
	}

	versionDirNames := make([]string, 0, len(subDirs))
	for _, subDir := range subDirs {
		if !subDir.IsDir() {
			logger.Warn(fmt.Sprintf("Skipping non-directory file: '%s'", subDir.Name()))
			continue
		}
		if _, ve := semver.ParseTolerant(subDir.Name()); ve != nil {
			logger.Warn(fmt.Sprintf("Skipping directory which is not a valid semver: '%s'", subDir.Name()))
		}
		versionDirNames = append(versionDirNames, subDir.Name())
	}

	sort.Slice(versionDirNames, func(i, j int) bool {
		vLeft, err := semver.ParseTolerant(versionDirNames[i])
		if err != nil {
			panic(err) // Logic error
		}
		vRight, err := semver.ParseTolerant(versionDirNames[j])
		if err != nil {
			panic(err) // Logic error
		}
		return vLeft.Compare(vRight) < 0
	})

	for _, dir := range versionDirNames {
		vDirPath := path.Join(versionDirPath, dir)
		files, err := os.ReadDir(vDirPath)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			if !strings.HasSuffix(file.Name(), ".cql") {
				continue
			}
			retVal = append(retVal, path.Join(vDirPath, file.Name()))
		}
	}

	return retVal
}

// NewCassandraConfig returns a new Cassandra config for test
func NewCassandraConfig() *config.Cassandra {
	return &config.Cassandra{
		User:     testCassandraUser,
		Password: testCassandraPassword,
		Hosts:    environment.GetCassandraAddress(),
		Port:     environment.GetCassandraPort(),
		Keyspace: testCassandraDatabaseNamePrefix + shuffle.String(testCassandraDatabaseNameSuffix),
	}
}
