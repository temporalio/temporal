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

package clitest

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/tools/sql"
)

type (
	// VersionTestSuite defines a test suite
	VersionTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite

		host                         string
		port                         string
		pluginName                   string
		executionSchemaFileLocation  string
		visibilitySchemaFileLocation string
	}
)

// NewVersionTestSuite returns a test suite
func NewVersionTestSuite(
	host string,
	port string,
	pluginName string,
	executionSchemaFileLocation string,
	visibilitySchemaFileLocation string,
) *VersionTestSuite {
	return &VersionTestSuite{
		host:                         host,
		port:                         port,
		pluginName:                   pluginName,
		executionSchemaFileLocation:  executionSchemaFileLocation,
		visibilitySchemaFileLocation: visibilitySchemaFileLocation,
	}
}

// SetupTest setups test suite
func (s *VersionTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// TestVerifyCompatibleVersion test
func (s *VersionTestSuite) TestVerifyCompatibleVersion() {
	database := "temporal_ver_test_" + persistencetests.GenerateRandomDBName(3)
	visDatabase := "temporal_vis_ver_test_" + persistencetests.GenerateRandomDBName(3)

	defer s.createDatabase(database)()
	defer s.createDatabase(visDatabase)()
	err := sql.RunTool([]string{
		"./tool",
		"-ep", s.host,
		"-p", s.port,
		"-u", testUser,
		"-pw", testPassword,
		"-db", database,
		"-pl", s.pluginName,
		"-q",
		"setup-schema",
		"-f", s.executionSchemaFileLocation,
		"-version", "10.0",
		"-o",
	})
	s.NoError(err)
	err = sql.RunTool([]string{
		"./tool",
		"-ep", s.host,
		"-p", s.port,
		"-u", testUser,
		"-pw", testPassword,
		"-db", visDatabase,
		"-pl", s.pluginName,
		"-q",
		"setup-schema",
		"-f", s.visibilitySchemaFileLocation,
		"-version", "10.0",
		"-o",
	})
	s.NoError(err)

	defaultCfg := config.SQL{
		ConnectAddr:  fmt.Sprintf("%v:%v", s.host, s.port),
		User:         testUser,
		Password:     testPassword,
		PluginName:   s.pluginName,
		DatabaseName: database,
	}
	visibilityCfg := defaultCfg
	visibilityCfg.DatabaseName = visDatabase
	cfg := config.Persistence{
		DefaultStore:    "default",
		VisibilityStore: "visibility",
		DataStores: map[string]config.DataStore{
			"default":    {SQL: &defaultCfg},
			"visibility": {SQL: &visibilityCfg},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit),
	}
	s.NoError(sql.VerifyCompatibleVersion(cfg))
}

// TestCheckCompatibleVersion test
func (s *VersionTestSuite) TestCheckCompatibleVersion() {
	flags := []struct {
		expectedVersion string
		actualVersion   string
		errStr          string
		expectedFail    bool
	}{
		{"2.0", "1.0", "version mismatch", false},
		{"1.0", "1.0", "", false},
		{"1.0", "2.0", "", false},
		{"1.0", "abc", "unable to read DB schema version", false},
	}
	for _, flag := range flags {
		s.runCheckCompatibleVersion(flag.expectedVersion, flag.actualVersion, flag.errStr, flag.expectedFail)
	}
}

func (s *VersionTestSuite) createDatabase(database string) func() {
	connection, err := newTestConn("", s.host, s.port, s.pluginName)
	s.NoError(err)
	err = connection.CreateDatabase(database)
	s.NoError(err)
	return func() {
		s.NoError(connection.DropDatabase(database))
		connection.Close()
	}
}

func (s *VersionTestSuite) runCheckCompatibleVersion(
	expected string, actual string, errStr string, expectedFail bool,
) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	database := fmt.Sprintf("version_test_%v", r.Int63())
	defer s.createDatabase(database)()

	dir := "check_version"
	tmpDir, err := ioutil.TempDir("", dir)
	s.NoError(err)
	defer os.RemoveAll(tmpDir)

	subdir := tmpDir + "/" + database
	s.NoError(os.Mkdir(subdir, os.FileMode(0744)))

	s.createSchemaForVersion(subdir, actual)
	if expected != actual {
		s.createSchemaForVersion(subdir, expected)
	}

	sqlFile := subdir + "/v" + actual + "/tmp.sql"
	s.NoError(sql.RunTool([]string{
		"./tool",
		"-ep", s.host,
		"-p", s.port,
		"-u", testUser,
		"-pw", testPassword,
		"-db", database,
		"-pl", s.pluginName,
		"-q",
		"setup-schema",
		"-f", sqlFile,
		"-version", actual,
		"-o",
	}))
	if expectedFail {
		os.RemoveAll(subdir + "/v" + actual)
	}

	cfg := config.SQL{
		ConnectAddr:  fmt.Sprintf("%v:%v", s.host, s.port),
		User:         testUser,
		Password:     testPassword,
		PluginName:   s.pluginName,
		DatabaseName: database,
	}
	err = sql.CheckCompatibleVersion(cfg, expected)
	if len(errStr) > 0 {
		s.Error(err)
		s.Contains(err.Error(), errStr)
	} else {
		s.NoError(err)
	}
}

func (s *VersionTestSuite) createSchemaForVersion(subdir string, v string) {
	vDir := subdir + "/v" + v
	s.NoError(os.Mkdir(vDir, os.FileMode(0744)))
	cqlFile := vDir + "/tmp.sql"
	s.NoError(ioutil.WriteFile(cqlFile, []byte{}, os.FileMode(0644)))
}
