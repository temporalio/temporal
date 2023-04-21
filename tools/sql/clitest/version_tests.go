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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
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
	s.NoError(persistencesql.VerifyCompatibleVersion(cfg, resolver.NewNoopResolver()))
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
