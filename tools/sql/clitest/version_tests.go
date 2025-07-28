package clitest

import (
	"fmt"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/primitives"
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
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
	s.NoError(persistencesql.VerifyCompatibleVersion(cfg, resolver.NewNoopResolver(), log.NewNoopLogger()))
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
