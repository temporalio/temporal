package tests

import (
	"fmt"
	"io/ioutil"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
)

// TODO merge the initialization with existing persistence setup
const (
	testMySQLUser               = "temporal"
	testMySQLPassword           = "temporal"
	testMySQLConnectionProtocol = "tcp"
	testMySQLDatabaseNamePrefix = "test_"
	testMySQLDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testMySQLSchema = "../../../../../schema/mysql/v57/temporal/schema.sql"
)

func TestMySQLMatchingTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

// NewMySQLConfig returns a new MySQL config for test
func NewMySQLConfig() *config.SQL {
	return &config.SQL{
		User:     testMySQLUser,
		Password: testMySQLPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetMySQLAddress(),
			strconv.Itoa(environment.GetMySQLPort()),
		),
		ConnectProtocol: testMySQLConnectionProtocol,
		PluginName:      "mysql",
		DatabaseName:    testMySQLDatabaseNamePrefix + shuffle.String(testMySQLDatabaseNameSuffix),
	}
}

func SetupMySQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(&adminCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL database: %v", err))
	}
}

func SetupMySQLSchema(cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testMySQLSchema)
	if err != nil {
		panic(err)
	}

	content, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		panic(err)
	}
	err = db.Exec(string(content))
	if err != nil {
		panic(err)
	}
}

func TearDownMySQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(&adminCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL database: %v", err))
	}
}
