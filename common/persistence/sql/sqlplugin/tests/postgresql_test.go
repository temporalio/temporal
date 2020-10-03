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

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
)

// TODO merge the initialization with existing persistence setup
const (
	testPostgreSQLUser               = "temporal"
	testPostgreSQLPassword           = "temporal"
	testPostgreSQLConnectionProtocol = "tcp"
	testPostgreSQLDatabaseNamePrefix = "test_"
	testPostgreSQLDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testPostgreSQLSchema = "../../../../../schema/postgresql/temporal/schema.sql"
)

func TestPostgreSQLMatchingTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

// NewPostgreSQLConfig returns a new MySQL config for test
func NewPostgreSQLConfig() *config.SQL {
	return &config.SQL{
		User:     testPostgreSQLUser,
		Password: testPostgreSQLPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetPostgreSQLAddress(),
			strconv.Itoa(environment.GetPostgreSQLPort()),
		),
		ConnectProtocol: testPostgreSQLConnectionProtocol,
		PluginName:      "postgres",
		DatabaseName:    testPostgreSQLDatabaseNamePrefix + shuffle.String(testPostgreSQLDatabaseNameSuffix),
	}
}

func SetupPostgreSQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(&adminCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL database: %v", err))
	}
}

func SetupPostgreSQLSchema(cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testPostgreSQLSchema)
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

func TearDownPostgreSQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(&adminCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL database: %v", err))
	}
}
