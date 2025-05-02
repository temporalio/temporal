package tests

import (
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/temporal/environment"
	"go.uber.org/zap/zaptest"
)

// TODO merge the initialization with existing persistence setup
const (
	testPostgreSQLClusterName = "temporal_postgresql_cluster"

	testPostgreSQLUser               = "temporal"
	testPostgreSQLPassword           = "temporal"
	testPostgreSQLConnectionProtocol = "tcp"
	testPostgreSQLDatabaseNamePrefix = "test_"
	testPostgreSQLDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testPostgreSQLExecutionSchema  = "../../../schema/postgresql/v12/temporal/schema.sql"
	testPostgreSQLVisibilitySchema = "../../../schema/postgresql/v12/visibility/schema.sql"
)

type (
	PostgreSQLTestData struct {
		Cfg     *config.SQL
		Factory *sql.Factory
		Logger  log.Logger
		Metrics *metricstest.Capture
	}
)

func setUpPostgreSQLTest(t *testing.T, pluginName string) (PostgreSQLTestData, func()) {
	var testData PostgreSQLTestData
	testData.Cfg = NewPostgreSQLConfig(pluginName)
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	mh := metricstest.NewCaptureHandler()
	testData.Metrics = mh.StartCapture()
	SetupPostgreSQLDatabase(t, testData.Cfg)
	SetupPostgreSQLSchema(t, testData.Cfg)

	testData.Factory = sql.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testPostgreSQLClusterName,
		testData.Logger,
		mh,
	)

	tearDown := func() {
		testData.Factory.Close()
		mh.StopCapture(testData.Metrics)
		TearDownPostgreSQLDatabase(t, testData.Cfg)
	}

	return testData, tearDown
}

// NewPostgreSQLConfig returns a new MySQL config for test
func NewPostgreSQLConfig(pluginName string) *config.SQL {
	return &config.SQL{
		User:     testPostgreSQLUser,
		Password: testPostgreSQLPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetPostgreSQLAddress(),
			strconv.Itoa(environment.GetPostgreSQLPort()),
		),
		ConnectProtocol: testPostgreSQLConnectionProtocol,
		PluginName:      pluginName,
		DatabaseName:    testPostgreSQLDatabaseNamePrefix + shuffle.String(testPostgreSQLDatabaseNameSuffix),
	}
}

func SetupPostgreSQLDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create PostgreSQL admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to create PostgreSQL database: %v", err)
	}
}

func SetupPostgreSQLSchema(t *testing.T, cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create PostgreSQL admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testPostgreSQLExecutionSchema)
	if err != nil {
		t.Fatal(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	schemaPath, err = filepath.Abs(testPostgreSQLVisibilitySchema)
	if err != nil {
		t.Fatal(err)
	}

	statements, err = p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
}

func TearDownPostgreSQLDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create PostgreSQL admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to drop PostgreSQL database: %v", err)
	}
}
