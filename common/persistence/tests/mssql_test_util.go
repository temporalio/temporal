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
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/temporal/environment"
	"go.uber.org/zap/zaptest"
)

// TODO merge the initialization with existing persistence setup
const (
	testMSSQLClusterName = "temporal_postgresql_cluster"

	testMSSQLUser               = "sa"
	testMSSQLPassword           = "Temporal123!"
	testMSSQLConnectionProtocol = "tcp"
	testMSSQLDatabaseNamePrefix = "test_"
	testMSSQLDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testMSSQLExecutionSchema  = "../../../schema/mssql/v2019/temporal/schema.sql"
	testMSSQLVisibilitySchema = "../../../schema/mssql/v2019/visibility/schema.sql"
)

type (
	MSSQLTestData struct {
		Cfg     *config.SQL
		Factory *sql.Factory
		Logger  log.Logger
		Metrics *metricstest.Capture
	}
)

func setUpMSSQLTest(t *testing.T, pluginName string, connectAttrs map[string]string) (MSSQLTestData, func()) {
	var testData MSSQLTestData
	testData.Cfg = NewMSSQLConfig(pluginName, connectAttrs)
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	mh := metricstest.NewCaptureHandler()
	testData.Metrics = mh.StartCapture()
	SetupMSSQLDatabase(t, testData.Cfg)
	SetupMSSQLSchema(t, testData.Cfg)

	testData.Factory = sql.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testMSSQLClusterName,
		testData.Logger,
		mh,
		serialization.NewSerializer(),
	)

	tearDown := func() {
		testData.Factory.Close()
		mh.StopCapture(testData.Metrics)
		TearDownMSSQLDatabase(t, testData.Cfg)
	}

	return testData, tearDown
}

// NewMSSQLConfig returns a new MySQL config for test
func NewMSSQLConfig(pluginName string, connectAttrs map[string]string) *config.SQL {
	return &config.SQL{
		User:     testMSSQLUser,
		Password: testMSSQLPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetMSSQLAddress(),
			strconv.Itoa(environment.GetMSSQLPort()),
		),
		ConnectProtocol:   testMSSQLConnectionProtocol,
		PluginName:        pluginName,
		DatabaseName:      testMSSQLDatabaseNamePrefix + shuffle.String(testMSSQLDatabaseNameSuffix),
		ConnectAttributes: connectAttrs,
	}
}

func SetupMSSQLDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MSSQL admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to create MSSQL database: %v", err)
	}
}

func SetupMSSQLSchema(t *testing.T, cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MSSQL admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testMSSQLExecutionSchema)
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

	schemaPath, err = filepath.Abs(testMSSQLVisibilitySchema)
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

func TearDownMSSQLDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MSSQL admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to drop MSSQL database: %v", err)
	}
}
