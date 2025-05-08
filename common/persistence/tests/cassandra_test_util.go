package tests

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/temporal/environment"
	"go.uber.org/zap/zaptest"
)

const (
	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testCassandraExecutionSchema = "../../../schema/cassandra/temporal/schema.cql"
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
	SetUpCassandraDatabase(t, testData.Cfg, testData.Logger)
	SetUpCassandraSchema(t, testData.Cfg, testData.Logger)

	testData.Factory = cassandra.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		testData.Logger,
		metrics.NoopMetricsHandler,
	)

	tearDown := func() {
		testData.Factory.Close()
		TearDownCassandraKeyspace(t, testData.Cfg)
	}

	return testData, tearDown
}

func SetUpCassandraDatabase(t *testing.T, cfg *config.Cassandra, logger log.Logger) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(adminCfg, resolver.NewNoopResolver())
		},
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("unable to create Cassandra session: %v", err)
	}
	defer session.Close()

	if err := cassandra.CreateCassandraKeyspace(
		session,
		cfg.Keyspace,
		1,
		true,
		log.NewNoopLogger(),
	); err != nil {
		t.Fatalf("unable to create Cassandra keyspace: %v", err)
	}
}

func SetUpCassandraSchema(t *testing.T, cfg *config.Cassandra, logger log.Logger) {
	ApplySchemaUpdate(t, cfg, testCassandraExecutionSchema, logger)
}

func ApplySchemaUpdate(t *testing.T, cfg *config.Cassandra, schemaFile string, logger log.Logger) {
	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(*cfg, resolver.NewNoopResolver())
		},
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	schemaPath, err := filepath.Abs(schemaFile)
	if err != nil {
		t.Fatal(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		if err = session.Query(stmt).Exec(); err != nil {
			logger.Error(fmt.Sprintf("Unable to execute statement from file: %s\n  %s", schemaFile, stmt))
			t.Fatal(err)
		}
	}
}

func TearDownCassandraKeyspace(t *testing.T, cfg *config.Cassandra) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(adminCfg, resolver.NewNoopResolver())
		},
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("unable to create Cassandra session: %v", err)
	}
	defer session.Close()

	if err := cassandra.DropCassandraKeyspace(
		session,
		cfg.Keyspace,
		log.NewNoopLogger(),
	); err != nil {
		t.Fatalf("unable to drop Cassandra keyspace: %v", err)
	}
}

// GetSchemaFiles takes a root directory which contains subdirectories whose names are semantic versions and returns
// the .cql files within. E.g.: //schema/cassandra/temporal/versioned
// Subdirectories are ordered by semantic version, but files within the same subdirectory are in arbitrary order.
// All .cql files are returned regardless of whether they are named in manifest.json.
func GetSchemaFiles(t *testing.T, schemaDir string, logger log.Logger) []string {
	var retVal []string

	versionDirPath := path.Join(schemaDir, "versioned")
	subDirs, err := os.ReadDir(versionDirPath)
	if err != nil {
		t.Fatal(err)
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
			t.Fatal(err) // Logic error
		}
		vRight, err := semver.ParseTolerant(versionDirNames[j])
		if err != nil {
			t.Fatal(err) // Logic error
		}
		return vLeft.Compare(vRight) < 0
	})

	for _, dir := range versionDirNames {
		vDirPath := path.Join(versionDirPath, dir)
		files, err := os.ReadDir(vDirPath)
		if err != nil {
			t.Fatal(err)
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
		User:           testCassandraUser,
		Password:       testCassandraPassword,
		Hosts:          environment.GetCassandraAddress(),
		Port:           environment.GetCassandraPort(),
		Keyspace:       testCassandraDatabaseNamePrefix + shuffle.String(testCassandraDatabaseNameSuffix),
		ConnectTimeout: 30 * time.Second,
	}
}
