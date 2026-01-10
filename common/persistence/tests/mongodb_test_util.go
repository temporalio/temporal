package tests

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/mongodb"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/temporal/environment"
	"go.uber.org/zap/zaptest"
)

const (
	testMongoDBClusterName = cluster.TestCurrentClusterName

	testMongoDBUser     = "temporal"
	testMongoDBPassword = "temporal"

	testMongoDBDatabaseNamePrefix = "test_"
	testMongoDBDatabaseNameSuffix = "temporal_persistence"
)

type (
	MongoDBTestData struct {
		Cfg     *config.MongoDB
		Factory *mongodb.Factory
		Logger  log.Logger
		Metrics *metricstest.Capture
	}
)

func setUpMongoDBTest(t *testing.T) (MongoDBTestData, func()) {
	var testData MongoDBTestData
	testData.Cfg = NewMongoDBConfig()
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))

	mh := metricstest.NewCaptureHandler()
	testData.Metrics = mh.StartCapture()

	factory, err := mongodb.NewFactory(
		*testData.Cfg,
		testMongoDBClusterName,
		testData.Logger,
		mh,
	)
	if err != nil {
		mh.StopCapture(testData.Metrics)
		t.Fatalf("unable to create MongoDB factory: %v", err)
	}
	testData.Factory = factory

	tearDown := func() {
		testData.Factory.Close()
		mh.StopCapture(testData.Metrics)
		TearDownMongoDBDatabase(t, testData.Cfg)
	}

	return testData, tearDown
}

// NewMongoDBConfig returns a new MongoDB config for test.
func NewMongoDBConfig() *config.MongoDB {
	host := net.JoinHostPort(
		environment.GetMongoDBAddress(),
		strconv.Itoa(environment.GetMongoDBPort()),
	)

	return &config.MongoDB{
		User:           testMongoDBUser,
		Password:       testMongoDBPassword,
		AuthSource:     "admin",
		Hosts:          []string{host},
		DatabaseName:   testMongoDBDatabaseNamePrefix + shuffle.String(testMongoDBDatabaseNameSuffix),
		MaxConns:       20,
		ConnectTimeout: 10 * time.Second,
		ReplicaSet:     environment.GetMongoDBReplicaSet(),
		ReadPreference: "primary",
		WriteConcern:   "majority",
	}
}

func TearDownMongoDBDatabase(t *testing.T, cfg *config.MongoDB) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(buildMongoURI(cfg, "")))
	if err != nil {
		t.Fatalf("unable to connect to MongoDB: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	if err := client.Database(cfg.DatabaseName).Drop(ctx); err != nil {
		t.Fatalf("unable to drop MongoDB database %q: %v", cfg.DatabaseName, err)
	}
}

func buildMongoURI(cfg *config.MongoDB, database string) string {
	uri := "mongodb://"
	if cfg.User != "" && cfg.Password != "" {
		uri += fmt.Sprintf("%s:%s@", cfg.User, cfg.Password)
	}
	uri += strings.Join(cfg.Hosts, ",")
	uri += "/" + database

	var opts []string
	if cfg.ReplicaSet != "" {
		opts = append(opts, fmt.Sprintf("replicaSet=%s", cfg.ReplicaSet))
	}
	if cfg.AuthSource != "" {
		opts = append(opts, fmt.Sprintf("authSource=%s", cfg.AuthSource))
	}
	if len(opts) > 0 {
		uri += "?" + strings.Join(opts, "&")
	}
	return uri
}
