package persistencetests

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/temporal/environment"
)

type MongoTestCluster struct {
	cfg            config.MongoDB
	logger         log.Logger
	faultInjection *config.FaultInjection
}

func NewMongoTestCluster(cfg config.MongoDB, logger log.Logger, faultInjection *config.FaultInjection) *MongoTestCluster {
	return &MongoTestCluster{
		cfg:            cfg,
		logger:         logger,
		faultInjection: faultInjection,
	}
}

func (c *MongoTestCluster) SetupTestDatabase() {
	if err := dropMongoDatabase(c.cfg); err != nil {
		c.logger.Error("failed to drop mongo database before setup", tag.Error(err))
	}
}

func (c *MongoTestCluster) TearDownTestDatabase() {
	if err := dropMongoDatabase(c.cfg); err != nil {
		c.logger.Error("failed to drop mongo database during teardown", tag.Error(err))
	}
}

func (c *MongoTestCluster) Config() config.Persistence {
	storeName := "test-mongodb"
	cfgCopy := c.cfg
	return config.Persistence{
		DefaultStore:    storeName,
		VisibilityStore: storeName,
		DataStores: map[string]config.DataStore{
			storeName: {
				MongoDB:        &cfgCopy,
				FaultInjection: c.faultInjection,
			},
		},
	}
}

func NewTestBaseWithMongoDB(opts *TestBaseOptions) *TestBase {
	cfg := opts.MongoDBConfig
	if cfg == nil {
		defaults := GetMongoDBTestClusterOption()
		cfg = defaults.MongoDBConfig
	}
	cfgCopy := *cfg
	if cfgCopy.DatabaseName == "" {
		cfgCopy.DatabaseName = "test_" + GenerateRandomDBName(4) + "_temporal_persistence"
	}
	if len(cfgCopy.Hosts) == 0 {
		host := net.JoinHostPort(environment.GetMongoDBAddress(), strconv.Itoa(environment.GetMongoDBPort()))
		cfgCopy.Hosts = []string{host}
	}
	if cfgCopy.ConnectTimeout == 0 {
		cfgCopy.ConnectTimeout = 10 * time.Second
	}
	if cfgCopy.ReplicaSet == "" {
		cfgCopy.ReplicaSet = environment.GetMongoDBReplicaSet()
	}
	if cfgCopy.ReadPreference == "" {
		cfgCopy.ReadPreference = "primary"
	}
	if cfgCopy.WriteConcern == "" {
		cfgCopy.WriteConcern = "majority"
	}
	if opts.Logger == nil {
		opts.Logger = log.NewTestLogger()
	}
	opts.MongoDBConfig = &cfgCopy
	cluster := NewMongoTestCluster(cfgCopy, opts.Logger, opts.FaultInjection)
	return NewTestBaseForCluster(cluster, opts.Logger)
}

func GetMongoDBTestClusterOption() *TestBaseOptions {
	host := net.JoinHostPort(environment.GetMongoDBAddress(), strconv.Itoa(environment.GetMongoDBPort()))
	cfg := &config.MongoDB{
		User:           "temporal",
		Password:       "temporal",
		AuthSource:     "admin",
		Hosts:          []string{host},
		DatabaseName:   "test_" + GenerateRandomDBName(4) + "_temporal_persistence",
		MaxConns:       200,
		MinConns:       20,
		ConnectTimeout: 10 * time.Second,
		ReplicaSet:     environment.GetMongoDBReplicaSet(),
		ReadPreference: "primary",
		WriteConcern:   "majority",
	}
	return &TestBaseOptions{
		StoreType:         config.StoreTypeNoSQL,
		NoSQLDBPluginName: "mongodb",
		MongoDBConfig:     cfg,
	}
}

func dropMongoDatabase(cfg config.MongoDB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(buildMongoURI(cfg, "")))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	if err := client.Database(cfg.DatabaseName).Drop(ctx); err != nil {
		return fmt.Errorf("drop mongo database %q: %w", cfg.DatabaseName, err)
	}
	return nil
}

func buildMongoURI(cfg config.MongoDB, database string) string {
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
