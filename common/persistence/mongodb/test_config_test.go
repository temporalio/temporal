package mongodb_test

import (
	"fmt"
	"os"
	"strings"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/temporal/environment"
)

// newMongoTestConfig constructs a MongoDB config suitable for local unit tests.
// It honors the docker-compose defaults (temporal/temporal credentials, admin auth source)
// while allowing overrides via environment variables when needed.
func newMongoTestConfig(databaseName string) config.MongoDB {
	seeds := os.Getenv("MONGODB_SEEDS")
	var hosts []string
	if seeds != "" {
		parts := strings.Split(seeds, ",")
		hosts = hosts[:0]
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				hosts = append(hosts, trimmed)
			}
		}
	}
	if len(hosts) == 0 {
		hosts = []string{fmt.Sprintf("%s:%d", environment.GetMongoDBAddress(), environment.GetMongoDBPort())}
	}

	user := os.Getenv("TEMPORAL_MONGODB_TEST_USER")
	if user == "" {
		user = "temporal"
	}

	password := os.Getenv("TEMPORAL_MONGODB_TEST_PASSWORD")
	if password == "" {
		password = "temporal"
	}

	authSource := os.Getenv("TEMPORAL_MONGODB_TEST_AUTHSOURCE")
	if authSource == "" {
		authSource = "admin"
	}

	return config.MongoDB{
		Hosts:          hosts,
		User:           user,
		Password:       password,
		AuthSource:     authSource,
		ReplicaSet:     environment.GetMongoDBReplicaSet(),
		DatabaseName:   databaseName,
		ConnectTimeout: 10 * time.Second,
		MaxConns:       10,
		ReadPreference: "primary",
		WriteConcern:   "majority",
	}
}
