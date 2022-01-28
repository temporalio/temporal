package tests

import (
	"fmt"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/resolver"
	"path/filepath"
)

const (
	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testCassandraExecutionSchema  = "../../../schema/cassandra/temporal/schema.cql"
	testCassandraVisibilitySchema = "../../../schema/cassandra/visibility/schema.cql"
)

func SetupCassandraDatabase(cfg *config.Cassandra) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := gocql.NewSession(adminCfg, resolver.NewNoopResolver(), log.NewNoopLogger())
	if err != nil {
		panic(fmt.Sprintf("unable to create Cassandra session: %v", err))
	}
	defer session.Close()

	if err := cassandra.CreateCassandraKeyspace(
		session,
		cfg.Keyspace,
		1,
		true,
		log.NewNoopLogger(),
	); err != nil {
		panic(fmt.Sprintf("unable to create Cassandra keyspace: %v", err))
	}
}

func SetUpCassandraSchema(cfg *config.Cassandra) {
	session, err := gocql.NewSession(*cfg, resolver.NewNoopResolver(), log.NewNoopLogger())
	if err != nil {
		panic(fmt.Sprintf("unable to create Cassandra session: %v", err))
	}
	defer session.Close()

	schemaPath, err := filepath.Abs(testCassandraExecutionSchema)
	if err != nil {
		panic(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		panic(err)
	}

	for _, stmt := range statements {
		if err = session.Query(stmt).Exec(); err != nil {
			panic(err)
		}
	}

	schemaPath, err = filepath.Abs(testCassandraVisibilitySchema)
	if err != nil {
		panic(err)
	}

	statements, err = p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		panic(err)
	}

	for _, stmt := range statements {
		if err = session.Query(stmt).Exec(); err != nil {
			panic(err)
		}
	}
}

func TearDownCassandraKeyspace(cfg *config.Cassandra) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := gocql.NewSession(adminCfg, resolver.NewNoopResolver(), log.NewNoopLogger())
	if err != nil {
		panic(fmt.Sprintf("unable to create Cassandra session: %v", err))
	}
	defer session.Close()

	if err := cassandra.DropCassandraKeyspace(
		session,
		cfg.Keyspace,
		log.NewNoopLogger(),
	); err != nil {
		panic(fmt.Sprintf("unable to drop Cassandra keyspace: %v", err))
	}
}
