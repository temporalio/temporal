package cassandra

import (
	"fmt"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const cassandraPersistenceName = "cassandra"

// CreateCassandraKeyspace creates the keyspace using this session for given replica count
func CreateCassandraKeyspace(s gocql.Session, keyspace string, replicas int, overwrite bool, logger log.Logger) (err error) {
	// if overwrite flag is set, drop the keyspace and create a new one
	if overwrite {
		err = DropCassandraKeyspace(s, keyspace, logger)
		if err != nil {
			logger.Error("drop keyspace error", tag.Error(err))
			return
		}
	}
	err = s.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, replicas)).Exec()
	if err != nil {
		logger.Error("create keyspace error", tag.Error(err))
		return
	}
	logger.Debug("created keyspace", tag.Value(keyspace))

	return
}

// DropCassandraKeyspace drops the given keyspace, if it exists
func DropCassandraKeyspace(s gocql.Session, keyspace string, logger log.Logger) (err error) {
	err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	if err != nil {
		logger.Error("drop keyspace error", tag.Error(err))
		return
	}
	logger.Debug("dropped keyspace", tag.Value(keyspace))
	return
}
