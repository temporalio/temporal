package cassandra

import (
	"fmt"

	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	readSchemaVersionCQL = `SELECT curr_version from schema_version where keyspace_name=?`
)

type (
	SchemaVersionReader struct {
		session gocql.Session
	}
)

func NewSchemaVersionReader(session gocql.Session) *SchemaVersionReader {
	return &SchemaVersionReader{
		session: session,
	}
}

// ReadSchemaVersion returns the current schema version for the Keyspace
func (svr *SchemaVersionReader) ReadSchemaVersion(keyspace string) (string, error) {
	query := svr.session.Query(readSchemaVersionCQL, keyspace)

	iter := query.Iter()
	var version string
	success := iter.Scan(&version)
	err := iter.Close()
	if err == nil && !success {
		err = fmt.Errorf("no schema version found for keyspace %q", keyspace)
	}
	if err != nil {
		return "", fmt.Errorf("unable to get current schema version from Cassandra: %w", err)
	}

	return version, nil
}
