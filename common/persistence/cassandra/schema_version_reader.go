package cassandra

import (
	"errors"

	"github.com/gocql/gocql"
)

const (
	readSchemaVersionCQL = `SELECT curr_version from schema_version where keyspace_name=?`
)

type (
	SchemaVersionReader struct {
		session  *gocql.Session
		keyspace string
	}
)

var (
	ErrGetSchemaVersion = errors.New("failed to get current schema version from cassandra")
)

func NewSchemaVersionReader(session *gocql.Session, keyspace string) *SchemaVersionReader {
	return &SchemaVersionReader{
		keyspace: keyspace,
		session:  session,
	}
}

// ReadSchemaVersion returns the current schema version for the Keyspace
func (svr *SchemaVersionReader) ReadSchemaVersion() (string, error) {
	query := svr.session.Query(readSchemaVersionCQL, svr.keyspace)
	// when querying the DB schema version, override to local quorum
	// in case Cassandra node down (compared to using ALL)
	query.SetConsistency(gocql.LocalQuorum)

	iter := query.Iter()
	var version string
	if !iter.Scan(&version) {
		_ = iter.Close()
		return "", ErrGetSchemaVersion
	}
	if err := iter.Close(); err != nil {
		return "", err
	}
	return version, nil
}
