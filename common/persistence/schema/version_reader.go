package schema

type (
	VersionReader interface {
		// ReadSchemaVersion returns the current schema version for the keyspace
		ReadSchemaVersion(dbName string) (string, error)
	}
)
