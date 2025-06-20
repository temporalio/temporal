package tests

const (
	testUser     = "temporal"
	testPassword = "temporal"

	testMySQLExecutionSchemaFile        = "../../schema/mysql/v8/temporal/schema.sql"
	testMySQLVisibilitySchemaFile       = "../../schema/mysql/v8/visibility/schema.sql"
	testMySQLExecutionSchemaVersionDir  = "../../schema/mysql/v8/temporal/versioned"
	testMySQLVisibilitySchemaVersionDir = "../../schema/mysql/v8/visibility/versioned"

	testMySQLQuery = `
-- test sql file content

CREATE TABLE executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  --
  next_event_id BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  state BLOB NOT NULL,
  state_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id BINARY(16) NOT NULL,
  create_request_id VARCHAR(64) NOT NULL,
  state INT NOT NULL,
  status INT NOT NULL,
  start_version BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id)
);
`

	testPostgreSQLExecutionSchemaFile        = "../../schema/postgresql/v12/temporal/schema.sql"
	testPostgreSQLVisibilitySchemaFile       = "../../schema/postgresql/v12/visibility/schema.sql"
	testPostgreSQLExecutionSchemaVersionDir  = "../../schema/postgresql/v12/temporal/versioned"
	testPostgreSQLVisibilitySchemaVersionDir = "../../schema/postgresql/v12/visibility/versioned"

	testPostgreSQLQuery = `
-- test sql file content

CREATE TABLE executions(
  shard_id INTEGER NOT NULL,
  namespace_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  --
  next_event_id BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  state BYTEA NOT NULL,
  state_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INTEGER NOT NULL,
  namespace_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id BYTEA NOT NULL,
  create_request_id VARCHAR(64) NOT NULL,
  state INTEGER NOT NULL,
  status INTEGER NOT NULL,
  last_write_version BIGINT NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id)
);
`
)
