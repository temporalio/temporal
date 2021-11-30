// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package clitest

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/environment"
	postgresqlversion "go.temporal.io/server/schema/postgresql"
)

const (
	testPostgreSQLExecutionSchemaFile        = "../../../schema/postgresql/v96/temporal/schema.sql"
	testPostgreSQLVisibilitySchemaFile       = "../../../schema/postgresql/v96/visibility/schema.sql"
	testPostgreSQLExecutionSchemaVersionDir  = "../../../schema/postgresql/v96/temporal/versioned"
	testPostgreSQLVisibilitySchemaVersionDir = "../../../schema/postgresql/v96/visibility/versioned"
	testPostgreSQLQuery                      = `
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

func TestPostgreSQLConnTestSuite(t *testing.T) {
	suite.Run(t, NewSQLConnTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName, testPostgreSQLQuery,
	))
}

func TestPostgreSQLHandlerTestSuite(t *testing.T) {
	suite.Run(t, NewHandlerTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
	))
}

func TestPostgreSQLSetupSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, NewSetupSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
		testPostgreSQLQuery,
	))
}

func TestPostgreSQLUpdateSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, NewUpdateSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
		testPostgreSQLQuery,
		testPostgreSQLExecutionSchemaVersionDir,
		postgresqlversion.Version,
		testPostgreSQLVisibilitySchemaVersionDir,
		postgresqlversion.VisibilityVersion,
	))
}

func TestPostgreSQLVersionTestSuite(t *testing.T) {
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
		testPostgreSQLExecutionSchemaFile,
		testPostgreSQLVisibilitySchemaFile,
	))
}
