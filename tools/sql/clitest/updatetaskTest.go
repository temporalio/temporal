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
	"path/filepath"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema/test"
	"go.temporal.io/server/tools/sql"
)

// UpdateSchemaTestSuite defines a test suite
type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
	host                       string
	port                       string
	pluginName                 string
	sqlQuery                   string
	executionSchemaVersionDir  string
	executionVersion           string
	visibilitySchemaVersionDir string
	visibilityVersion          string
}

// NewUpdateSchemaTestSuite returns a test suite
func NewUpdateSchemaTestSuite(
	host string,
	port string,
	pluginName string,
	sqlQuery string,
	executionSchemaVersionDir string,
	executionVersion string,
	visibilitySchemaVersionDir string,
	visibilityVersion string,
) *UpdateSchemaTestSuite {
	return &UpdateSchemaTestSuite{
		host:                       host,
		port:                       port,
		pluginName:                 pluginName,
		sqlQuery:                   sqlQuery,
		executionSchemaVersionDir:  executionSchemaVersionDir,
		executionVersion:           executionVersion,
		visibilitySchemaVersionDir: visibilitySchemaVersionDir,
		visibilityVersion:          visibilityVersion,
	}
}

// SetupSuite setups test suite
func (s *UpdateSchemaTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.host, s.port, s.pluginName)
	if err != nil {
		s.Logger.Fatal("Error creating CQLClient", tag.Error(err))
	}
	s.SetupSuiteBase(conn, s.pluginName)
}

// TearDownSuite tear down test suite
func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestUpdateSchema test
func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	s.RunUpdateSchemaTest(sql.BuildCLIOptions(), conn, "--db", s.sqlQuery, []string{"executions", "current_executions"})
}

// TestDryrun test
func (s *UpdateSchemaTestSuite) TestDryrun() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.NoError(err)
	defer conn.Close()
	dir, err := filepath.Abs(s.executionSchemaVersionDir)
	s.NoError(err)
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, s.executionVersion)
}

// TestVisibilityDryrun test
func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.NoError(err)
	defer conn.Close()
	dir, err := filepath.Abs(s.visibilitySchemaVersionDir)
	s.NoError(err)
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, s.visibilityVersion)
}
