// Copyright (c) 2017 Uber Technologies, Inc.
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

package cassandra

import (
	"log"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/schema/cassandra"
	"github.com/uber/cadence/tools/common/schema/test"
)

type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
}

func TestUpdateSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateSchemaTestSuite))
}

func (s *UpdateSchemaTestSuite) SetupSuite() {
	client, err := newTestCQLClient(systemKeyspace)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.SetupSuiteBase(client)
}

func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	s.RunUpdateSchemaTest(buildCLIOptions(), client, "-k", createTestCQLFileContent(), []string{"events", "tasks"})
}

func (s *UpdateSchemaTestSuite) TestDryrun() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := "../../schema/cassandra/cadence/versioned"
	s.RunDryrunTest(buildCLIOptions(), client, "-k", dir, cassandra.Version)
}

func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := "../../schema/cassandra/visibility/versioned"
	s.RunDryrunTest(buildCLIOptions(), client, "-k", dir, cassandra.VisibilityVersion)
}
