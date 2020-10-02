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

package postgresql

import (
	"testing"

	"github.com/stretchr/testify/suite"

	pt "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/environment"
)

const (
	testUser      = "temporal"
	testPassword  = "temporal"
	testSchemaDir = "schema/postgresql"
)

func getTestClusterOption() *pt.TestBaseOptions {
	return &pt.TestBaseOptions{
		SQLDBPluginName: PluginName,
		DBUsername:      testUser,
		DBPassword:      testPassword,
		DBHost:          environment.GetPostgreSQLAddress(),
		DBPort:          environment.GetPostgreSQLPort(),
		SchemaDir:       testSchemaDir,
	}
}

func TestSQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(pt.HistoryV2PersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMatchingPersistenceSuite(t *testing.T) {
	s := new(pt.MatchingPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(pt.MetadataPersistenceSuiteV2)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLShardPersistenceSuite(t *testing.T) {
	s := new(pt.ShardPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerSuite(t *testing.T) {
	s := new(pt.ExecutionManagerSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerWithEventsV2(t *testing.T) {
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLVisibilityPersistenceSuite(t *testing.T) {
	s := new(pt.VisibilityPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestClusterMetadataPersistence(t *testing.T) {
	s := new(pt.ClusterMetadataManagerSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

// TODO flaky test in buildkite
// https://go.temporal.io/server/issues/2877
/*
FAIL: TestSQLQueuePersistence/TestNamespaceReplicationQueue (0.26s)
        queuePersistenceTest.go:102:
            	Error Trace:	queuePersistenceTest.go:102
            	Error:      	Not equal:
            	            	expected: 99
            	            	actual  : 98
            	Test:       	TestSQLQueuePersistence/TestNamespaceReplicationQueue
*/
//func TestSQLQueuePersistence(t *testing.T) {
//	s := new(pt.QueuePersistenceSuite)
//	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
//	s.TestBase.Setup()
//	suite.Run(t, s)
//}
