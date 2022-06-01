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

package persistencetests

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestPostgreSQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(HistoryV2PersistenceSuite)
	s.TestBase = NewTestBaseWithSQL(GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestPostgreSQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(MetadataPersistenceSuiteV2)
	s.TestBase = NewTestBaseWithSQL(GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestPostgreSQLClusterMetadataPersistence(t *testing.T) {
	s := new(ClusterMetadataManagerSuite)
	s.TestBase = NewTestBaseWithSQL(GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

// TODO flaky test in buildkite
// https://go.temporal.io/server/issues/2877
/*
FAIL: TestPostgreSQLQueuePersistence/TestNamespaceReplicationQueue (0.26s)
        queuePersistenceTest.go:102:
            	Error Trace:	queuePersistenceTest.go:102
            	Error:      	Not equal:
            	            	expected: 99
            	            	actual  : 98
            	Test:       	TestPostgreSQLQueuePersistence/TestNamespaceReplicationQueue
*/
//func TestPostgreSQLQueuePersistence(t *testing.T) {
//	s := new(QueuePersistenceSuite)
//	s.TestBase = NewTestBaseWithSQL(GetPostgreSQLTestClusterOption())
//	s.TestBase.Setup()
//	suite.Run(t, s)
//}
