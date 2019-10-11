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

package persistencetests

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestSQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(HistoryV2PersistenceSuite)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMatchingPersistenceSuite(t *testing.T) {
	s := new(MatchingPersistenceSuite)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(MetadataPersistenceSuiteV2)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLShardPersistenceSuite(t *testing.T) {
	s := new(ShardPersistenceSuite)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerSuite(t *testing.T) {
	s := new(ExecutionManagerSuite)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerWithEventsV2(t *testing.T) {
	s := new(ExecutionManagerSuiteForEventsV2)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLVisibilityPersistenceSuite(t *testing.T) {
	s := new(VisibilityPersistenceSuite)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLQueuePersistence(t *testing.T) {
	s := new(QueuePersistenceSuite)
	s.TestBase = NewTestBaseWithSQL(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}
