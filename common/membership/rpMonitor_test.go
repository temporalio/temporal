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

package membership

import (
	"testing"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RpoSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestRpoSuite(t *testing.T) {
	suite.Run(t, new(RpoSuite))
}

func (s *RpoSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RpoSuite) TestRingpopMonitor() {
	serviceName := primitives.HistoryService
	testService := NewTestRingpopCluster(s.T(), "rpm-test", 3, "0.0.0.0", "", serviceName, "127.0.0.1")
	s.NotNil(testService, "Failed to create test service")

	logger := log.NewNoopLogger()

	rpm := testService.rings[0]

	time.Sleep(time.Second)

	listenCh := make(chan *ChangedEvent, 5)
	err := rpm.AddListener(serviceName, "test-listener", listenCh)
	s.Nil(err, "AddListener failed")

	host, err := rpm.Lookup(serviceName, "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.NotNil(host, "Ringpop monitor returned a nil host")

	logger.Info("Killing host 1")
	testService.KillHost(testService.hostUUIDs[1])

	select {
	case e := <-listenCh:
		s.Equal(1, len(e.HostsRemoved), "ringpop monitor event does not report the removed host")
		s.Equal(testService.hostAddrs[1], e.HostsRemoved[0].GetAddress(), "ringpop monitor reported that a wrong host was removed")
		s.Nil(e.HostsAdded, "Unexpected host reported to be added by ringpop monitor")
		s.Nil(e.HostsUpdated, "Unexpected host reported to be updated by ringpop monitor")
	case <-time.After(time.Minute):
		s.Fail("Timed out waiting for failure to be detected by ringpop")
	}

	host, err = rpm.Lookup(serviceName, "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.NotEqual(testService.hostAddrs[1], host.GetAddress(), "Ringpop monitor assigned key to dead host")

	err = rpm.RemoveListener(serviceName, "test-listener")
	s.Nil(err, "RemoveListener() failed")

	rpm.Stop()
	testService.Stop()
}

func (s *RpoSuite) TestCompareMembers() {
	s.testCompareMembers([]string{}, []string{"a"}, true)
	s.testCompareMembers([]string{}, []string{"a", "b"}, true)
	s.testCompareMembers([]string{"a"}, []string{"a", "b"}, true)
	s.testCompareMembers([]string{}, []string{"a"}, true)
	s.testCompareMembers([]string{}, []string{"a", "b"}, true)
	s.testCompareMembers([]string{}, []string{}, false)
	s.testCompareMembers([]string{"a"}, []string{"a"}, false)
	s.testCompareMembers([]string{"a", "b"}, []string{"a", "b"}, false)
}

func (s *RpoSuite) testCompareMembers(curr []string, new []string, hasDiff bool) {
	resolver := &ringpopServiceResolver{}
	currMembers := make(map[string]struct{}, len(curr))
	for _, m := range curr {
		currMembers[m] = struct{}{}
	}
	resolver.membersMap = currMembers
	newMembers, changed := resolver.compareMembers(new)
	s.Equal(hasDiff, changed)
	s.Equal(len(new), len(newMembers))
	for _, m := range new {
		_, ok := newMembers[m]
		s.True(ok)
	}
}
