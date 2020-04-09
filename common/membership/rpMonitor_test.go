package membership

import (
	"testing"
	"time"

	"github.com/temporalio/temporal/common/primitives"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/log/loggerimpl"
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

	logger := loggerimpl.NewNopLogger()

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
