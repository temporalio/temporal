package membership

import (
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
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
	testService := NewTestRingpopCluster("rpm-test", 3, "127.0.0.1", "", "rpm-test")
	s.NotNil(testService, "Failed to create test service")

	services := []string{"rpm-test"}

	logger := bark.NewLoggerFromLogrus(log.New())
	rpm := NewRingpopMonitor(services, testService.rings[0], logger)
	err := rpm.Start()
	s.Nil(err, "Failed to start ringpop monitor")

	// Sleep to give time for the ring to stabilize
	time.Sleep(time.Second)

	listenCh := make(chan *ChangedEvent, 5)
	err = rpm.AddListener("rpm-test", "test-listener", listenCh)
	s.Nil(err, "AddListener failed")

	host, err := rpm.Lookup("rpm-test", "key")
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

	host, err = rpm.Lookup("rpm-test", "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.NotEqual(testService.hostAddrs[1], host.GetAddress(), "Ringpop monitor assigned key to dead host")

	err = rpm.RemoveListener("rpm-test", "test-listener")
	s.Nil(err, "RemoveListener() failed")

	rpm.Stop()
	testService.Stop()
}
