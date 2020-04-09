package ringpop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/temporalio/temporal/common/service/config"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"

	"github.com/temporalio/temporal/common/log/loggerimpl"
)

type RingpopSuite struct {
	*require.Assertions
	suite.Suite
}

func TestRingpopSuite(t *testing.T) {
	suite.Run(t, new(RingpopSuite))
}

func (s *RingpopSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *RingpopSuite) TestHostsMode() {
	var cfg config.Ringpop
	err := yaml.Unmarshal([]byte(getHostsConfig()), &cfg)
	s.Nil(err)
	s.Equal("test", cfg.Name)
	s.Equal("1.2.3.4", cfg.BroadcastAddress)
	s.Equal(time.Second*30, cfg.MaxJoinDuration)
	err = validateRingpopConfig(&cfg)
	s.Nil(err)
	f, err := NewRingpopFactory(&cfg, nil, "test", nil, loggerimpl.NewNopLogger(), nil)
	s.Nil(err)
	s.NotNil(f)
}

type mockResolver struct {
	Hosts map[string][]string
}

func (resolver *mockResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	addrs, ok := resolver.Hosts[host]
	if !ok {
		return nil, fmt.Errorf("Host was not resolved: %s", host)
	}
	return addrs, nil
}

func (s *RingpopSuite) TestInvalidConfig() {
	var cfg config.Ringpop
	s.Error(validateRingpopConfig(&cfg))
	cfg.Name = "test"
	s.NoError(validateRingpopConfig(&cfg))
	cfg.BroadcastAddress = "sjhdfskdjhf"
	s.Error(validateRingpopConfig(&cfg))
}

func getHostsConfig() string {
	return `name: "test"
broadcastAddress: "1.2.3.4"
maxJoinDuration: 30s`
}
