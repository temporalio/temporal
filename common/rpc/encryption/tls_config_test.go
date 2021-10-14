package encryption

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
)

type (
	tlsConfigTest struct {
		suite.Suite
		*require.Assertions
	}
)

func TestTLSConfigSuite(t *testing.T) {
	s := new(tlsConfigTest)
	suite.Run(t, s)
}

func (s *tlsConfigTest) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *tlsConfigTest) TestIsEnabled() {

	emptyCfg := config.GroupTLS{}
	s.False(emptyCfg.IsEnabled())
	cfg := config.GroupTLS{Server: config.ServerTLS{KeyFile: "foo"}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Server: config.ServerTLS{KeyData: "foo"}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: true}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: false}}
	s.False(cfg.IsEnabled())
}

func (s *tlsConfigTest) TestIsSystemWorker() {

	cfg := &config.RootTLS{}
	s.False(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertFile: "foo"}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertData: "foo"}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: true}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: false}}}
	s.False(isSystemWorker(cfg))
}
