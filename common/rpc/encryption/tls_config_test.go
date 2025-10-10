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
	}
)

func TestTLSConfigSuite(t *testing.T) {
	s := new(tlsConfigTest)
	suite.Run(t, s)
}

func (s *tlsConfigTest) TestIsEnabled() {

	emptyCfg := config.GroupTLS{}
	require.False(s.T(), emptyCfg.IsServerEnabled())
	require.False(s.T(), emptyCfg.IsClientEnabled())
	cfg := config.GroupTLS{Server: config.ServerTLS{KeyFile: "foo"}}
	require.True(s.T(), cfg.IsServerEnabled())
	require.False(s.T(), cfg.IsClientEnabled())
	cfg = config.GroupTLS{Server: config.ServerTLS{KeyData: "foo"}}
	require.True(s.T(), cfg.IsServerEnabled())
	require.False(s.T(), cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}
	require.False(s.T(), cfg.IsServerEnabled())
	require.True(s.T(), cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}
	require.False(s.T(), cfg.IsServerEnabled())
	require.True(s.T(), cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: true}}
	require.False(s.T(), cfg.IsServerEnabled())
	require.True(s.T(), cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: false}}
	require.False(s.T(), cfg.IsServerEnabled())
	require.False(s.T(), cfg.IsClientEnabled())

}

func (s *tlsConfigTest) TestIsSystemWorker() {

	cfg := &config.RootTLS{}
	require.False(s.T(), isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertFile: "foo"}}
	require.True(s.T(), isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertData: "foo"}}
	require.True(s.T(), isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}}
	require.True(s.T(), isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}}
	require.True(s.T(), isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: true}}}
	require.True(s.T(), isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: false}}}
	require.False(s.T(), isSystemWorker(cfg))
}

func (s *tlsConfigTest) TestCertFileAndData() {
	s.testGroupTLS(s.testCertFileAndData)
}

func (s *tlsConfigTest) TestKeyFileAndData() {
	s.testGroupTLS(s.testKeyFileAndData)
}

func (s *tlsConfigTest) TestClientCAData() {
	s.testGroupTLS(s.testClientCAData)
}

func (s *tlsConfigTest) TestClientCAFiles() {
	s.testGroupTLS(s.testClientCAFiles)
}

func (s *tlsConfigTest) TestRootCAData() {
	s.testGroupTLS(s.testRootCAData)
}

func (s *tlsConfigTest) TestRootCAFiles() {
	s.testGroupTLS(s.testRootCAFiles)
}

func (s *tlsConfigTest) testGroupTLS(f func(*config.RootTLS, *config.GroupTLS)) {

	cfg := &config.RootTLS{Internode: config.GroupTLS{}}
	f(cfg, &cfg.Internode)
	cfg = &config.RootTLS{Frontend: config.GroupTLS{}}
	f(cfg, &cfg.Frontend)
}

func (s *tlsConfigTest) testCertFileAndData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertFile: "foo"}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertData: "bar"}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertFile: "foo", CertData: "bar"}
	require.Error(s.T(), validateRootTLS(cfg))
}

func (s *tlsConfigTest) testKeyFileAndData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyFile: "foo"}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyData: "bar"}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyFile: "foo", KeyData: "bar"}
	require.Error(s.T(), validateRootTLS(cfg))
}

func (s *tlsConfigTest) testClientCAData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo", "bar"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo", " "}}
	require.Error(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{""}}
	require.Error(s.T(), validateRootTLS(cfg))
}

func (s *tlsConfigTest) testClientCAFiles(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo", "bar"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo", " "}}
	require.Error(s.T(), validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{""}}
	require.Error(s.T(), validateRootTLS(cfg))
}

func (s *tlsConfigTest) testRootCAData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Client = config.ClientTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo", "bar"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo", " "}}
	require.Error(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{""}}
	require.Error(s.T(), validateRootTLS(cfg))
}

func (s *tlsConfigTest) testRootCAFiles(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Client = config.ClientTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo", "bar"}}
	require.Nil(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo", " "}}
	require.Error(s.T(), validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{""}}
	require.Error(s.T(), validateRootTLS(cfg))
}

func (s *tlsConfigTest) TestSystemWorkerTLSConfig() {
	cfg := &config.RootTLS{}
	cfg.SystemWorker = config.WorkerTLS{}
	require.Nil(s.T(), validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertFile: "foo"}
	require.Nil(s.T(), validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertData: "bar"}
	require.Nil(s.T(), validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertFile: "foo", CertData: "bar"}
	require.Error(s.T(), validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyFile: "foo"}
	require.Nil(s.T(), validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyData: "bar"}
	require.Nil(s.T(), validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyFile: "foo", KeyData: "bar"}
	require.Error(s.T(), validateRootTLS(cfg))

	cfg.SystemWorker = config.WorkerTLS{Client: config.ClientTLS{}}
	client := &cfg.SystemWorker.Client
	client.RootCAData = []string{}
	require.Nil(s.T(), validateRootTLS(cfg))
	client.RootCAData = []string{"foo"}
	require.Nil(s.T(), validateRootTLS(cfg))
	client.RootCAData = []string{"foo", "bar"}
	require.Nil(s.T(), validateRootTLS(cfg))
	client.RootCAData = []string{"foo", " "}
	require.Error(s.T(), validateRootTLS(cfg))
	client.RootCAData = []string{""}
	require.Error(s.T(), validateRootTLS(cfg))
}
