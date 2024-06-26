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

package rpc

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/tests/testutils"
)

const (
	internodeServerCertSerialNumber = 100
	frontendServerCertSerialNumber  = 150
)

var (
	frontendURL         = "dummy://" // not needed for test
	frontendHTTPURL         = "dummy://" // not needed for test
	noExtraInterceptors = []grpc.UnaryClientInterceptor{}
)

type localStoreRPCSuite struct {
	*require.Assertions
	*suite.Suite

	controller *gomock.Controller

	logger log.Logger

	insecureRPCFactory                      *TestFactory
	internodeMutualTLSRPCFactory            *TestFactory
	internodeServerTLSRPCFactory            *TestFactory
	internodeAltMutualTLSRPCFactory         *TestFactory
	frontendMutualTLSRPCFactory             *TestFactory
	frontendServerTLSRPCFactory             *TestFactory
	frontendSystemWorkerMutualTLSRPCFactory *TestFactory
	frontendDynamicTLSFactory               *TestFactory
	internodeDynamicTLSFactory              *TestFactory
	internodeMutualTLSRPCRefreshFactory     *TestFactory
	frontendMutualTLSRPCRefreshFactory      *TestFactory
	remoteClusterMutualTLSRPCFactory        *TestFactory
	frontendConfigRootCAForceTLSFactory     *TestFactory

	internodeCertDir        string
	frontendCertDir         string
	frontendAltCertDir      string
	frontendRollingCertDir  string
	internodeRefreshCertDir string
	frontendRefreshCertDir  string

	internodeChain        testutils.CertChain
	frontendChain         testutils.CertChain
	frontendAltChain      testutils.CertChain
	frontendRollingCerts  []*tls.Certificate
	internodeRefreshChain testutils.CertChain
	internodeRefreshCA    *tls.Certificate
	frontendRefreshChain  testutils.CertChain
	frontendRefreshCA     *tls.Certificate

	frontendClientCertDir string
	frontendClientChain   testutils.CertChain

	membershipConfig               config.Membership
	frontendConfigServerTLS        config.GroupTLS
	frontendConfigMutualTLS        config.GroupTLS
	frontendConfigPerHostOverrides config.GroupTLS
	frontendConfigRootCAOnly       config.GroupTLS
	frontendConfigRootCAForceTLS   config.GroupTLS
	frontendConfigAltRootCAOnly    config.GroupTLS
	systemWorkerOnly               config.WorkerTLS
	frontendConfigSystemWorker     config.WorkerTLS
	frontendConfigMutualTLSRefresh config.GroupTLS

	internodeConfigMutualTLS        config.GroupTLS
	internodeConfigServerTLS        config.GroupTLS
	internodeConfigAltMutualTLS     config.GroupTLS
	internodeConfigMutualTLSRefresh config.GroupTLS

	dynamicCACertPool *x509.CertPool
	wrongCACertPool   *x509.CertPool

	dynamicConfigProvider *encryption.TestDynamicTLSConfigProvider
}

func TestLocalStoreTLSSuite(t *testing.T) {
	t.Skip("Skipping flaky test")
	suite.Run(t, &localStoreRPCSuite{
		Suite: &suite.Suite{},
	})
}

func (s *localStoreRPCSuite) TearDownSuite() {
	_ = os.RemoveAll(s.internodeCertDir)
	_ = os.RemoveAll(s.frontendCertDir)
}

func (s *localStoreRPCSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.logger = log.NewTestLogger()

	provider, err := encryption.NewTLSConfigProviderFromConfig(serverCfgInsecure.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	insecureFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, nil, noExtraInterceptors, nil)
	s.NotNil(insecureFactory)
	s.insecureRPCFactory = i(insecureFactory)

	s.frontendCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontend")
	s.NoError(err)
	s.frontendChain, err = testutils.GenerateTestChain(s.frontendCertDir, localhostIPv4)
	s.NoError(err)

	s.internodeCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteInternode")
	s.NoError(err)
	s.internodeChain, err = testutils.GenerateTestChain(s.internodeCertDir, localhostIPv4)
	s.NoError(err)

	s.frontendAltCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendAlt")
	s.NoError(err)
	s.frontendAltChain, err = testutils.GenerateTestChain(s.frontendAltCertDir, localhost)
	s.NoError(err)

	s.frontendClientCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendClient")
	s.NoError(err)
	s.frontendClientChain, err = testutils.GenerateTestChain(s.frontendClientCertDir, localhostIPv4)
	s.NoError(err)

	s.frontendRollingCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendRolling")
	s.NoError(err)
	s.frontendRollingCerts, s.dynamicCACertPool, s.wrongCACertPool, err = testutils.GenerateTestCerts(s.frontendRollingCertDir, localhostIPv4, 2)
	s.NoError(err)

	s.internodeRefreshCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteInternodeRefresh")
	s.NoError(err)
	s.internodeRefreshChain, s.internodeRefreshCA, err = testutils.GenerateTestChainWithSN(s.internodeRefreshCertDir, localhostIPv4, internodeServerCertSerialNumber)
	s.NoError(err)

	s.frontendRefreshCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendRefresh")
	s.NoError(err)
	s.frontendRefreshChain, s.frontendRefreshCA, err = testutils.GenerateTestChainWithSN(s.frontendRefreshCertDir, localhostIPv4, frontendServerCertSerialNumber)
	s.NoError(err)

	s.membershipConfig = config.Membership{
		MaxJoinDuration:  5,
		BroadcastAddress: localhostIPv4,
	}

	frontendConfigBase := config.GroupTLS{
		Server: config.ServerTLS{
			CertFile: s.frontendChain.CertPubFile,
			KeyFile:  s.frontendChain.CertKeyFile,
		},
	}

	s.frontendConfigServerTLS = frontendConfigBase
	s.frontendConfigServerTLS.Client = config.ClientTLS{RootCAFiles: []string{s.frontendChain.CaPubFile}}

	s.frontendConfigMutualTLS = frontendConfigBase
	s.frontendConfigMutualTLS.Server.ClientCAFiles = []string{s.frontendClientChain.CaPubFile}
	s.frontendConfigMutualTLS.Server.RequireClientAuth = true

	s.frontendConfigPerHostOverrides = s.frontendConfigServerTLS
	s.frontendConfigPerHostOverrides.PerHostOverrides = map[string]config.ServerTLS{
		localhost: {
			CertFile:          s.frontendAltChain.CertPubFile,
			KeyFile:           s.frontendAltChain.CertKeyFile,
			ClientCAFiles:     []string{s.frontendAltChain.CaPubFile},
			RequireClientAuth: true,
		},
	}
	s.frontendConfigRootCAOnly = config.GroupTLS{
		Client: config.ClientTLS{
			RootCAData: []string{testutils.ConvertFileToBase64(s.frontendChain.CaPubFile)},
		},
	}
	s.frontendConfigRootCAForceTLS = s.frontendConfigRootCAOnly
	s.frontendConfigRootCAForceTLS.Client.ForceTLS = true

	s.frontendConfigAltRootCAOnly = config.GroupTLS{
		Server: config.ServerTLS{
			RequireClientAuth: true,
		},
		Client: config.ClientTLS{
			RootCAData: []string{testutils.ConvertFileToBase64(s.frontendAltChain.CaPubFile)},
		},
	}
	s.systemWorkerOnly = config.WorkerTLS{
		CertFile: s.frontendClientChain.CertPubFile,
		KeyFile:  s.frontendClientChain.CertKeyFile,
	}
	s.frontendConfigSystemWorker = s.systemWorkerOnly
	s.frontendConfigSystemWorker.Client = config.ClientTLS{
		RootCAFiles: []string{s.frontendChain.CaPubFile},
	}

	s.internodeConfigMutualTLS = config.GroupTLS{
		Server: config.ServerTLS{
			CertFile:          s.internodeChain.CertPubFile,
			KeyFile:           s.internodeChain.CertKeyFile,
			ClientCAFiles:     []string{s.internodeChain.CaPubFile},
			RequireClientAuth: true,
		},
		Client: config.ClientTLS{
			RootCAFiles: []string{s.internodeChain.CaPubFile},
		},
	}
	s.internodeConfigServerTLS = config.GroupTLS{
		Server: config.ServerTLS{
			CertData: testutils.ConvertFileToBase64(s.internodeChain.CertPubFile),
			KeyData:  testutils.ConvertFileToBase64(s.internodeChain.CertKeyFile),
		},
		Client: config.ClientTLS{
			RootCAData: []string{testutils.ConvertFileToBase64(s.internodeChain.CaPubFile)},
		},
	}
	s.internodeConfigAltMutualTLS = config.GroupTLS{
		Server: config.ServerTLS{
			CertFile:          s.frontendAltChain.CertPubFile,
			KeyFile:           s.frontendAltChain.CertKeyFile,
			ClientCAFiles:     []string{s.frontendAltChain.CaPubFile},
			RequireClientAuth: true,
		},
		Client: config.ClientTLS{
			RootCAFiles: []string{s.frontendAltChain.CaPubFile},
		},
	}

	s.internodeConfigMutualTLSRefresh = mutualGroupTLSFromChain(s.internodeRefreshChain)
	s.frontendConfigMutualTLSRefresh = mutualGroupTLSFromChain(s.frontendRefreshChain)
}

func mutualGroupTLSFromChain(chain testutils.CertChain) config.GroupTLS {
	return config.GroupTLS{
		Server: config.ServerTLS{
			CertFile:          chain.CertPubFile,
			KeyFile:           chain.CertKeyFile,
			ClientCAFiles:     []string{chain.CaPubFile},
			RequireClientAuth: true,
		},
		Client: config.ClientTLS{
			RootCAFiles: []string{chain.CaPubFile},
		},
	}
}

func (s *localStoreRPCSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.setupInternode()
	s.setupFrontend()
}

func (s *localStoreRPCSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *localStoreRPCSuite) setupFrontend() {
	localStoreServerTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Frontend: s.frontendConfigServerTLS,
		},
	}

	localStoreMutualTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Frontend: s.frontendConfigPerHostOverrides,
		},
	}

	localStoreMutualTLSSystemWorker := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode:    s.internodeConfigMutualTLS,
			Frontend:     s.frontendConfigMutualTLS,
			SystemWorker: s.frontendConfigSystemWorker,
		},
	}

	localStoreMutualTLSWithRefresh := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Frontend:        s.frontendConfigMutualTLSRefresh,
			Internode:       s.frontendConfigMutualTLSRefresh,
			RefreshInterval: time.Second,
			ExpirationChecks: config.CertExpirationValidation{
				WarningWindow: time.Hour * 24 * 14,
				ErrorWindow:   time.Hour * 24 * 7,
				CheckInterval: time.Second,
			},
		},
	}

	localStoreRootCAForceTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Frontend: s.frontendConfigRootCAForceTLS,
		},
	}

	localStoreMutualTLSRemoteCluster := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Frontend:       s.frontendConfigPerHostOverrides,
			RemoteClusters: map[string]config.GroupTLS{localhostIPv4: s.frontendConfigPerHostOverrides},
		},
	}

	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err := provider.GetFrontendClientConfig()
	s.NoError(err)
	frontendMutualTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(frontendMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreServerTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	frontendServerTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, nil, noExtraInterceptors, nil)
	s.NotNil(frontendServerTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSSystemWorker.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	frontendSystemWorkerMutualTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(frontendSystemWorkerMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSWithRefresh.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	frontendMutualTLSRefreshFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(frontendMutualTLSRefreshFactory)

	s.frontendMutualTLSRPCFactory = f(frontendMutualTLSFactory)
	s.frontendServerTLSRPCFactory = f(frontendServerTLSFactory)
	s.frontendSystemWorkerMutualTLSRPCFactory = f(frontendSystemWorkerMutualTLSFactory)

	s.dynamicConfigProvider, err = encryption.NewTestDynamicTLSConfigProvider(
		&localStoreMutualTLS.TLS,
		s.frontendRollingCerts,
		s.dynamicCACertPool,
		s.frontendRollingCerts,
		s.dynamicCACertPool,
		s.wrongCACertPool)
	s.NoError(err)
	tlsConfig, err = s.dynamicConfigProvider.GetFrontendClientConfig()
	s.NoError(err)
	dynamicServerTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, s.dynamicConfigProvider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.frontendDynamicTLSFactory = f(dynamicServerTLSFactory)
	s.internodeDynamicTLSFactory = i(dynamicServerTLSFactory)

	s.frontendMutualTLSRPCRefreshFactory = f(frontendMutualTLSRefreshFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreRootCAForceTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	frontendRootCAForceTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(frontendServerTLSFactory)
	s.frontendConfigRootCAForceTLSFactory = f(frontendRootCAForceTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSRemoteCluster.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	remoteClusterMutualTLSRPCFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(remoteClusterMutualTLSRPCFactory)
	s.remoteClusterMutualTLSRPCFactory = r(remoteClusterMutualTLSRPCFactory)
}

func (s *localStoreRPCSuite) setupInternode() {
	localStoreServerTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigServerTLS,
			Frontend:  s.frontendConfigRootCAOnly,
		},
	}

	localStoreMutualTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigMutualTLS,
			Frontend:  s.frontendConfigRootCAOnly,
		},
	}

	localStoreAltMutualTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigAltMutualTLS,
			Frontend:  s.frontendConfigAltRootCAOnly,
		},
	}

	localStoreMutualTLSWithRefresh := *localStoreMutualTLS
	localStoreMutualTLSWithRefresh.TLS.Internode = s.internodeConfigMutualTLSRefresh
	localStoreMutualTLSWithRefresh.TLS.RefreshInterval = time.Second

	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err := provider.GetFrontendClientConfig()
	s.NoError(err)
	internodeMutualTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(internodeMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreServerTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	internodeServerTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(internodeServerTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreAltMutualTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	internodeMutualAltTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(internodeMutualAltTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSWithRefresh.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err = provider.GetFrontendClientConfig()
	s.NoError(err)
	internodeMutualTLSRefreshFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, frontendURL, frontendHTTPURL, tlsConfig, noExtraInterceptors, nil)
	s.NotNil(internodeMutualTLSRefreshFactory)

	s.internodeMutualTLSRPCFactory = i(internodeMutualTLSFactory)
	s.internodeServerTLSRPCFactory = i(internodeServerTLSFactory)
	s.internodeAltMutualTLSRPCFactory = i(internodeMutualAltTLSFactory)
	s.internodeMutualTLSRPCRefreshFactory = i(internodeMutualTLSRefreshFactory)
}

func f(r *rpc.RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: Frontend, RPCFactory: r}
}

func i(r *rpc.RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: Internode, RPCFactory: r}
}

func r(r *rpc.RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: RemoteCluster, RPCFactory: r}
}

func (s *localStoreRPCSuite) TestServerTLS() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.internodeServerTLSRPCFactory, s.internodeServerTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestServerTLSFrontendToFrontend() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.frontendServerTLSRPCFactory, s.frontendServerTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLS() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.internodeMutualTLSRPCFactory, s.internodeMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLSFrontendToFrontend() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.frontendMutualTLSRPCFactory, s.frontendMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLSFrontendToRemoteCluster() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.remoteClusterMutualTLSRPCFactory, s.remoteClusterMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLSButClientInsecure() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.internodeMutualTLSRPCFactory, s.insecureRPCFactory, false)
}

func (s *localStoreRPCSuite) TestServerTLSButClientInsecure() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.internodeServerTLSRPCFactory, s.insecureRPCFactory, false)
}

func (s *localStoreRPCSuite) TestMutualTLSButClientNoCert() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.internodeMutualTLSRPCFactory, s.internodeServerTLSRPCFactory, false)
}

func (s *localStoreRPCSuite) TestServerTLSButClientAddsCert() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.internodeServerTLSRPCFactory, s.internodeMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLSSystemWorker() {
	runHelloWorldTest(s.Suite, localhostIPv4, s.frontendSystemWorkerMutualTLSRPCFactory, s.frontendSystemWorkerMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestDynamicServerTLSFrontend() {
	s.testDynamicServerTLS(localhostIPv4, true)
}

func (s *localStoreRPCSuite) TestDynamicServerTLSInternode() {
	s.testDynamicServerTLS(localhostIPv4, false)
}

func (s *localStoreRPCSuite) TestDynamicServerTLSOverrideFrontend() {
	s.testDynamicServerTLS(localhost, true)
}

func (s *localStoreRPCSuite) TestDynamicServerTLSOverrideInternode() {
	s.testDynamicServerTLS(localhost, false)
}

func (s *localStoreRPCSuite) testDynamicServerTLS(host string, frontend bool) {
	server, client := s.getTestFactory(frontend)
	var index int
	s.dynamicConfigProvider.InternodeClientCertProvider.SetServerName(host)
	s.dynamicConfigProvider.FrontendClientCertProvider.SetServerName(host)
	runHelloWorldMultipleDials(s.Suite, host, server, client, 5,
		func(tlsInfo *credentials.TLSInfo, err error) {
			s.validateTLSInfo(tlsInfo, err, int64((index+1)%2+100))
			index++
		})
}

func (s *localStoreRPCSuite) TestDynamicRootCAFrontend() {
	s.testDynamicRootCA(localhostIPv4, true)
}

func (s *localStoreRPCSuite) TestDynamicRootCAInternode() {
	s.testDynamicRootCA(localhostIPv4, true)
}

func (s *localStoreRPCSuite) TestDynamicRootCAOverrideFrontend() {
	s.testDynamicRootCA(localhost, true)
}

func (s *localStoreRPCSuite) TestDynamicRootCAOverrideInternode() {
	s.testDynamicRootCA(localhost, true)
}

func (s *localStoreRPCSuite) TestCertExpiration() {
	sixHours := time.Hour * 6
	s.testCertExpiration(s.insecureRPCFactory, sixHours, 0)
	s.testCertExpiration(s.internodeMutualTLSRPCFactory, sixHours, 0)
	s.testCertExpiration(s.internodeServerTLSRPCFactory, sixHours, 0)
	s.testCertExpiration(s.internodeAltMutualTLSRPCFactory, sixHours, 0)
	s.testCertExpiration(s.frontendMutualTLSRPCFactory, sixHours, 0)
	s.testCertExpiration(s.frontendServerTLSRPCFactory, sixHours, 0)
	s.testCertExpiration(s.frontendSystemWorkerMutualTLSRPCFactory, sixHours, 0)

	twoDays := time.Hour * 48
	s.testCertExpiration(s.insecureRPCFactory, twoDays, 0)
	s.testCertExpiration(s.internodeMutualTLSRPCFactory, twoDays, 3)
	s.testCertExpiration(s.internodeServerTLSRPCFactory, twoDays, 3)
	s.testCertExpiration(s.internodeAltMutualTLSRPCFactory, twoDays, 2)
	s.testCertExpiration(s.frontendMutualTLSRPCFactory, twoDays, 4)
	s.testCertExpiration(s.frontendServerTLSRPCFactory, twoDays, 2)
	s.testCertExpiration(s.frontendSystemWorkerMutualTLSRPCFactory, twoDays, 6)
}

func (s *localStoreRPCSuite) testCertExpiration(factory *TestFactory, timeWindow time.Duration, nExpiring int) {
	expiring, expired, err := factory.GetTLSConfigProvider().GetExpiringCerts(timeWindow)
	s.NotNil(expiring)
	s.Empty(expired)
	s.NoError(err)
	s.Equal(nExpiring, len(expiring))
}

func (s *localStoreRPCSuite) testDynamicRootCA(host string, frontend bool) {
	server, client := s.getTestFactory(frontend)
	var index int
	valid := true
	runHelloWorldMultipleDials(s.Suite, host, server, client, 5,
		func(tlsInfo *credentials.TLSInfo, err error) {
			if valid {
				s.NoError(err)
			} else {
				s.Error(err)
			}
			index++
			if index == 2 {
				s.dynamicConfigProvider.InternodeClientCertProvider.SwitchToWrongServerRootCACerts()
				s.dynamicConfigProvider.FrontendClientCertProvider.SwitchToWrongServerRootCACerts()
				valid = false
			}
		})
}

func (s *localStoreRPCSuite) getTestFactory(frontend bool) (server *TestFactory, client *TestFactory) {
	if frontend {
		server = s.frontendDynamicTLSFactory
		client = s.frontendDynamicTLSFactory
	} else {
		server = s.internodeDynamicTLSFactory
		client = s.internodeDynamicTLSFactory
	}
	return server, client
}

func (s *localStoreRPCSuite) TestServerTLSRefreshInternode() {
	s.testServerTLSRefresh(s.internodeMutualTLSRPCRefreshFactory, s.internodeRefreshCA, s.internodeRefreshCertDir, internodeServerCertSerialNumber)
}

func (s *localStoreRPCSuite) TestServerTLSRefreshFrontend() {
	s.testServerTLSRefresh(s.frontendMutualTLSRPCRefreshFactory, s.frontendRefreshCA, s.frontendRefreshCertDir, frontendServerCertSerialNumber)
}

func (s *localStoreRPCSuite) testServerTLSRefresh(factory *TestFactory, ca *tls.Certificate, certDir string, serialNumber int64) {
	server, port := startHelloWorldServer(s.Suite, factory)
	defer server.Stop()

	host := localhostIPv4 + ":" + port
	tlsInfo, err := dialHelloAndGetTLSInfo(s.Suite, host, factory, factory.serverUsage)
	s.validateTLSInfo(tlsInfo, err, serialNumber) // serial number of server cert before refresh

	srvrCert, err := testutils.GenerateServerCert(ca, localhostIPv4, serialNumber+100, testutils.CertFilePath(certDir), testutils.KeyFilePath(certDir))
	s.NoError(err)
	s.NotNil(srvrCert)

	time.Sleep(time.Second * 2) // let server refresh certs

	tlsInfo, err = dialHelloAndGetTLSInfo(s.Suite, host, factory, factory.serverUsage)
	s.validateTLSInfo(tlsInfo, err, serialNumber+100) // serial number of server cert after refresh
}

func (s *localStoreRPCSuite) validateTLSInfo(tlsInfo *credentials.TLSInfo, err error, serialNumber int64) {
	s.NoError(err)
	s.NotNil(tlsInfo)
	s.NotNil(tlsInfo.State.PeerCertificates)
	sn := (*tlsInfo.State.PeerCertificates[0].SerialNumber).Int64()
	s.Equal(serialNumber, sn)
}

func (s *localStoreRPCSuite) TestClientForceTLS() {
	options, err := s.frontendConfigRootCAForceTLSFactory.RPCFactory.GetFrontendGRPCServerOptions()
	s.NoError(err)
	s.Nil(options)
}

func (s *localStoreRPCSuite) TestSystemWorkerOnlyConfig() {
	localStoreSystemWorkerOnly := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			SystemWorker: s.systemWorkerOnly,
		},
	}
	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreSystemWorkerOnly.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	tlsConfig, err := provider.GetFrontendClientConfig()
	s.NoError(err)
	s.NotNil(tlsConfig)
	s.NotNil(tlsConfig.GetClientCertificate)
	cert, err := tlsConfig.GetClientCertificate(nil)
	s.NoError(err)
	s.NotNil(cert)
}
