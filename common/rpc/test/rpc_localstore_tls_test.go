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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
)

const (
	internodeServerCertSerialNumber = 100
	frontendServerCertSerialNumber  = 150
)

type localStoreRPCSuite struct {
	*require.Assertions
	suite.Suite

	controller *gomock.Controller

	logger log.Logger

	insecureRPCFactory                      *TestFactory
	internodeMutualTLSRPCFactory            *TestFactory
	internodeServerTLSRPCFactory            *TestFactory
	ringpopMutualTLSRPCFactoryA             *TestFactory
	ringpopMutualTLSRPCFactoryB             *TestFactory
	ringpopServerTLSRPCFactoryA             *TestFactory
	ringpopServerTLSRPCFactoryB             *TestFactory
	internodeAltMutualTLSRPCFactory         *TestFactory
	frontendMutualTLSRPCFactory             *TestFactory
	frontendServerTLSRPCFactory             *TestFactory
	frontendSystemWorkerMutualTLSRPCFactory *TestFactory
	frontendDynamicTLSFactory               *TestFactory
	internodeDynamicTLSFactory              *TestFactory
	internodeMutualTLSRPCRefreshFactory     *TestFactory
	frontendMutualTLSRPCRefreshFactory      *TestFactory

	internodeCertDir        string
	frontendCertDir         string
	frontendAltCertDir      string
	frontendRollingCertDir  string
	internodeRefreshCertDir string
	frontendRefreshCertDir  string

	internodeChain        CertChain
	frontendChain         CertChain
	frontendAltChain      CertChain
	frontendRollingCerts  []*tls.Certificate
	internodeRefreshChain CertChain
	internodeRefreshCA    *tls.Certificate
	frontendRefreshChain  CertChain
	frontendRefreshCA     *tls.Certificate

	frontendClientCertDir string
	frontendClientChain   CertChain

	membershipConfig               config.Membership
	frontendConfigServerTLS        config.GroupTLS
	frontendConfigMutualTLS        config.GroupTLS
	frontendConfigPerHostOverrides config.GroupTLS
	frontendConfigRootCAOnly       config.GroupTLS
	frontendConfigAltRootCAOnly    config.GroupTLS
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

type CertChain struct {
	CertPubFile string
	CertKeyFile string
	CaPubFile   string
}

func TestLocalStoreTLSSuite(t *testing.T) {
	suite.Run(t, &localStoreRPCSuite{})
}

func (s *localStoreRPCSuite) TearDownSuite() {
	_ = os.RemoveAll(s.internodeCertDir)
	_ = os.RemoveAll(s.frontendCertDir)
}
func (s *localStoreRPCSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.logger = log.NewTestLogger()

	provider, err := encryption.NewTLSConfigProviderFromConfig(serverCfgInsecure.TLS, nil, s.logger, nil)
	s.NoError(err)
	insecureFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(insecureFactory)
	s.insecureRPCFactory = i(insecureFactory)

	s.frontendCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontend")
	s.NoError(err)
	s.frontendChain = s.GenerateTestChain(s.frontendCertDir, localhostIPv4)

	s.internodeCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteInternode")
	s.NoError(err)
	s.internodeChain = s.GenerateTestChain(s.internodeCertDir, localhostIPv4)

	s.frontendAltCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendAlt")
	s.NoError(err)
	s.frontendAltChain = s.GenerateTestChain(s.frontendAltCertDir, localhost)

	s.frontendClientCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendClient")
	s.NoError(err)
	s.frontendClientChain = s.GenerateTestChain(s.frontendClientCertDir, localhostIPv4)

	s.frontendRollingCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendRolling")
	s.NoError(err)
	s.frontendRollingCerts, s.dynamicCACertPool, s.wrongCACertPool = s.GenerateTestCerts(s.frontendRollingCertDir, localhostIPv4, 2)

	s.internodeRefreshCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteInternodeRefresh")
	s.NoError(err)
	s.internodeRefreshChain, s.internodeRefreshCA = s.GenerateTestChainWithSN(s.internodeRefreshCertDir, localhostIPv4, internodeServerCertSerialNumber)

	s.frontendRefreshCertDir, err = os.MkdirTemp("", "localStoreRPCSuiteFrontendRefresh")
	s.NoError(err)
	s.frontendRefreshChain, s.frontendRefreshCA = s.GenerateTestChainWithSN(s.frontendRefreshCertDir, localhostIPv4, frontendServerCertSerialNumber)

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
			RootCAData: []string{convertFileToBase64(s.frontendChain.CaPubFile)},
		},
	}
	s.frontendConfigAltRootCAOnly = config.GroupTLS{
		Server: config.ServerTLS{
			RequireClientAuth: true,
		},
		Client: config.ClientTLS{
			RootCAData: []string{convertFileToBase64(s.frontendAltChain.CaPubFile)},
		},
	}
	s.frontendConfigSystemWorker = config.WorkerTLS{
		CertFile: s.frontendClientChain.CertPubFile,
		KeyFile:  s.frontendClientChain.CertKeyFile,
		Client: config.ClientTLS{
			RootCAFiles: []string{s.frontendChain.CaPubFile},
		},
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
			CertData: convertFileToBase64(s.internodeChain.CertPubFile),
			KeyData:  convertFileToBase64(s.internodeChain.CertKeyFile),
		},
		Client: config.ClientTLS{
			RootCAData: []string{convertFileToBase64(s.internodeChain.CaPubFile)},
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

func mutualGroupTLSFromChain(chain CertChain) config.GroupTLS {

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
	s.setupInternodeRingpop()
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

	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	frontendMutualTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(frontendMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreServerTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	frontendServerTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(frontendServerTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSSystemWorker.TLS, nil, s.logger, nil)
	s.NoError(err)
	frontendSystemWorkerMutualTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(frontendSystemWorkerMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSWithRefresh.TLS, nil, s.logger, nil)
	s.NoError(err)
	frontendMutualTLSRefreshFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
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
	dynamicServerTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, s.dynamicConfigProvider, dynamicconfig.NewNoopCollection())
	s.frontendDynamicTLSFactory = f(dynamicServerTLSFactory)
	s.internodeDynamicTLSFactory = i(dynamicServerTLSFactory)

	s.frontendMutualTLSRPCRefreshFactory = f(frontendMutualTLSRefreshFactory)
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

	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	internodeMutualTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(internodeMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreServerTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	internodeServerTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(internodeServerTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreAltMutualTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	internodeMutualAltTLSFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(internodeMutualAltTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLSWithRefresh.TLS, nil, s.logger, nil)
	s.NoError(err)
	internodeMutualTLSRefreshFactory := rpc.NewFactory(rpcTestCfgDefault, "tester", s.logger, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(internodeMutualTLSRefreshFactory)

	s.internodeMutualTLSRPCFactory = i(internodeMutualTLSFactory)
	s.internodeServerTLSRPCFactory = i(internodeServerTLSFactory)
	s.internodeAltMutualTLSRPCFactory = i(internodeMutualAltTLSFactory)
	s.internodeMutualTLSRPCRefreshFactory = i(internodeMutualTLSRefreshFactory)
}

func (s *localStoreRPCSuite) setupInternodeRingpop() {
	ringpopServerTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigServerTLS,
		},
	}

	ringpopMutualTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigMutualTLS,
		},
	}

	rpcCfgA := &config.RPC{GRPCPort: 0, MembershipPort: 7600, BindOnIP: localhostIPv4}
	rpcCfgB := &config.RPC{GRPCPort: 0, MembershipPort: 7601, BindOnIP: localhostIPv4}

	dcClient := dynamicconfig.NewMockClient(s.controller)
	dcClient.EXPECT().GetBoolValue(dynamicconfig.EnableRingpopTLS, gomock.Any(), false).Return(true, nil).AnyTimes()
	dc := dynamicconfig.NewCollection(dcClient, s.logger)

	provider, err := encryption.NewTLSConfigProviderFromConfig(ringpopMutualTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	ringpopMutualTLSFactoryA := rpc.NewFactory(rpcCfgA, "tester-A", s.logger, provider, dc)
	s.NotNil(ringpopMutualTLSFactoryA)
	ringpopMutualTLSFactoryB := rpc.NewFactory(rpcCfgB, "tester-B", s.logger, provider, dc)
	s.NotNil(ringpopMutualTLSFactoryB)

	provider, err = encryption.NewTLSConfigProviderFromConfig(ringpopServerTLS.TLS, nil, s.logger, nil)
	s.NoError(err)
	ringpopServerTLSFactoryA := rpc.NewFactory(rpcCfgA, "tester-A", s.logger, provider, dc)
	s.NotNil(ringpopServerTLSFactoryA)
	ringpopServerTLSFactoryB := rpc.NewFactory(rpcCfgB, "tester-B", s.logger, provider, dc)
	s.NotNil(ringpopServerTLSFactoryB)

	s.ringpopMutualTLSRPCFactoryA = i(ringpopMutualTLSFactoryA)
	s.ringpopMutualTLSRPCFactoryB = i(ringpopMutualTLSFactoryB)
	s.ringpopServerTLSRPCFactoryA = i(ringpopServerTLSFactoryA)
	s.ringpopServerTLSRPCFactoryB = i(ringpopServerTLSFactoryB)
}

func (s *localStoreRPCSuite) GenerateTestChain(tempDir string, commonName string) CertChain {

	chain, _ := s.GenerateTestChainWithSN(tempDir, commonName, 0)
	return chain
}

func (s *localStoreRPCSuite) GenerateTestChainWithSN(tempDir string, commonName string, serialNumber int64,
) (CertChain, *tls.Certificate) {

	caPubFile := caFilePath(tempDir)
	certPubFile := certFilePath(tempDir)
	certPrivFile := keyFilePath(tempDir)

	caCert := s.GenerateSelfSignedCA(caPubFile)
	s.GenerateServerCert(caCert, commonName, serialNumber, certPubFile, certPrivFile)

	return CertChain{CaPubFile: caPubFile, CertPubFile: certPubFile, CertKeyFile: certPrivFile}, caCert
}

func (s *localStoreRPCSuite) GenerateTestCerts(tempDir string, commonName string, num int,
) (certs []*tls.Certificate, caPool *x509.CertPool, wrongCAPool *x509.CertPool) {

	caCert := s.GenerateSelfSignedCA(caFilePath(tempDir))
	caPool = s.GenerateSelfSignedCAPool(caCert)

	chains := make([]*tls.Certificate, num)
	for i := 0; i < num; i++ {
		certPubFile := tempDir + fmt.Sprintf("/cert_pub_%d.pem", i)
		certPrivFile := tempDir + fmt.Sprintf("/cert_priv_%d.pem", i)
		cert := s.GenerateServerCert(caCert, commonName, int64(i+100), certPubFile, certPrivFile)
		chains[i] = cert
	}

	wrongCACert := s.GenerateSelfSignedCA(caFilePath(tempDir))
	wrongCAPool = s.GenerateSelfSignedCAPool(wrongCACert)

	return chains, caPool, wrongCAPool
}

func (s *localStoreRPCSuite) GenerateSelfSignedCAPool(caCert *tls.Certificate) *x509.CertPool {
	caPEM := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Certificate[0],
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(s.pemEncodeToBytes(caPEM))
	return caPool
}

func (s *localStoreRPCSuite) GenerateSelfSignedCA(filePath string) *tls.Certificate {
	caCert, err := encryption.GenerateSelfSignedX509CA("undefined", nil, 512)
	s.NoError(err)

	s.pemEncodeToFile(filePath, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Certificate[0],
	})
	return caCert
}

func (s *localStoreRPCSuite) GenerateServerCert(
	caCert *tls.Certificate,
	commonName string,
	serialNumber int64,
	certPubFile string,
	certPrivFile string) *tls.Certificate {

	serverCert, privKey, err := encryption.GenerateServerX509UsingCAAndSerialNumber(commonName, serialNumber, caCert)
	s.NoError(err)

	certPEM := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCert.Certificate[0],
	}
	s.pemEncodeToFile(certPubFile, certPEM)

	keyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	}
	s.pemEncodeToFile(certPrivFile, keyPEM)

	cert, err := tls.X509KeyPair(s.pemEncodeToBytes(certPEM), s.pemEncodeToBytes(keyPEM))
	s.NoError(err)
	return &cert
}

func (s *localStoreRPCSuite) pemEncodeToFile(file string, block *pem.Block) {
	bytes := s.pemEncodeToBytes(block)
	err := os.WriteFile(file, bytes, os.FileMode(0644))
	s.NoError(err)
}

func (s *localStoreRPCSuite) pemEncodeToBytes(block *pem.Block) []byte {
	pemBuffer := new(bytes.Buffer)
	err := pem.Encode(pemBuffer, block)
	s.NoError(err)
	return pemBuffer.Bytes()
}

func f(r *rpc.RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: Frontend, RPCFactory: r}
}

func i(r *rpc.RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: Internode, RPCFactory: r}
}

func convertFileToBase64(file string) string {
	fileBytes, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return base64.StdEncoding.EncodeToString(fileBytes)
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
	if len(expired) > 0 {
	}
	s.NotNil(expiring)
	s.Nil(err)
	s.Equal(nExpiring, len(expiring))
}

func (s *localStoreRPCSuite) testDynamicRootCA(host string, frontend bool) {

	server, client := s.getTestFactory(frontend)
	var index int
	var valid = true
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

	s.GenerateServerCert(ca, localhostIPv4, serialNumber+100, certFilePath(certDir), keyFilePath(certDir))

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

func (s *localStoreRPCSuite) TestRingpopMutualTLS() {
	runRingpopTLSTest(s.Suite, s.logger, s.ringpopMutualTLSRPCFactoryA, s.ringpopMutualTLSRPCFactoryB, false)
}

func (s *localStoreRPCSuite) TestRingpopServerTLS() {
	runRingpopTLSTest(s.Suite, s.logger, s.ringpopServerTLSRPCFactoryA, s.ringpopServerTLSRPCFactoryB, false)
}

func (s *localStoreRPCSuite) TestRingpopInvalidTLS() {
	runRingpopTLSTest(s.Suite, s.logger, s.insecureRPCFactory, s.ringpopServerTLSRPCFactoryB, true)
}

func runRingpopTLSTest(s suite.Suite, logger log.Logger, serverA *TestFactory, serverB *TestFactory, expectError bool) {
	// Start two ringpop nodes
	chA := serverA.GetRingpopChannel()
	chB := serverB.GetRingpopChannel()
	defer chA.Close()
	defer chB.Close()

	// Ping A through B to make sure B's dialer uses TLS to communicate with A
	hostPortA := chA.PeerInfo().HostPort
	err := chB.Ping(context.Background(), hostPortA)
	if expectError {
		s.Error(err)
	} else {
		s.NoError(err)
	}

	// Confirm that A's listener is actually using TLS
	clientTLSConfig, err := serverB.GetInternodeClientTlsConfig()
	s.NoError(err)

	conn, err := tls.Dial("tcp", hostPortA, clientTLSConfig)
	if conn != nil {
		_ = conn.Close()
	}
	if expectError {
		s.Error(err)
	} else {
		s.NoError(err)
	}
}
