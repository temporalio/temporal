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
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/service/config"
)

type localStoreRPCSuite struct {
	*require.Assertions
	suite.Suite

	logger log.Logger

	insecureRPCFactory           *TestFactory
	internodeMutualTLSRPCFactory *TestFactory
	internodeServerTLSRPCFactory *TestFactory
	frontendMutualTLSRPCFactory  *TestFactory
	frontendServerTLSRPCFactory  *TestFactory

	internodeCertDir string
	frontendCertDir  string

	frontendChain  CertChain
	internodeChain CertChain
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
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)

	provider, err := encryption.NewTLSConfigProviderFromConfig(serverCfgInsecure.TLS)
	s.NoError(err)
	insecureFactory := NewFactory(rpcTestCfgDefault, "tester", s.logger, provider)
	s.NotNil(insecureFactory)
	s.insecureRPCFactory = i(insecureFactory)

	s.frontendCertDir, err = ioutil.TempDir("", "localStoreRPCSuiteFrontend")
	s.NoError(err)
	s.frontendChain = s.GenerateTestChain(s.frontendCertDir)

	s.internodeCertDir, err = ioutil.TempDir("", "localStoreRPCSuiteInternode")
	s.NoError(err)
	s.internodeChain = s.GenerateTestChain(s.internodeCertDir)
}

func (s *localStoreRPCSuite) SetupTest() {
	s.setupInternode(s.internodeChain, s.frontendChain)
	s.setupFrontend(s.internodeChain, s.frontendChain)
}

func (s *localStoreRPCSuite) setupFrontend(internodeChain CertChain, frontendChain CertChain) {
	localStoreServerTLS := &config.Global{
		Membership: config.Membership{
			Name:             "its-a-me-mario",
			MaxJoinDuration:  5,
			BroadcastAddress: "127.0.0.1",
		},
		TLS: config.RootTLS{
			Frontend: config.GroupTLS{
				Server: config.ServerTLS{
					CertFile: frontendChain.CertPubFile,
					KeyFile:  frontendChain.CertKeyFile,
				},
				Client: config.ClientTLS{RootCAFiles: []string{frontendChain.CaPubFile}},
			},
		},
	}

	localStoreMutualTLS := &config.Global{
		Membership: config.Membership{
			Name:             "its-a-me-mario",
			MaxJoinDuration:  5,
			BroadcastAddress: "127.0.0.1",
		},
		TLS: config.RootTLS{
			Frontend: config.GroupTLS{
				Server: config.ServerTLS{
					CertFile:          frontendChain.CertPubFile,
					KeyFile:           frontendChain.CertKeyFile,
					ClientCAFiles:     []string{internodeChain.CaPubFile},
					RequireClientAuth: true,
				},
				Client: config.ClientTLS{
					RootCAFiles: []string{frontendChain.CaPubFile},
				},
			},
		},
	}

	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLS.TLS)
	s.NoError(err)
	frontendMutualTLSFactory := NewFactory(rpcTestCfgDefault, "tester", s.logger, provider)
	s.NotNil(frontendMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreServerTLS.TLS)
	s.NoError(err)
	frontendServerTLSFactory := NewFactory(rpcTestCfgDefault, "tester", s.logger, provider)
	s.NoError(err)
	s.NotNil(frontendServerTLSFactory)

	s.frontendMutualTLSRPCFactory = f(frontendMutualTLSFactory)
	s.frontendServerTLSRPCFactory = f(frontendServerTLSFactory)
}

func (s *localStoreRPCSuite) setupInternode(internodeChain CertChain, frontendChain CertChain) {
	localStoreServerTLS := &config.Global{
		Membership: config.Membership{
			Name:             "its-a-me-mario",
			MaxJoinDuration:  5,
			BroadcastAddress: "127.0.0.1",
		},
		TLS: config.RootTLS{
			Internode: config.GroupTLS{
				Server: config.ServerTLS{
					CertFile: internodeChain.CertPubFile,
					KeyFile:  internodeChain.CertKeyFile,
				},
				Client: config.ClientTLS{
					RootCAFiles: []string{internodeChain.CaPubFile},
				},
			},
			Frontend: config.GroupTLS{
				Client: config.ClientTLS{
					RootCAFiles: []string{frontendChain.CaPubFile},
				},
			},
		},
	}

	localStoreMutualTLS := &config.Global{
		Membership: config.Membership{
			Name:             "its-a-me-mario",
			MaxJoinDuration:  5,
			BroadcastAddress: "127.0.0.1",
		},
		TLS: config.RootTLS{
			Internode: config.GroupTLS{
				Server: config.ServerTLS{
					CertFile:          internodeChain.CertPubFile,
					KeyFile:           internodeChain.CertKeyFile,
					ClientCAFiles:     []string{internodeChain.CaPubFile},
					RequireClientAuth: true,
				},
				Client: config.ClientTLS{
					RootCAFiles: []string{internodeChain.CaPubFile},
				},
			},
			Frontend: config.GroupTLS{
				Server: config.ServerTLS{
					RequireClientAuth: true,
				},
				Client: config.ClientTLS{
					RootCAFiles: []string{frontendChain.CaPubFile},
				},
			},
		},
	}

	provider, err := encryption.NewTLSConfigProviderFromConfig(localStoreMutualTLS.TLS)
	s.NoError(err)
	internodeMutualTLSFactory := NewFactory(rpcTestCfgDefault, "tester", s.logger, provider)
	s.NotNil(internodeMutualTLSFactory)

	provider, err = encryption.NewTLSConfigProviderFromConfig(localStoreServerTLS.TLS)
	s.NoError(err)
	internodeServerTLSFactory := NewFactory(rpcTestCfgDefault, "tester", s.logger, provider)
	s.NoError(err)
	s.NotNil(internodeServerTLSFactory)

	s.internodeMutualTLSRPCFactory = i(internodeMutualTLSFactory)
	s.internodeServerTLSRPCFactory = i(internodeServerTLSFactory)
}

func (s *localStoreRPCSuite) GenerateTestChain(tempDir string) CertChain {
	caCert, err := encryption.GenerateSelfSignedX509CA("undefined", nil, 512)
	s.NoError(err)

	serverCert, privKey, err := encryption.GenerateServerX509UsingCA("127.0.0.1", caCert)
	s.NoError(err)

	caPubFile := tempDir + "/ca_pub.pem"
	certPubFile := tempDir + "/cert_pub.pem"
	certPrivFile := tempDir + "/cert_priv.pem"

	s.pemEncodeToFile(caPubFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Certificate[0],
	})

	s.pemEncodeToFile(certPubFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCert.Certificate[0],
	})

	s.pemEncodeToFile(certPrivFile, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	})

	return CertChain{CaPubFile: caPubFile, CertPubFile: certPubFile, CertKeyFile: certPrivFile}
}

func (s *localStoreRPCSuite) pemEncodeToFile(file string, block *pem.Block) {
	pemBuffer := new(bytes.Buffer)
	err := pem.Encode(pemBuffer, block)
	s.NoError(err)
	err = ioutil.WriteFile(file, pemBuffer.Bytes(), os.FileMode(0644))
	s.NoError(err)
}

func f(r *RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: Frontend, RPCFactory: r}
}

func i(r *RPCFactory) *TestFactory {
	return &TestFactory{serverUsage: Internode, RPCFactory: r}
}

func (s *localStoreRPCSuite) TestServerTLS() {
	runHelloWorldTest(s.Suite, s.internodeServerTLSRPCFactory, s.internodeServerTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestServerTLSInternodeToFrontend() {
	runHelloWorldTest(s.Suite, s.frontendServerTLSRPCFactory, s.internodeServerTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLS() {
	runHelloWorldTest(s.Suite, s.internodeMutualTLSRPCFactory, s.internodeMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLSInternodeToFrontend() {
	runHelloWorldTest(s.Suite, s.frontendMutualTLSRPCFactory, s.internodeMutualTLSRPCFactory, true)
}

func (s *localStoreRPCSuite) TestMutualTLSButClientInsecure() {
	runHelloWorldTest(s.Suite, s.internodeMutualTLSRPCFactory, s.insecureRPCFactory, false)
}

func (s *localStoreRPCSuite) TestServerTLSButClientInsecure() {
	runHelloWorldTest(s.Suite, s.internodeServerTLSRPCFactory, s.insecureRPCFactory, false)
}

func (s *localStoreRPCSuite) TestMutualTLSButClientNoCert() {
	runHelloWorldTest(s.Suite, s.internodeMutualTLSRPCFactory, s.internodeServerTLSRPCFactory, false)
}

func (s *localStoreRPCSuite) TestServerTLSButClientAddsCert() {
	runHelloWorldTest(s.Suite, s.internodeServerTLSRPCFactory, s.internodeMutualTLSRPCFactory, true)
}
