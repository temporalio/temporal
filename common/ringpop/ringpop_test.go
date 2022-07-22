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

package ringpop

import (
	"context"
	"crypto/tls"
	"os"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/tests/testhelper"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
)

type (
	RingpopSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		logger           log.Logger
		internodeCertDir string
		internodeChain   testhelper.CertChain

		membershipConfig         config.Membership
		internodeConfigMutualTLS config.GroupTLS
		internodeConfigServerTLS config.GroupTLS

		ringpopMutualTLSFactoryA *ringpopFactory
		ringpopMutualTLSFactoryB *ringpopFactory
		ringpopServerTLSFactoryA *ringpopFactory
		ringpopServerTLSFactoryB *ringpopFactory

		insecureFactory *ringpopFactory
	}
)

const (
	localhostIPv4 = "127.0.0.1"
)

var (
	rpcTestCfgDefault = &config.RPC{
		GRPCPort:       0,
		MembershipPort: 7600,
		BindOnIP:       localhostIPv4,
	}
	serverCfgInsecure = &config.Global{
		Membership: config.Membership{
			MaxJoinDuration:  5,
			BroadcastAddress: localhostIPv4,
		},
	}
)

func TestRingpopSuite(t *testing.T) {
	suite.Run(t, new(RingpopSuite))
}

func (s *RingpopSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.logger = log.NewTestLogger()
	s.controller = gomock.NewController(s.T())

	var err error
	s.internodeCertDir, err = os.MkdirTemp("", "RingpopSuiteInternode")
	s.NoError(err)
	s.internodeChain, err = testhelper.GenerateTestChain(s.internodeCertDir, localhostIPv4)
	s.NoError(err)

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
			CertData: testhelper.ConvertFileToBase64(s.internodeChain.CertPubFile),
			KeyData:  testhelper.ConvertFileToBase64(s.internodeChain.CertKeyFile),
		},
		Client: config.ClientTLS{
			RootCAData: []string{testhelper.ConvertFileToBase64(s.internodeChain.CaPubFile)},
		},
	}

	s.setupInternodeRingpop()
}

func (s *RingpopSuite) TearDownSuite() {
	_ = os.RemoveAll(s.internodeCertDir)
}

func (s *RingpopSuite) TestHostsMode() {
	var cfg config.Membership
	err := yaml.Unmarshal([]byte(getHostsConfig()), &cfg)
	s.Nil(err)
	s.Equal("1.2.3.4", cfg.BroadcastAddress)
	s.Equal(time.Second*30, cfg.MaxJoinDuration)
	err = ValidateRingpopConfig(&cfg)
	s.Nil(err)
	f, err := NewRingpopFactory(&cfg, "test", nil, log.NewNoopLogger(), nil, nil, nil, nil)
	s.Nil(err)
	s.NotNil(f)
}

func (s *RingpopSuite) TestInvalidConfig() {
	var cfg config.Membership
	cfg.MaxJoinDuration = time.Minute
	s.NoError(ValidateRingpopConfig(&cfg))
	cfg.BroadcastAddress = "sjhdfskdjhf"
	s.Error(ValidateRingpopConfig(&cfg))
}

func getHostsConfig() string {
	return `name: "test"
broadcastAddress: "1.2.3.4"
maxJoinDuration: 30s`
}

func newTestRingpopFactory(
	serviceName string,
	logger log.Logger,
	rpcConfig *config.RPC,
	tlsProvider encryption.TLSConfigProvider,
	dc *dynamicconfig.Collection,
) *ringpopFactory {
	return &ringpopFactory{
		config:          nil,
		serviceName:     serviceName,
		servicePortMap:  nil,
		logger:          logger,
		metadataManager: nil,
		rpcConfig:       rpcConfig,
		tlsFactory:      tlsProvider,
		dc:              dc,
	}
}

func (s *RingpopSuite) TestRingpopMutualTLS() {
	runRingpopTLSTest(s.Suite, s.logger, s.ringpopMutualTLSFactoryA, s.ringpopMutualTLSFactoryB, false)
}

func (s *RingpopSuite) TestRingpopServerTLS() {
	runRingpopTLSTest(s.Suite, s.logger, s.ringpopServerTLSFactoryA, s.ringpopServerTLSFactoryB, false)
}

func (s *RingpopSuite) TestRingpopInvalidTLS() {
	runRingpopTLSTest(s.Suite, s.logger, s.insecureFactory, s.ringpopServerTLSFactoryB, true)
}

func runRingpopTLSTest(s suite.Suite, logger log.Logger, serverA *ringpopFactory, serverB *ringpopFactory, expectError bool) {
	// Start two ringpop nodes
	chA := serverA.getTChannel()
	chB := serverB.getTChannel()
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
	clientTLSConfig, err := serverB.tlsFactory.GetInternodeClientConfig()
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

func (s *RingpopSuite) setupInternodeRingpop() {
	provider, err := encryption.NewTLSConfigProviderFromConfig(serverCfgInsecure.TLS, metrics.NoopClient, s.logger, nil)
	s.NoError(err)
	s.insecureFactory = newTestRingpopFactory("tester", s.logger, rpcTestCfgDefault, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(s.insecureFactory)

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
	dcClient.EXPECT().GetBoolValue(dynamicconfig.Key(dynamicconfig.EnableRingpopTLS), gomock.Any(), false).Return(true, nil).AnyTimes()
	dc := dynamicconfig.NewCollection(dcClient, s.logger)

	provider, err = encryption.NewTLSConfigProviderFromConfig(ringpopMutualTLS.TLS, metrics.NoopClient, s.logger, nil)
	s.NoError(err)
	s.ringpopMutualTLSFactoryA = newTestRingpopFactory("tester-A", s.logger, rpcCfgA, provider, dc)
	s.NotNil(s.ringpopMutualTLSFactoryA)
	s.ringpopMutualTLSFactoryB = newTestRingpopFactory("tester-B", s.logger, rpcCfgB, provider, dc)
	s.NotNil(s.ringpopMutualTLSFactoryB)

	provider, err = encryption.NewTLSConfigProviderFromConfig(ringpopServerTLS.TLS, metrics.NoopClient, s.logger, nil)
	s.NoError(err)
	s.ringpopServerTLSFactoryA = newTestRingpopFactory("tester-A", s.logger, rpcCfgA, provider, dc)
	s.NotNil(s.ringpopServerTLSFactoryA)
	s.ringpopServerTLSFactoryB = newTestRingpopFactory("tester-B", s.logger, rpcCfgB, provider, dc)
	s.NotNil(s.ringpopServerTLSFactoryB)
}
