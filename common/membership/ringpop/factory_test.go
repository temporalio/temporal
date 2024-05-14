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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/tests/testutils"
)

type (
	RingpopSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		logger           log.Logger
		internodeCertDir string
		internodeChain   testutils.CertChain

		membershipConfig         config.Membership
		internodeConfigMutualTLS config.GroupTLS
		internodeConfigServerTLS config.GroupTLS

		mutualTLSFactoryA *factory
		mutualTLSFactoryB *factory
		serverTLSFactoryA *factory
		serverTLSFactoryB *factory

		insecureFactory *factory
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
	t.Parallel()
	suite.Run(t, new(RingpopSuite))
}

func (s *RingpopSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.logger = log.NewTestLogger()
	s.controller = gomock.NewController(s.T())

	var err error
	s.internodeCertDir, err = os.MkdirTemp("", "RingpopSuiteInternode")
	s.NoError(err)
	s.internodeChain, err = testutils.GenerateTestChain(s.internodeCertDir, localhostIPv4)
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
			CertData: testutils.ConvertFileToBase64(s.internodeChain.CertPubFile),
			KeyData:  testutils.ConvertFileToBase64(s.internodeChain.CertKeyFile),
		},
		Client: config.ClientTLS{
			RootCAData: []string{testutils.ConvertFileToBase64(s.internodeChain.CaPubFile)},
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

	params := factoryParams{
		Config:      &cfg,
		ServiceName: "test",
		Logger:      log.Logger(log.NewNoopLogger()),
	}
	f, err := newFactory(params)
	s.Nil(err)
	s.NotNil(f)
}

func getHostsConfig() string {
	return `name: "test"
broadcastAddress: "1.2.3.4"
maxJoinDuration: 30s`
}

func (s *RingpopSuite) TestInvalidBroadcastAddress() {
	cfg := config.Membership{
		MaxJoinDuration:  time.Minute,
		BroadcastAddress: "oopsie",
	}
	logger := log.Logger(log.NewNoopLogger())
	params := factoryParams{
		Config:      &cfg,
		ServiceName: "test",
		Logger:      logger,
	}
	_, err := newFactory(params)

	s.ErrorIs(err, errMalformedBroadcastAddress)
	s.ErrorContains(err, "oopsie")
}

func newTestRingpopFactory(
	serviceName primitives.ServiceName,
	logger log.Logger,
	rpcConfig *config.RPC,
	tlsProvider encryption.TLSConfigProvider,
	dc *dynamicconfig.Collection,
) *factory {
	return &factory{
		ServiceName: serviceName,
		Logger:      logger,
		RPCConfig:   rpcConfig,
		TLSFactory:  tlsProvider,
		DC:          dc,
	}
}

func (s *RingpopSuite) TestRingpopMutualTLS() {
	s.NoError(runRingpopTLSTest(&s.Suite, s.mutualTLSFactoryA, s.mutualTLSFactoryB))
}

func (s *RingpopSuite) TestRingpopServerTLS() {
	s.NoError(runRingpopTLSTest(&s.Suite, s.serverTLSFactoryA, s.serverTLSFactoryB))
}

func (s *RingpopSuite) TestRingpopInvalidTLS() {
	s.Error(runRingpopTLSTest(&s.Suite, s.insecureFactory, s.serverTLSFactoryB))
}

func runRingpopTLSTest(s *suite.Suite, serverA *factory, serverB *factory) error {
	// Start two ringpop nodes
	chA := serverA.getTChannel()
	chB := serverB.getTChannel()
	defer chA.Close()
	defer chB.Close()

	// Ping A through B to make sure B's dialer uses TLS to communicate with A
	hostPortA := chA.PeerInfo().HostPort
	if err := chB.Ping(context.Background(), hostPortA); err != nil {
		return err
	}

	// Confirm that A's listener is actually using TLS
	clientTLSConfig, err := serverB.TLSFactory.GetInternodeClientConfig()
	s.NoError(err)

	conn, err := tls.Dial("tcp", hostPortA, clientTLSConfig)
	if err != nil {
		return err
	}
	if conn != nil {
		_ = conn.Close()
	}
	return nil
}

func (s *RingpopSuite) setupInternodeRingpop() {
	provider, err := encryption.NewTLSConfigProviderFromConfig(serverCfgInsecure.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	s.insecureFactory = newTestRingpopFactory("tester", s.logger, rpcTestCfgDefault, provider, dynamicconfig.NewNoopCollection())
	s.NotNil(s.insecureFactory)

	serverTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigServerTLS,
		},
	}

	mutualTLS := &config.Global{
		Membership: s.membershipConfig,
		TLS: config.RootTLS{
			Internode: s.internodeConfigMutualTLS,
		},
	}

	rpcCfgA := &config.RPC{GRPCPort: 0, MembershipPort: 7600, BindOnIP: localhostIPv4}
	rpcCfgB := &config.RPC{GRPCPort: 0, MembershipPort: 7601, BindOnIP: localhostIPv4}

	dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient(map[dynamicconfig.Key]any{
		dynamicconfig.EnableRingpopTLS.Key(): true,
	}), s.logger)

	provider, err = encryption.NewTLSConfigProviderFromConfig(mutualTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	s.mutualTLSFactoryA = newTestRingpopFactory("tester-A", s.logger, rpcCfgA, provider, dc)
	s.NotNil(s.mutualTLSFactoryA)
	s.mutualTLSFactoryB = newTestRingpopFactory("tester-B", s.logger, rpcCfgB, provider, dc)
	s.NotNil(s.mutualTLSFactoryB)

	provider, err = encryption.NewTLSConfigProviderFromConfig(serverTLS.TLS, metrics.NoopMetricsHandler, s.logger, nil)
	s.NoError(err)
	s.serverTLSFactoryA = newTestRingpopFactory("tester-A", s.logger, rpcCfgA, provider, dc)
	s.NotNil(s.serverTLSFactoryA)
	s.serverTLSFactoryB = newTestRingpopFactory("tester-B", s.logger, rpcCfgB, provider, dc)
	s.NotNil(s.serverTLSFactoryB)
}
