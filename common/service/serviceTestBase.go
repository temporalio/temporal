// Copyright (c) 2017 Uber Technologies, Inc.
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

package service

import (
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"

	"go.uber.org/yarpc"
	"go.uber.org/zap"
)

type (
	// serviceTestBase is the test implementation used for testing
	serviceTestBase struct {
		hostInfo          *membership.HostInfo
		clusterMetadata   cluster.Metadata
		messagingClient   messaging.Client
		kafkaClient       messaging.Client
		clientBean        client.Bean
		timeSource        clock.TimeSource
		membershipMonitor membership.Monitor
		archivalMetadata  archiver.ArchivalMetadata
		archiverProvider  provider.ArchiverProvider
		serializer        persistence.PayloadSerializer

		metrics metrics.Client
		logger  log.Logger
	}
)

var _ Service = (*serviceTestBase)(nil)

const (
	testHostName = "test_host"
)

var (
	testHostInfo = membership.NewHostInfo(testHostName, nil)
)

// NewTestService is the new service instance created for testing
func NewTestService(
	clusterMetadata cluster.Metadata,
	messagingClient messaging.Client,
	metrics metrics.Client,
	clientBean client.Bean,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
	serializer persistence.PayloadSerializer,
) Service {

	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger := loggerimpl.NewLogger(zapLogger)

	return &serviceTestBase{
		hostInfo:         testHostInfo,
		clusterMetadata:  clusterMetadata,
		messagingClient:  messagingClient,
		metrics:          metrics,
		clientBean:       clientBean,
		timeSource:       clock.NewRealTimeSource(),
		logger:           logger,
		archivalMetadata: archivalMetadata,
		archiverProvider: archiverProvider,
		serializer:       serializer,
	}
}

// GetHostName returns the name of host running the service
func (s *serviceTestBase) GetHostName() string {
	return testHostName
}

// Start the service
func (s *serviceTestBase) Start() {
}

// Stop stops the service
func (s *serviceTestBase) Stop() {
}

func (s *serviceTestBase) GetLogger() log.Logger {
	return s.logger
}

func (s *serviceTestBase) GetThrottledLogger() log.Logger {
	return s.logger
}

// GetMetricsClient returns the metric client for service
func (s *serviceTestBase) GetMetricsClient() metrics.Client {
	return s.metrics
}

// GetClientBean returns the client bean used by service
func (s *serviceTestBase) GetClientBean() client.Bean {
	return s.clientBean
}

// GetMetricsClient returns the metric client for service
func (s *serviceTestBase) GetTimeSource() clock.TimeSource {
	return s.timeSource
}

// GetDispatcher returns the dispatcher used by service
func (s *serviceTestBase) GetDispatcher() *yarpc.Dispatcher {
	return nil
}

// GetMembershipMonitor returns the membership monitor used by service
func (s *serviceTestBase) GetMembershipMonitor() membership.Monitor {
	return s.membershipMonitor
}

// GetHostInfo returns host info
func (s *serviceTestBase) GetHostInfo() *membership.HostInfo {
	return s.hostInfo
}

// GetClusterMetadata returns the service cluster metadata
func (s *serviceTestBase) GetClusterMetadata() cluster.Metadata {
	return s.clusterMetadata
}

// GetMessagingClient returns the messaging client against Kafka
func (s *serviceTestBase) GetMessagingClient() messaging.Client {
	return s.messagingClient
}

// GetArchivalMetadata returns the cluster level archival metadata
func (s *serviceTestBase) GetArchivalMetadata() archiver.ArchivalMetadata {
	return s.archivalMetadata
}

// GetArchivalProvider returns the archiver provider used by the service
func (s *serviceTestBase) GetArchiverProvider() provider.ArchiverProvider {
	return s.archiverProvider
}

// GetPayloadSerializer returns the payload serializer used by the service
func (s *serviceTestBase) GetPayloadSerializer() persistence.PayloadSerializer {
	return s.serializer
}
