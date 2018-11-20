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
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"

	"github.com/uber-common/bark"

	"go.uber.org/yarpc"
)

type (
	// serviceTestBase is the test implementation used for testing
	serviceTestBase struct {
		hostInfo          *membership.HostInfo
		clusterMetadata   cluster.Metadata
		messagingClient   messaging.Client
		kafkaClient       messaging.Client
		clientFactory     client.Factory
		membershipMonitor membership.Monitor

		metrics metrics.Client
		logger  bark.Logger
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
func NewTestService(clusterMetadata cluster.Metadata, messagingClient messaging.Client, metrics metrics.Client,
	logger bark.Logger) Service {
	return &serviceTestBase{
		hostInfo:        testHostInfo,
		clusterMetadata: clusterMetadata,
		messagingClient: messagingClient,
		metrics:         metrics,
		logger:          logger,
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

// GetLogger returns the logger for service
func (s *serviceTestBase) GetLogger() bark.Logger {
	return s.logger
}

// GetMetricsClient returns the metric client for service
func (s *serviceTestBase) GetMetricsClient() metrics.Client {
	return s.metrics
}

// GetClientFactory returns the ClientFactory for service
func (s *serviceTestBase) GetClientFactory() client.Factory {
	return s.clientFactory
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
