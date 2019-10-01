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
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"

	"go.uber.org/yarpc"
)

type (
	// Service is the interface which must be implemented by all the services
	Service interface {
		// GetHostName returns the name of host running the service
		GetHostName() string

		// Start the service
		Start()

		// Stop stops the service
		Stop()

		GetLogger() log.Logger

		GetThrottledLogger() log.Logger

		GetMetricsClient() metrics.Client

		GetClientBean() client.Bean

		GetTimeSource() clock.TimeSource

		GetDispatcher() *yarpc.Dispatcher

		GetMembershipMonitor() membership.Monitor

		GetHostInfo() *membership.HostInfo

		// GetClusterMetadata returns the service cluster metadata
		GetClusterMetadata() cluster.Metadata

		// GetMessagingClient returns the messaging client against Kafka
		GetMessagingClient() messaging.Client

		GetArchivalMetadata() archiver.ArchivalMetadata

		GetArchiverProvider() provider.ArchiverProvider

		GetPayloadSerializer() persistence.PayloadSerializer
	}
)
