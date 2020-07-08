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

package resource

import (
	"net"

	sdkclient "go.temporal.io/temporal/client"

	"go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
)

type (
	// Resource is the interface which expose common resources
	Resource interface {
		common.Daemon

		// static infos

		GetServiceName() string
		GetHostName() string
		GetHostInfo() *membership.HostInfo
		GetArchivalMetadata() archiver.ArchivalMetadata
		GetClusterMetadata() cluster.Metadata

		// other common resources

		GetNamespaceCache() cache.NamespaceCache
		GetTimeSource() clock.TimeSource
		GetPayloadSerializer() persistence.PayloadSerializer
		GetMetricsClient() metrics.Client
		GetArchiverProvider() provider.ArchiverProvider
		GetMessagingClient() messaging.Client

		// membership infos

		GetMembershipMonitor() membership.Monitor
		GetFrontendServiceResolver() membership.ServiceResolver
		GetMatchingServiceResolver() membership.ServiceResolver
		GetHistoryServiceResolver() membership.ServiceResolver
		GetWorkerServiceResolver() membership.ServiceResolver

		// internal services clients

		GetSDKClient() sdkclient.Client
		GetFrontendRawClient() frontend.Client
		GetFrontendClient() frontend.Client
		GetMatchingRawClient() matching.Client
		GetMatchingClient() matching.Client
		GetHistoryRawClient() history.Client
		GetHistoryClient() history.Client
		GetRemoteAdminClient(cluster string) admin.Client
		GetRemoteFrontendClient(cluster string) frontend.Client
		GetClientBean() client.Bean

		// persistence clients

		GetMetadataManager() persistence.MetadataManager
		GetTaskManager() persistence.TaskManager
		GetVisibilityManager() persistence.VisibilityManager
		GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue
		GetShardManager() persistence.ShardManager
		GetHistoryManager() persistence.HistoryManager
		GetExecutionManager(int) (persistence.ExecutionManager, error)
		GetPersistenceBean() persistenceClient.Bean

		// loggers

		GetLogger() log.Logger
		GetThrottledLogger() log.Logger

		// for registering handlers
		GetGRPCListener() net.Listener
	}
)
