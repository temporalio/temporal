// Copyright (c) 2019 Uber Technologies, Inc.
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
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/yarpc"

	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/client/frontend"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
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

		GetDomainCache() cache.DomainCache
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

		GetSDKClient() workflowservice.WorkflowServiceClient
		GetFrontendRawClient() frontend.ClientGRPC
		GetFrontendClient() frontend.ClientGRPC
		GetMatchingRawClient() matching.Client
		GetMatchingClient() matching.Client
		GetHistoryRawClient() history.Client
		GetHistoryClient() history.Client
		GetRemoteAdminClient(cluster string) admin.Client
		GetRemoteFrontendClient(cluster string) frontend.ClientGRPC
		GetClientBean() client.Bean

		// persistence clients

		GetMetadataManager() persistence.MetadataManager
		GetTaskManager() persistence.TaskManager
		GetVisibilityManager() persistence.VisibilityManager
		GetDomainReplicationQueue() persistence.DomainReplicationQueue
		GetShardManager() persistence.ShardManager
		GetHistoryManager() persistence.HistoryManager
		GetExecutionManager(int) (persistence.ExecutionManager, error)
		GetPersistenceBean() persistenceClient.Bean

		// loggers

		GetLogger() log.Logger
		GetThrottledLogger() log.Logger

		// for registering handlers
		GetDispatcher() *yarpc.Dispatcher
		GetGRPCDispatcher() *yarpc.Dispatcher
	}
)
