package resource

import (
	"net"

	sdkclient "go.temporal.io/temporal/client"

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
