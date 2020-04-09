package resource

import (
	"github.com/uber-go/tally"
	sdkclient "go.temporal.io/temporal/client"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/authorization"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/elasticsearch"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	// BootstrapParams holds the set of parameters
	// needed to bootstrap a service
	BootstrapParams struct {
		Name            string
		InstanceID      string
		Logger          log.Logger
		ThrottledLogger log.Logger

		MetricScope                  tally.Scope
		MembershipFactoryInitializer MembershipFactoryInitializerFunc
		RPCFactory                   common.RPCFactory
		AbstractDatastoreFactory     persistenceClient.AbstractDataStoreFactory
		PersistenceConfig            config.Persistence
		ClusterMetadata              cluster.Metadata
		ReplicatorConfig             config.Replicator
		MetricsClient                metrics.Client
		MessagingClient              messaging.Client
		ESClient                     elasticsearch.Client
		ESConfig                     *elasticsearch.Config
		DynamicConfig                dynamicconfig.Client
		DCRedirectionPolicy          config.DCRedirectionPolicy
		PublicClient                 sdkclient.Client
		ArchivalMetadata             archiver.ArchivalMetadata
		ArchiverProvider             provider.ArchiverProvider
		Authorizer                   authorization.Authorizer
	}

	// MembershipMonitorFactory provides a bootstrapped membership monitor
	MembershipMonitorFactory interface {
		// GetMembershipMonitor return a membership monitor
		GetMembershipMonitor() (membership.Monitor, error)
	}

	// MembershipFactoryInitializerFunc is used for deferred initialization of the MembershipFactory
	// to allow for the PersistenceBean to be constructed further downstream.
	MembershipFactoryInitializerFunc func(persistenceBean persistenceClient.Bean, logger log.Logger) (MembershipMonitorFactory, error)
)
