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
