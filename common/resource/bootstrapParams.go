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
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/sdk"
)

type (
	// BootstrapParams holds the set of parameters
	// needed to bootstrap a service
	BootstrapParams struct {
		Name            string
		InstanceID      string
		ThrottledLogger log.Logger
		NamespaceLogger log.Logger

		MembershipFactoryInitializer MembershipFactoryInitializerFunc
		RPCFactory                   common.RPCFactory
		ClientFactoryProvider        client.FactoryProvider
		PersistenceConfig            config.Persistence
		ClusterMetadataConfig        *cluster.Config
		ReplicatorConfig             config.Replicator
		ServerMetricsReporter        metrics.Reporter
		SDKMetricsReporter           metrics.Reporter
		MetricsClient                metrics.Client
		DCRedirectionPolicy          config.DCRedirectionPolicy
		SdkClientFactory             sdk.ClientFactory
		ArchivalMetadata             archiver.ArchivalMetadata
		ArchiverProvider             provider.ArchiverProvider
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
