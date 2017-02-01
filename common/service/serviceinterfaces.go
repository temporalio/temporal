package service

import (
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"

	"code.uber.internal/devexp/minions/client"
	"code.uber.internal/devexp/minions/common/membership"
)

type (
	// Service is the interface which must be implemented by all the services
	Service interface {
		// GetHostName returns the name of host running the service
		GetHostName() string

		// Start starts the service
		Start(thriftService []thrift.TChanServer)

		// Stop stops the service
		Stop()

		GetLogger() bark.Logger

		GetMetricsScope() tally.Scope

		GetClientFactory() client.Factory

		GetMembershipMonitor() membership.Monitor

		GetHostInfo() *membership.HostInfo
	}
)
