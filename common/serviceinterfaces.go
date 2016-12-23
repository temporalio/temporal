package common

import (
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
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
	}
)
