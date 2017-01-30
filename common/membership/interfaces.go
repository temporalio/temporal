package membership

import "errors"

// ErrUnknownService is thrown for a service that is not tracked by this instance
var ErrUnknownService = errors.New("Service not tracked by Monitor")

// ErrInsufficientHosts is thrown when there are not enough hosts to serve the request
var ErrInsufficientHosts = errors.New("Not enough hosts to serve the request")

// ErrListenerAlreadyExist is thrown on a duplicate AddListener call from the same listener
var ErrListenerAlreadyExist = errors.New("Listener already exist for the service")

type (

	// ChangedEvent describes a change in membership
	ChangedEvent struct {
		HostsAdded   []*HostInfo
		HostsUpdated []*HostInfo
		HostsRemoved []*HostInfo
	}

	// Monitor provides membership information for all cadence services.
	// It can be used to query which member host of a service is responsible for serving a given key.
	Monitor interface {
		Start() error
		Stop()
		WhoAmI() (*HostInfo, error)
		Lookup(service string, key string) (*HostInfo, error)
		GetResolver(service string) (ServiceResolver, error)
		// AddListener adds a listener for this service.
		// The listener will get notified on the given
		// channel, whenever there is a membership change.
		// @service: The service to be listened on
		// @name: The name for identifying the listener
		// @notifyChannel: The channel on which the caller receives notifications
		AddListener(service string, name string, notifyChannel chan<- *ChangedEvent) error
		// RemoveListener removes a listener for this service.
		RemoveListener(service string, name string) error
	}

	// ServiceResolver provides membership information for a specific cadence service.
	// It can be used to resolve which member host is responsible for serving a given key.
	ServiceResolver interface {
		Lookup(key string) (*HostInfo, error)
		// AddListener adds a listener which will get notified on the given
		// channel, whenever membership changes.
		// @name: The name for identifying the listener
		// @notifyChannel: The channel on which the caller receives notifications
		AddListener(name string, notifyChannel chan<- *ChangedEvent) error
		// RemoveListener removes a listener for this service.
		RemoveListener(name string) error
	}
)
