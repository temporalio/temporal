package static

import (
	"context"
	"time"

	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

type (
	staticMonitor struct {
		hosts     map[primitives.ServiceName]Hosts
		resolvers map[primitives.ServiceName]*staticResolver
	}

	Hosts struct {
		// Addresses of all hosts per service.
		All []string
		// Address of this host. May be empty if this host is not running this service.
		Self string
	}
)

func SingleLocalHost(host string) Hosts {
	return Hosts{All: []string{host}, Self: host}
}

func newStaticMonitor(hosts map[primitives.ServiceName]Hosts) membership.Monitor {
	resolvers := make(map[primitives.ServiceName]*staticResolver, len(hosts))
	for service, hostList := range hosts {
		resolvers[service] = newStaticResolver(hostList.All)
	}

	return &staticMonitor{
		hosts:     hosts,
		resolvers: resolvers,
	}
}

func (s *staticMonitor) Start() {
	for service, r := range s.resolvers {
		r.start(s.hosts[service].All)
	}
}

func (s *staticMonitor) EvictSelf() error {
	return nil
}

func (s *staticMonitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	return 0, nil
}

func (s *staticMonitor) GetResolver(service primitives.ServiceName) (membership.ServiceResolver, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, membership.ErrUnknownService
	}
	return resolver, nil
}

func (s *staticMonitor) GetReachableMembers() ([]string, error) {
	return nil, nil
}

func (s *staticMonitor) WaitUntilInitialized(_ context.Context) error {
	return nil
}

func (s *staticMonitor) SetDraining(draining bool) error {
	return nil
}

func (s *staticMonitor) ApproximateMaxPropagationTime() time.Duration {
	return 0
}
