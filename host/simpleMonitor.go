package host

import (
	"fmt"

	"github.com/temporalio/temporal/common/membership"
)

type simpleMonitor struct {
	hostInfo  *membership.HostInfo
	resolvers map[string]membership.ServiceResolver
}

// NewSimpleMonitor returns a simple monitor interface
func newSimpleMonitor(serviceName string, hosts map[string][]string) membership.Monitor {
	resolvers := make(map[string]membership.ServiceResolver, len(hosts))
	for service, hostList := range hosts {
		resolvers[service] = newSimpleResolver(service, hostList)
	}

	hostInfo := membership.NewHostInfo(hosts[serviceName][0], map[string]string{membership.RoleKey: serviceName})
	return &simpleMonitor{hostInfo, resolvers}
}

func (s *simpleMonitor) Start() {
}

func (s *simpleMonitor) Stop() {
}

func (s *simpleMonitor) WhoAmI() (*membership.HostInfo, error) {
	return s.hostInfo, nil
}

func (s *simpleMonitor) GetResolver(service string) (membership.ServiceResolver, error) {
	return s.resolvers[service], nil
}

func (s *simpleMonitor) Lookup(service string, key string) (*membership.HostInfo, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, fmt.Errorf("cannot lookup host for service %v", service)
	}
	return resolver.Lookup(key)
}

func (s *simpleMonitor) AddListener(service string, name string, notifyChannel chan<- *membership.ChangedEvent) error {
	return nil
}

func (s *simpleMonitor) RemoveListener(service string, name string) error {
	return nil
}

func (s *simpleMonitor) GetReachableMembers() ([]string, error) {
	return nil, nil
}
