package host

import (
	"github.com/dgryski/go-farm"

	"github.com/temporalio/temporal/common/membership"
)

type simpleResolver struct {
	hosts    []*membership.HostInfo
	hashfunc func([]byte) uint32
}

// newSimpleResolver returns a service resolver that maintains static mapping
// between services and host info
func newSimpleResolver(service string, hosts []string) membership.ServiceResolver {
	hostInfos := make([]*membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		hostInfos = append(hostInfos, membership.NewHostInfo(host, map[string]string{membership.RoleKey: service}))
	}
	return &simpleResolver{hostInfos, farm.Fingerprint32}
}

func (s *simpleResolver) Lookup(key string) (*membership.HostInfo, error) {
	hash := int(s.hashfunc([]byte(key)))
	idx := hash % len(s.hosts)
	return s.hosts[idx], nil
}

func (s *simpleResolver) AddListener(name string, notifyChannel chan<- *membership.ChangedEvent) error {
	return nil
}

func (s *simpleResolver) RemoveListener(name string) error {
	return nil
}

func (s *simpleResolver) MemberCount() int {
	return len(s.hosts)
}

func (s *simpleResolver) Members() []*membership.HostInfo {
	return s.hosts
}
