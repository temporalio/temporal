//go:build temporal_minimal && !temporal_with_ringpop

package temporal

import (
	"errors"
	"fmt"
	"slices"

	"go.uber.org/fx"
)

var errRingpopExcluded = errors.New("ringpop membership is not compiled into this server; supply static service hosts with WithStaticHosts or rebuild with the temporal_with_ringpop build tag")

func defaultMembershipModule() fx.Option {
	return fx.Error(errRingpopExcluded)
}

func (so *serverOptions) validateMembershipConfig() error {
	if len(so.hostsByService) == 0 {
		return errRingpopExcluded
	}
	if err := so.validateStaticHosts(); err != nil {
		return fmt.Errorf("%w; %v", err, errRingpopExcluded)
	}
	for service := range so.serviceNames {
		hosts := so.hostsByService[service]
		if len(hosts.All) == 0 {
			return fmt.Errorf("%w: %v; %v", missingServiceInStaticHosts, service, errRingpopExcluded)
		}
		if hosts.Self == "" || !slices.Contains(hosts.All, hosts.Self) {
			return fmt.Errorf("static hosts for %q service must include a nonempty self-address in the list of all hosts; %v", service, errRingpopExcluded)
		}
	}
	return nil
}
