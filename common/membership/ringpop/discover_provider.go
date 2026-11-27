package ringpop

import (
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type refreshHook func() ([]string, error)

// refreshingHosts provides a replaceable view of hosts to bootstrap and perform
// healing from. Hosts refreshes the view on every call. A successful refresh
// replaces the view, including when it returns an empty list.
//
// Note: If a refresh fails, Hosts logs a warning and returns the previous view.
// This preserves the last known seeds when the persistence store is unavailable.
type refreshingHosts struct {
	mu sync.Mutex

	hosts   []string
	refresh refreshHook

	logger log.Logger
}

// Hosts refreshes the current host view, and returns it. A refresh error does
// not clear the view or prevent the caller from using it.
func (p *refreshingHosts) Hosts() ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	hosts, err := p.refresh()
	if err != nil {
		p.logger.Warn("unable to refresh bootstrap hostports", tag.Error(err))
	} else {
		p.hosts = hosts
	}

	return p.hosts, nil
}

func newRefreshingHosts(logger log.Logger, refresh refreshHook, hosts ...string) *refreshingHosts {
	return &refreshingHosts{
		hosts:  hosts,
		logger: logger,

		refresh: refresh,
	}
}
