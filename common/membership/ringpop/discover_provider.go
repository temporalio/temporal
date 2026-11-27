package ringpop

import (
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type refreshHook func() ([]string, error)

// SnapshotHosts provides a replaceable snapshot of hosts to bootstrap and perform
// healing from. Hosts refreshes the snapshot on every call. A successful refresh
// replaces the snapshot, including when it returns an empty list.
//
// Note: If a refresh fails, Hosts logs a warning and returns the previous snapshot.
// This preserves the last known seeds when the persistence store is unavailable.
type SnapshotHosts struct {
	mu sync.Mutex

	hosts   []string
	refresh refreshHook

	logger log.Logger
}

// Hosts refreshes the current host snapshot, and returns it. A refresh error does
// not clear the snapshot or prevent the caller from using it.
func (p *SnapshotHosts) Hosts() ([]string, error) {
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

// New creates a provider with an initial host snapshot, a logger, and a refresh
// hook.
func NewSnapshotHosts(logger log.Logger, refresh refreshHook, hosts ...string) *SnapshotHosts {
	return &SnapshotHosts{
		hosts:  hosts,
		logger: logger,

		refresh: refresh,
	}
}
