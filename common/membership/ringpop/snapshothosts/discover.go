package snapshothosts

import (
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type refreshHook func() ([]string, error)

// SnapshotHosts provides a replaceable snapshot of hosts to bootstrap and perform
// healing from. Hosts refreshes the snapshot after the expiry interval. A successful
// refresh replaces the snapshot, including when it returns an empty list.
//
// Note: If a refresh fails, Hosts logs a warning and returns the previous snapshot.
// This preserves the last known seeds when the persistence store is unavailable.
type SnapshotHosts struct {
	mu sync.Mutex

	hosts          []string
	refresh        refreshHook
	expiresAt      time.Time
	expiryInterval time.Duration

	logger log.Logger
}

// Hosts returns the current host snapshot, refreshing it if expired. A refresh
// error does not clear the snapshot or prevent the caller from using it.
func (p *SnapshotHosts) Hosts() ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if time.Now().After(p.expiresAt) {
		hosts, err := p.refresh()

		if err != nil {
			p.logger.Warn("unable to refresh bootstrap hostports", tag.Error(err))
		} else {
			p.hosts = hosts
			p.expiresAt = time.Now().Add(p.expiryInterval)
		}
	}

	return p.hosts, nil
}

// New creates a provider with an initial host snapshot, a logger, a refresh hook,
// and the interval after which Hosts attempts to refresh the snapshot.
func New(logger log.Logger, refresh refreshHook, expiryInterval time.Duration, hosts ...string) *SnapshotHosts {
	return &SnapshotHosts{
		hosts:  hosts,
		logger: logger,

		refresh:        refresh,
		expiresAt:      time.Now().Add(expiryInterval),
		expiryInterval: expiryInterval,
	}
}
