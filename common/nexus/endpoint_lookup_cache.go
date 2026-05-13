package nexus

import (
	"sync"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// EndpointLookupCache is a concurrent-safe in-memory view of Nexus endpoints keyed by ID and name.
//
// Writers fall into two roles:
//   - Owner: the sole authority on the endpoint set. It mints new versions and writes them
//     via ApplyChange (incremental update) or ReplaceAll (full snapshot).
//   - Follower: a passive replica. It does not mint versions; it only writes snapshots it has
//     previously fetched from the owner, via ReplaceAll.
//
// Production mode (one cache instance per process): the owner and any followers run in separate
// processes, each with their own cache. Every cache has exactly one writer: its local owner or
// follower.
//
// Shared mode (one cache instance shared by owner and follower in the same process): both write
// to the same instance. Correctness relies on the monotonic version: only the owner
// mints new versions, so a follower's snapshot is always at or behind the owner's current
// version. The version check on every write drops a follower's stale snapshot and accepts a
// same-version snapshot as idempotent.
type EndpointLookupCache struct {
	lock                 sync.RWMutex
	version              int64
	nexusEndpointsByID   map[string]*persistencespb.NexusEndpointEntry
	nexusEndpointsByName map[string]*persistencespb.NexusEndpointEntry
}

// NewEndpointLookupCache returns an empty cache at version 0.
func NewEndpointLookupCache() *EndpointLookupCache {
	return &EndpointLookupCache{
		nexusEndpointsByID:   make(map[string]*persistencespb.NexusEndpointEntry),
		nexusEndpointsByName: make(map[string]*persistencespb.NexusEndpointEntry),
	}
}

// GetByID looks up an endpoint by ID at the cache's current version.
func (c *EndpointLookupCache) GetByID(id string) (entry *persistencespb.NexusEndpointEntry, version int64, found bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	entry, found = c.nexusEndpointsByID[id]
	return entry, c.version, found
}

// GetByName looks up an endpoint by name at the cache's current version.
func (c *EndpointLookupCache) GetByName(name string) (entry *persistencespb.NexusEndpointEntry, version int64, found bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	entry, found = c.nexusEndpointsByName[name]
	return entry, c.version, found
}

// Version returns the cache's current version.
func (c *EndpointLookupCache) Version() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.version
}

// ReplaceAll replaces the entire cache contents with the given entries at the given version.
// The write is dropped if the given version is older than the cache's current version.
func (c *EndpointLookupCache) ReplaceAll(
	version int64,
	entries []*persistencespb.NexusEndpointEntry,
) {
	nexusEndpointsByID := make(map[string]*persistencespb.NexusEndpointEntry, len(entries))
	nexusEndpointsByName := make(map[string]*persistencespb.NexusEndpointEntry, len(entries))
	for _, entry := range entries {
		nexusEndpointsByID[entry.Id] = entry
		nexusEndpointsByName[entry.Endpoint.Spec.Name] = entry
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// When the cache is shared, this drops stale observer writes so they cannot overwrite a newer
	// state already written by the owner.
	if version < c.version {
		return
	}

	c.version = version
	c.nexusEndpointsByID = nexusEndpointsByID
	c.nexusEndpointsByName = nexusEndpointsByName
}

// ApplyChange applies a single endpoint change. Pass previous=nil for a create, current=nil for
// a delete, or both non-nil for an update. The write is dropped if the given version is older
// than the cache's current version.
func (c *EndpointLookupCache) ApplyChange(
	version int64,
	previous *persistencespb.NexusEndpointEntry,
	current *persistencespb.NexusEndpointEntry,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// When the cache is shared, this drops stale observer writes so they cannot overwrite a newer
	// state already written by the owner.
	if version < c.version {
		return
	}

	c.version = version
	if previous != nil {
		delete(c.nexusEndpointsByID, previous.Id)
		delete(c.nexusEndpointsByName, previous.Endpoint.Spec.Name)
	}
	if current != nil {
		c.nexusEndpointsByID[current.Id] = current
		c.nexusEndpointsByName[current.Endpoint.Spec.Name] = current
	}
}

// Clear empties the cache and resets the version to 0. Must only be invoked by the owner.
func (c *EndpointLookupCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.version = 0
	c.nexusEndpointsByID = make(map[string]*persistencespb.NexusEndpointEntry)
	c.nexusEndpointsByName = make(map[string]*persistencespb.NexusEndpointEntry)
}
