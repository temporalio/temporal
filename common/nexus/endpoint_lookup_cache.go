package nexus

import (
	"sync"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// EndpointLookupCache is a concurrent-safe in-memory view of Nexus endpoints keyed by ID and name.
//
// Writers play one of two roles:
//   - Owner: mints new versions via ApplyChangeLocked / ReplaceAllLocked. Holds the embedded
//     RWMutex across multi-step operations (e.g. read-version, persist, apply).
//   - Follower: publishes snapshots it has previously fetched from the owner, via ReplaceAll.
//
// When owner and follower share one instance, the monotonic version is the safety invariant: a
// follower's snapshot is always at or behind the owner's current version, so ReplaceAll drops
// stale follower writes.
//
// Use the *Locked variants when the lock is already held; use the unsuffixed variants otherwise.
type EndpointLookupCache struct {
	sync.RWMutex
	// version is the monotonic version. The owner bumps it on every change; followers
	// only observe and forward it via ReplaceAll.
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
	c.RLock()
	defer c.RUnlock()
	return c.GetByIDLocked(id)
}

// GetByIDLocked is the same as GetByID but assumes the caller already holds the lock.
func (c *EndpointLookupCache) GetByIDLocked(id string) (entry *persistencespb.NexusEndpointEntry, version int64, found bool) {
	entry, found = c.nexusEndpointsByID[id]
	return entry, c.version, found
}

// GetByName looks up an endpoint by name at the cache's current version.
func (c *EndpointLookupCache) GetByName(name string) (entry *persistencespb.NexusEndpointEntry, version int64, found bool) {
	c.RLock()
	defer c.RUnlock()
	return c.GetByNameLocked(name)
}

// GetByNameLocked is the same as GetByName but assumes the caller already holds the lock.
func (c *EndpointLookupCache) GetByNameLocked(name string) (entry *persistencespb.NexusEndpointEntry, version int64, found bool) {
	entry, found = c.nexusEndpointsByName[name]
	return entry, c.version, found
}

// Version returns the cache's current version.
func (c *EndpointLookupCache) Version() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.version
}

// VersionLocked is the same as Version but assumes the caller already holds the lock.
func (c *EndpointLookupCache) VersionLocked() int64 {
	return c.version
}

// ReplaceAll replaces the entire cache contents with the given entries at the given version.
// The write is dropped if the given version is older than the cache's current version.
func (c *EndpointLookupCache) ReplaceAll(version int64, entries []*persistencespb.NexusEndpointEntry) {
	c.Lock()
	defer c.Unlock()
	c.ReplaceAllLocked(version, entries)
}

// ReplaceAllLocked is the same as ReplaceAll but assumes the caller already holds the write lock.
func (c *EndpointLookupCache) ReplaceAllLocked(version int64, entries []*persistencespb.NexusEndpointEntry) {
	// When the cache is shared, this drops stale observer writes so they cannot overwrite a newer
	// state already written by the owner.
	if version < c.version {
		return
	}
	nexusEndpointsByID := make(map[string]*persistencespb.NexusEndpointEntry, len(entries))
	nexusEndpointsByName := make(map[string]*persistencespb.NexusEndpointEntry, len(entries))
	for _, entry := range entries {
		nexusEndpointsByID[entry.Id] = entry
		nexusEndpointsByName[entry.Endpoint.Spec.Name] = entry
	}
	c.version = version
	c.nexusEndpointsByID = nexusEndpointsByID
	c.nexusEndpointsByName = nexusEndpointsByName
}

// ApplyChangeLocked applies a single endpoint change and bumps the version by 1. Pass
// previous=nil for a create, current=nil for a delete, or both non-nil for an update. The
// caller must hold the write lock; intended for owner use.
func (c *EndpointLookupCache) ApplyChangeLocked(
	previous *persistencespb.NexusEndpointEntry,
	current *persistencespb.NexusEndpointEntry,
) {
	c.version++
	if previous != nil {
		delete(c.nexusEndpointsByID, previous.Id)
		delete(c.nexusEndpointsByName, previous.Endpoint.Spec.Name)
	}
	if current != nil {
		c.nexusEndpointsByID[current.Id] = current
		c.nexusEndpointsByName[current.Endpoint.Spec.Name] = current
	}
}

// ClearLocked empties the cache and resets the version to 0. The caller must hold the write lock;
// intended for owner use.
func (c *EndpointLookupCache) ClearLocked() {
	c.version = 0
	c.nexusEndpointsByID = make(map[string]*persistencespb.NexusEndpointEntry)
	c.nexusEndpointsByName = make(map[string]*persistencespb.NexusEndpointEntry)
}
