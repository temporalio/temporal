package common

import (
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// ClientCache store initialized clients
	ClientCache interface {
		Lookup(key string, index int) (string, error) // pass through to keyResolver
		GetClientForKey(key string, index int) (any, error)
		GetClientForClientKey(clientKey string) (any, error)
		GetAllClients() ([]any, error)
		// Evict removes the cached entry for the given key and runs its
		// release fn.
		Evict(clientKey string)
		// EvictAll removes every cached entry and runs each one's release fn.
		// Used to deterministically release cached gRPC connections on shutdown.
		EvictAll()
	}

	keyResolver interface {
		Lookup(key string, index int) (string, error)
		GetAllAddresses() ([]string, error)
	}

	// The returned release fn (if non-nil) is invoked when the entry is evicted.
	clientProvider func(clientKey string) (any, func() error, error)

	// ClientCacheEntry contains a client and optional lifecycle callbacks for
	// its backing resource.
	ClientCacheEntry struct {
		Client any
		// IsValid must be fast, non-blocking, and must not call back into its
		// ClientCache because it is invoked while a cache lock is held.
		IsValid func() bool
		// Release is invoked outside cache locks after removal or replacement.
		Release func() error
	}

	// ClientCacheProvider creates a client cache entry.
	ClientCacheProvider func(clientKey string) (ClientCacheEntry, error)

	cachedEntry struct {
		ClientCacheEntry
	}

	clientCacheImpl struct {
		keyResolver    keyResolver
		clientProvider ClientCacheProvider

		cacheLock sync.RWMutex
		clients   map[string]cachedEntry

		logger log.Logger
	}
)

// NewClientCache creates a new client cache based on membership
func NewClientCache(
	keyResolver keyResolver,
	clientProvider clientProvider,
	logger log.Logger,
) ClientCache {
	return NewClientCacheWithProvider(
		keyResolver,
		func(clientKey string) (ClientCacheEntry, error) {
			client, release, err := clientProvider(clientKey)
			return ClientCacheEntry{Client: client, Release: release}, err
		},
		logger,
	)
}

// NewClientCacheWithProvider creates a client cache whose entries can report
// when their backing resources are no longer usable and need replacement.
func NewClientCacheWithProvider(
	keyResolver keyResolver,
	clientProvider ClientCacheProvider,
	logger log.Logger,
) ClientCache {
	return &clientCacheImpl{
		keyResolver:    keyResolver,
		clientProvider: clientProvider,

		clients: make(map[string]cachedEntry),
		logger:  logger,
	}
}

func (c *clientCacheImpl) Lookup(key string, index int) (string, error) {
	return c.keyResolver.Lookup(key, index)
}

func (c *clientCacheImpl) GetClientForKey(key string, index int) (any, error) {
	clientKey, err := c.Lookup(key, index)
	if err != nil {
		return nil, err
	}
	return c.GetClientForClientKey(clientKey)
}

func (c *clientCacheImpl) GetClientForClientKey(clientKey string) (any, error) {
	c.cacheLock.RLock()
	entry, ok := c.clients[clientKey]
	valid := ok && entry.isValid()
	c.cacheLock.RUnlock()
	if valid {
		return entry.Client, nil
	}

	c.cacheLock.Lock()
	entry, ok = c.clients[clientKey]
	if ok && entry.isValid() {
		c.cacheLock.Unlock()
		return entry.Client, nil
	}

	newEntry, err := c.clientProvider(clientKey)
	if err != nil {
		c.cacheLock.Unlock()
		return nil, err
	}
	c.clients[clientKey] = cachedEntry{ClientCacheEntry: newEntry}
	c.cacheLock.Unlock()

	if ok {
		c.release(entry)
	}
	return newEntry.Client, nil
}

func (c *clientCacheImpl) GetAllClients() ([]any, error) {
	var result []any
	allAddresses, err := c.keyResolver.GetAllAddresses()
	if err != nil {
		return nil, err
	}
	for _, addr := range allAddresses {
		client, err := c.GetClientForClientKey(addr)
		if err != nil {
			return nil, err
		}
		result = append(result, client)
	}

	return result, nil
}

func (c *clientCacheImpl) Evict(clientKey string) {
	c.cacheLock.Lock()
	entry, ok := c.clients[clientKey]
	if ok {
		delete(c.clients, clientKey)
	}
	c.cacheLock.Unlock()

	if ok {
		c.release(entry)
	}
}

func (c *clientCacheImpl) EvictAll() {
	c.cacheLock.Lock()
	entries := c.clients
	c.clients = make(map[string]cachedEntry)
	c.cacheLock.Unlock()

	for _, entry := range entries {
		c.release(entry)
	}
}

func (e cachedEntry) isValid() bool {
	return e.IsValid == nil || e.IsValid()
}

func (c *clientCacheImpl) release(entry cachedEntry) {
	if entry.Release == nil {
		return
	}
	if err := entry.Release(); err != nil {
		c.logger.Warn("Error releasing evicted client resource", tag.Error(err))
	}
}
