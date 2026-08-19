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

	cachedEntry struct {
		client  any
		release func() error
	}

	clientCacheImpl struct {
		keyResolver    keyResolver
		clientProvider clientProvider

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
	c.cacheLock.RUnlock()
	if ok {
		return entry.client, nil
	}

	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	entry, ok = c.clients[clientKey]
	if ok {
		return entry.client, nil
	}

	client, release, err := c.clientProvider(clientKey)
	if err != nil {
		return nil, err
	}
	c.clients[clientKey] = cachedEntry{client: client, release: release}
	return client, nil
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

	if ok && entry.release != nil {
		if err := entry.release(); err != nil {
			c.logger.Warn("Error releasing evicted client resource", tag.Error(err))
		}
	}
}

func (c *clientCacheImpl) EvictAll() {
	c.cacheLock.Lock()
	entries := c.clients
	c.clients = make(map[string]cachedEntry)
	c.cacheLock.Unlock()

	for _, entry := range entries {
		if entry.release != nil {
			if err := entry.release(); err != nil {
				c.logger.Warn("Error releasing evicted client resource", tag.Error(err))
			}
		}
	}
}
