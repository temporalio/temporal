package common

import (
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type (
	// ClientCache store initialized clients
	ClientCache interface {
		Lookup(key string, index int) (string, error) // pass through to keyResolver
		GetClientForKey(key string, index int) (any, error)
		GetClientForClientKey(clientKey string) (any, error)
		GetAllClients() ([]any, error)
		// Evict removes the cached entry for the given key and closes its connection.
		Evict(clientKey string)
		// EvictAll removes every cached entry and closes its connection.
		// Used to deterministically release cached gRPC connections on shutdown.
		EvictAll()
	}

	keyResolver interface {
		Lookup(key string, index int) (string, error)
		GetAllAddresses() ([]string, error)
	}

	clientProvider func(clientKey string) (any, *grpc.ClientConn, error)

	cachedEntry struct {
		client     any
		connection *grpc.ClientConn
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
	valid := ok && entry.isValid()
	c.cacheLock.RUnlock()
	if valid {
		return entry.client, nil
	}

	c.cacheLock.Lock()
	entry, ok = c.clients[clientKey]
	if ok && entry.isValid() {
		c.cacheLock.Unlock()
		return entry.client, nil
	}

	client, connection, err := c.clientProvider(clientKey)
	if err != nil {
		c.cacheLock.Unlock()
		return nil, err
	}
	c.clients[clientKey] = cachedEntry{client: client, connection: connection}
	c.cacheLock.Unlock()

	if ok {
		c.release(entry)
	}
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
	return e.connection == nil || e.connection.GetState() != connectivity.Shutdown
}

func (c *clientCacheImpl) release(entry cachedEntry) {
	if entry.connection == nil {
		return
	}
	if err := entry.connection.Close(); err != nil {
		c.logger.Warn("Error releasing evicted client resource", tag.Error(err))
	}
}
