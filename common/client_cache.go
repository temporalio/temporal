package common

import (
	"sync"
)

type (
	// ClientCache store initialized clients
	ClientCache interface {
		Lookup(key string, index int) (string, error) // pass through to keyResolver
		GetClientForKey(key string, index int) (any, error)
		GetClientForClientKey(clientKey string) (any, error)
		GetAllClients() ([]any, error)
	}

	keyResolver interface {
		Lookup(key string, index int) (string, error)
		GetAllAddresses() ([]string, error)
	}

	clientProvider func(string) (any, error)

	clientCacheImpl struct {
		keyResolver    keyResolver
		clientProvider clientProvider

		cacheLock sync.RWMutex
		clients   map[string]any
	}
)

// NewClientCache creates a new client cache based on membership
func NewClientCache(
	keyResolver keyResolver,
	clientProvider clientProvider,
) ClientCache {

	return &clientCacheImpl{
		keyResolver:    keyResolver,
		clientProvider: clientProvider,

		clients: make(map[string]any),
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
	client, ok := c.clients[clientKey]
	c.cacheLock.RUnlock()
	if ok {
		return client, nil
	}

	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	client, ok = c.clients[clientKey]
	if ok {
		return client, nil
	}

	client, err := c.clientProvider(clientKey)
	if err != nil {
		return nil, err
	}
	c.clients[clientKey] = client
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
