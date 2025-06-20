package common

import (
	"sync"
)

type (
	// ClientCache store initialized clients
	ClientCache interface {
		GetClientForKey(key string) (interface{}, error)
		GetClientForClientKey(clientKey string) (interface{}, error)
		GetAllClients() ([]interface{}, error)
	}

	keyResolver interface {
		Lookup(key string) (string, error)
		GetAllAddresses() ([]string, error)
	}

	clientProvider func(string) (interface{}, error)

	clientCacheImpl struct {
		keyResolver    keyResolver
		clientProvider clientProvider

		cacheLock sync.RWMutex
		clients   map[string]interface{}
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

		clients: make(map[string]interface{}),
	}
}

func (c *clientCacheImpl) GetClientForKey(key string) (interface{}, error) {
	clientKey, err := c.keyResolver.Lookup(key)
	if err != nil {
		return nil, err
	}

	return c.GetClientForClientKey(clientKey)
}

func (c *clientCacheImpl) GetClientForClientKey(clientKey string) (interface{}, error) {
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

func (c *clientCacheImpl) GetAllClients() ([]interface{}, error) {
	var result []interface{}
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
