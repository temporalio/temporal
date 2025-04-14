// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
