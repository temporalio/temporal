// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package cluster

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/collection"
)

type tlsConfigProvider interface {
	GetRemoteClusterClientConfig(hostname string) (*tls.Config, error)
}

type HttpClient struct {
	http.Client
	Address string
}

type HttpClientCache struct {
	metadata    Metadata
	tlsProvider tlsConfigProvider
	clients     collection.SyncMap[string, *HttpClient]
}

func NewHttpClientCache(
	metadata Metadata,
	tlsProvider tlsConfigProvider,
) *HttpClientCache {
	cache := &HttpClientCache{
		metadata:    metadata,
		tlsProvider: tlsProvider,
		clients:     collection.NewSyncMap[string, *HttpClient](),
	}
	metadata.RegisterMetadataChangeCallback(cache, cache.evictionCallback)
	return cache
}

// Get returns a cached HttpClient if available, or constructs a new one for the given cluster name.
func (c *HttpClientCache) Get(targetClusterName string) (*HttpClient, error) {
	client, ok := c.clients.Get(targetClusterName)
	if ok {
		return client, nil
	}

	client, err := c.newClientForCluster(targetClusterName)
	if err != nil {
		return nil, err
	}

	c.clients.Set(targetClusterName, client)
	return client, nil
}

func (c *HttpClientCache) newClientForCluster(targetClusterName string) (*HttpClient, error) {
	targetInfo, ok := c.metadata.GetAllClusterInfo()[targetClusterName]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find cluster metadata for cluster %s", targetClusterName))
	}

	address, err := url.Parse(targetInfo.HTTPAddress)
	if err != nil {
		return nil, err
	}

	client := http.Client{}

	if c.tlsProvider != nil {
		tlsClientConfig, err := c.tlsProvider.GetRemoteClusterClientConfig(address.Hostname())
		if err != nil {
			return nil, err
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsClientConfig}
	}

	return &HttpClient{
		Address: targetInfo.HTTPAddress,
		Client:  client,
	}, nil
}

// evictionCallback is invoked by cluster.Metadata when cluster information changes.
// It invalidates clients which are either no longer present or have had their HTTP address changed.
// It is assumed that TLS information has not changed for clusters that are unmodified.
func (c *HttpClientCache) evictionCallback(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
	for oldClusterName, oldClusterInfo := range oldClusterMetadata {
		if oldClusterName == c.metadata.GetCurrentClusterName() || oldClusterInfo == nil {
			continue
		}

		newClusterInfo, exists := newClusterMetadata[oldClusterName]
		if !exists || oldClusterInfo.HTTPAddress != newClusterInfo.HTTPAddress {
			// Cluster was removed or had its HTTP address changed, so invalidate the cached client for that cluster.
			client, ok := c.clients.Pop(oldClusterName)
			if ok {
				client.CloseIdleConnections()
			}
		}
	}
}
