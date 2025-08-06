package cluster

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
)

type tlsConfigProvider interface {
	GetRemoteClusterClientConfig(hostname string) (*tls.Config, error)
}

type FrontendHTTPClientCache struct {
	metadata    Metadata
	tlsProvider tlsConfigProvider
	clients     *collection.FallibleOnceMap[string, *common.FrontendHTTPClient]
}

func NewFrontendHTTPClientCache(
	metadata Metadata,
	tlsProvider tlsConfigProvider,
) *FrontendHTTPClientCache {
	cache := &FrontendHTTPClientCache{
		metadata:    metadata,
		tlsProvider: tlsProvider,
	}
	cache.clients = collection.NewFallibleOnceMap(cache.newClientForCluster)
	metadata.RegisterMetadataChangeCallback(cache, cache.evictionCallback)
	return cache
}

// Get returns a cached HttpClient if available, or constructs a new one for the given cluster name.
func (c *FrontendHTTPClientCache) Get(targetClusterName string) (*common.FrontendHTTPClient, error) {
	return c.clients.Get(targetClusterName)
}

func (c *FrontendHTTPClientCache) newClientForCluster(targetClusterName string) (*common.FrontendHTTPClient, error) {
	targetInfo, ok := c.metadata.GetAllClusterInfo()[targetClusterName]
	if !ok {
		return nil, serviceerror.NewNotFoundf("could not find cluster metadata for cluster %s", targetClusterName)
	}

	if targetInfo.HTTPAddress == "" {
		return nil, serviceerror.NewInternalf("HTTPAddress not configured for cluster: %s", targetClusterName)
	}
	host, _, err := net.SplitHostPort(targetInfo.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", serviceerror.NewInternal("invalid frontend address"), err)
	}

	// dialer and transport field values copied from http.DefaultTransport.
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	urlScheme := "http"
	if c.tlsProvider != nil {
		tlsClientConfig, err := c.tlsProvider.GetRemoteClusterClientConfig(host)
		if err != nil {
			return nil, err
		}
		if tlsClientConfig != nil {
			transport.TLSClientConfig = tlsClientConfig
			urlScheme = "https"
		}
	}

	return &common.FrontendHTTPClient{
		Address: targetInfo.HTTPAddress,
		Scheme:  urlScheme,
		Client:  http.Client{Transport: transport},
	}, nil
}

// evictionCallback is invoked by cluster.Metadata when cluster information changes.
// It invalidates clients which are either no longer present or have had their HTTP address changed.
// It is assumed that TLS information has not changed for clusters that are unmodified.
func (c *FrontendHTTPClientCache) evictionCallback(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
	for oldClusterName, oldClusterInfo := range oldClusterMetadata {
		if oldClusterName == c.metadata.GetCurrentClusterName() || oldClusterInfo == nil {
			continue
		}

		newClusterInfo, exists := newClusterMetadata[oldClusterName]
		if !exists || newClusterInfo == nil || oldClusterInfo.HTTPAddress != newClusterInfo.HTTPAddress {
			// Cluster was removed or had its HTTP address changed, so invalidate the cached client for that cluster.
			client, ok := c.clients.Pop(oldClusterName)
			if ok {
				client.CloseIdleConnections()
			}
		}
	}
}
