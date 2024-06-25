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

package callbacks

import (
	"net/http"

	"go.uber.org/fx"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/queues"
)

// Header key used to identify callbacks that originate from and target the same cluster.
const callbackSourceHeader = "source"

var Module = fx.Module(
	"component.callbacks",
	fx.Provide(ConfigProvider),
	fx.Provide(HTTPCallerProviderProvider),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterExecutor),
)

func HTTPCallerProviderProvider(
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
) HTTPCallerProvider {
	m := collection.NewOnceMap(func(queues.NamespaceIDAndDestination) HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			client := &http.Client{}

			if r.Header == nil || r.Header.Get(callbackSourceHeader) == "" {
				return client.Do(r)
			}

			callbackSource := r.Header.Get(callbackSourceHeader)
			for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
				if callbackSource == clusterInfo.ClusterID {
					internalClient, err := httpClientCache.Get(clusterName)
					if err != nil {
						logger.Warn("HTTPCallerProviderProvider unable to get FrontendHTTPClient for callback target cluster. Using default HTTP client.", tag.SourceCluster(clusterMetadata.GetCurrentClusterName()), tag.TargetCluster(clusterName))
						return client.Do(r)
					}
					r.URL.Scheme = internalClient.Scheme()
					r.URL.Host = internalClient.Host()
					r.Host = internalClient.Host()
					return internalClient.Do(r)
				}
			}

			logger.Warn("HTTPCallerProviderProvider unable to get ClusterInformation for callback target cluster. Using default HTTP client.", tag.SourceCluster(clusterMetadata.GetCurrentClusterName()), tag.TargetCluster(callbackSource))
			return client.Do(r)
		}
	})
	return m.Get
}
