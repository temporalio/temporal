package callbacks

import (
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus"
)

func routeRequest(r *http.Request,
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
) (*http.Response, error) {
	if r.Header == nil || r.Header.Get(callbackSourceHeader) == "" {
		return localClient.Do(r)
	}
	callbackSource := r.Header.Get(callbackSourceHeader)
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if callbackSource == clusterInfo.ClusterID {
			var frontendClient *common.FrontendHTTPClient
			if clusterMetadata.GetCurrentClusterName() == clusterName {
				frontendClient = localClient
			} else {
				fe, err := httpClientCache.Get(clusterName)
				if err != nil {
					logger.Warn(
						"HTTPCallerProvider unable to get FrontendHTTPClient for callback target cluster. Using local HTTP Client.",
						tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
						tag.TargetCluster(clusterName),
						tag.Error(err),
					)
					break
				}
				frontendClient = fe
			}
			r.URL.Scheme = frontendClient.Scheme
			r.URL.Host = frontendClient.Address
			if r.URL.String() == nexus.SystemCallbackURL {
				r.URL.Path = nexus.RouteCompletionCallbackNoIdentifier
			}
			r.Host = frontendClient.Address

			return frontendClient.Do(r)
		}
	}
	return localClient.Do(r)
}
