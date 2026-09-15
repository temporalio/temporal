package callback

import (
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus"
)

// Legacy header key used to identify callbacks that originate from and target the same cluster.
const callbackSourceHeader = "source"

func routeRequest(
	r *http.Request,
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
	defaultClient *http.Client,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
	inspectSourceHeader bool,
) (*http.Response, error) {
	isSystemCallback := r.URL.String() == nexus.SystemCallbackURL
	// Older servers populated this header for worker targets. Treat the request as external unless legacy inspection is enabled.
	if !isSystemCallback && (!inspectSourceHeader || r.Header == nil || r.Header.Get(callbackSourceHeader) == "") {
		return defaultClient.Do(r)
	}
	// If we got here, we assume that the endpoint in the original call was a worker target, and we should route
	// internally, either to a local frontend, or one of the other connected clusters' frontends.
	var frontendClient *common.FrontendHTTPClient
	callbackSource := r.Header.Get(callbackSourceHeader)
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if callbackSource == clusterInfo.ClusterID {
			if clusterMetadata.GetCurrentClusterName() == clusterName {
				frontendClient = localClient
			} else {
				fec, err := httpClientCache.Get(clusterName)
				if err != nil {
					logger.Warn(
						"HTTPCallerProvider unable to get FrontendHTTPClient for callback target cluster. Using local HTTP Client.",
						tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
						tag.TargetCluster(clusterName),
						tag.Error(err),
					)
					frontendClient = localClient
				} else {
					frontendClient = fec
				}
			}
			break
		}
	}
	if frontendClient == nil {
		// This can happen when a cluster is disconnected.
		logger.Warn(
			"HTTPCallerProvider unable to find the target cluster. Using local HTTP Client.",
			tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
		)
		frontendClient = localClient
	}

	if isSystemCallback {
		r.URL.Path = nexus.PathCompletionCallbackNoIdentifier
	}
	r.URL.Scheme = frontendClient.Scheme
	r.URL.Host = frontendClient.Address
	r.Host = frontendClient.Address
	return frontendClient.Do(r)
}
