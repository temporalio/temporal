package callbacks

import (
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus"
	"go.temporal.io/server/components/nexusoperations"
)

func routeRequest(
	r *http.Request,
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
	defaultCilent *http.Client,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,

) (*http.Response, error) {
	// this source header is populated in nexusoperations/executors for worker targets
	// if this header is not populated then we assume it's and external target
	if r.Header == nil || r.Header.Get(nexusoperations.NexusCallbackSourceHeader) == "" {
		return defaultCilent.Do(r)
	}
	callbackSource := r.Header.Get(nexusoperations.NexusCallbackSourceHeader)
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if callbackSource == clusterInfo.ClusterID {
			return execRequest(
				r,
				httpClientCache,
				localClient,
				logger,
				clusterMetadata.GetCurrentClusterName(),
				clusterName,
			)
		}
	}
	// this should not happen
	logger.Error(
		"HTTPCallerProvider unable to find the target cluster. Using local HTTP Client.",
		tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
	)
	return localClient.Do(r)
}

func execRequest(
	r *http.Request,
	httpClientCache *cluster.FrontendHTTPClientCache,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
	currentClusterName, targetClusterName string,
) (*http.Response, error) {
	var frontendClient *common.FrontendHTTPClient
	if targetClusterName == currentClusterName {
		frontendClient = localClient
	} else {
		fe, err := httpClientCache.Get(targetClusterName)
		if err != nil {
			logger.Warn(
				"HTTPCallerProvider unable to get FrontendHTTPClient for callback target cluster. Using local HTTP Client.",
				tag.SourceCluster(currentClusterName),
				tag.TargetCluster(targetClusterName),
				tag.Error(err),
			)
			return localClient.Do(r)
		}
		frontendClient = fe
	}
	r.URL.Scheme = frontendClient.Scheme
	r.URL.Host = frontendClient.Address
	// we need this check while there is a config to toggle
	// systemURL usage
	if r.URL.String() == nexus.SystemCallbackURL {
		r.URL.Path = nexus.RouteCompletionCallbackNoIdentifier
	}
	r.Host = frontendClient.Address
	return frontendClient.Do(r)
}
