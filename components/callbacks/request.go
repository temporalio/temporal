package callbacks

import (
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus"
)

// Header key used to identify callbacks that originate from and target the same cluster.
// Note: this is the nexusoperations.NexusCallbackSourceHeader stripped of Nexus-Callback-
const callbackSourceHeader = "source"

func routeRequest(
	r *http.Request,
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
	defaultCilent *http.Client,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
) (*http.Response, error) {
	// this source header is populated in nexusoperations/executors (via the ClientProvider) for worker targets
	// if this header is not populated then we assume it's and external target
	if r.Header == nil || r.Header.Get(callbackSourceHeader) == "" {
		return defaultCilent.Do(r)
	}
	callbackSource := r.Header.Get(callbackSourceHeader)
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
	// this cannot happen when a cluster is removed from the group
	logger.Warn(
		"HTTPCallerProvider unable to find the target cluster. Using local HTTP Client.",
		tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
	)
	// we need this check while there is a config to toggle
	// systemURL usage
	if r.URL.String() == nexus.SystemCallbackURL {
		r.URL.Path = nexus.PathCompletionCallbackNoIdentifier
	}
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
	// we need this check while there is a config to toggle
	// systemURL usage
	if r.URL.String() == nexus.SystemCallbackURL {
		r.URL.Path = nexus.PathCompletionCallbackNoIdentifier
	}
	r.URL.Scheme = frontendClient.Scheme
	r.URL.Host = frontendClient.Address
	r.Host = frontendClient.Address
	return frontendClient.Do(r)
}
