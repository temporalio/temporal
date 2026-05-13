package callback

import (
	"errors"
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

// Header key used to identify callbacks that originate from and target the same cluster.
// Note: this is the nexusoperations.NexusCallbackSourceHeader stripped of Nexus-Callback-
const callbackSourceHeader = "source"

// routeSystemCallbackRequest routes a system callback request to the appropriate frontend client
// based on the callback token's namespace and active cluster.
func routeSystemCallbackRequest(
	r *http.Request,
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	httpClientCache *cluster.FrontendHTTPClientCache,
	callbackTokenGenerator *commonnexus.CallbackTokenGenerator,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
) (*http.Response, error) {
	var frontendClient *common.FrontendHTTPClient
	if r.Header != nil {
		token, err := commonnexus.DecodeCallbackToken(r.Header.Get(commonnexus.CallbackTokenHeader))
		if err != nil {
			logger.Error("failed to decode callback token", tag.Error(err))
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
		}

		completion, err := callbackTokenGenerator.DecodeCompletion(token)
		if err != nil {
			logger.Error("failed to decode completion from token", tag.Error(err))
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
		}

		// Normalize to support two possible token shapes:
		// - legacy HSM tokens carry namespace/workflow IDs directly
		// - CHASM tokens carry an encoded component ref instead
		namespaceID := completion.GetNamespaceId()
		businessID := completion.GetWorkflowId()
		if namespaceID == "" && len(completion.GetComponentRef()) > 0 {
			ref := &persistencespb.ChasmComponentRef{}
			if err := ref.Unmarshal(completion.GetComponentRef()); err != nil {
				logger.Error("failed to decode CHASM component ref from callback token", tag.Error(err))
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
			}
			if ref.GetNamespaceId() == "" {
				logger.Error("decoded CHASM component ref is missing namespace ID")
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
			}
			if ref.GetBusinessId() == "" {
				logger.Error("decoded CHASM component ref is missing business ID")
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
			}
			namespaceID = ref.GetNamespaceId()
			businessID = ref.GetBusinessId()
		}

		ns, err := namespaceRegistry.GetNamespaceByID(namespace.ID(namespaceID))
		if err != nil {
			logger.Error("failed to get namespace for nexus completion request", tag.WorkflowNamespaceID(namespaceID), tag.Error(err))
			var nfe *serviceerror.NamespaceNotFound
			if errors.As(err, &nfe) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace %q not found", namespaceID)
			}
			return nil, commonnexus.ConvertGRPCError(err, false)
		}
		clusterName := ns.ActiveClusterName(namespace.RoutingKey{ID: businessID})
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
	} else {
		frontendClient = localClient
	}
	r.URL.Path = commonnexus.PathCompletionCallbackNoIdentifier
	r.URL.Scheme = frontendClient.Scheme
	r.URL.Host = frontendClient.Address
	r.Host = frontendClient.Address
	return frontendClient.Do(r)
}

func routeRequest(
	r *http.Request,
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	httpClientCache *cluster.FrontendHTTPClientCache,
	callbackTokenGenerator *commonnexus.CallbackTokenGenerator,
	defaultClient *http.Client,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
) (*http.Response, error) {
	if r.URL.String() == commonnexus.SystemCallbackURL {
		return routeSystemCallbackRequest(r, clusterMetadata, namespaceRegistry, httpClientCache, callbackTokenGenerator, localClient, logger)
	}
	// This source header is populated in nexusoperations/tasks (via the ClientProvider) for worker targets
	// if this header is not populated then we assume it's an external target.
	if r.Header == nil || r.Header.Get(callbackSourceHeader) == "" {
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

	r.URL.Scheme = frontendClient.Scheme
	r.URL.Host = frontendClient.Address
	r.Host = frontendClient.Address
	return frontendClient.Do(r)
}
