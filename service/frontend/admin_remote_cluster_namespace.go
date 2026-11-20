package frontend

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc/codes"
)

// GetRemoteClusterNamespace reads a namespace from a connected remote cluster
// through this cluster's remote-cluster client. Operator tools use it to
// compare replicas while talking only to their local frontend, whose remote
// clients already carry the routing and TLS identity for each peer.
//
// Only a verified peer's namespace lookup may return NamespaceNotFound: the
// peer's identity is checked before the namespace is read, so a cluster
// metadata entry that routes to the wrong cluster can never be mistaken for a
// missing replica.
func (adh *AdminHandler) GetRemoteClusterNamespace(
	ctx context.Context,
	request *adminservice.GetRemoteClusterNamespaceRequest,
) (_ *adminservice.GetRemoteClusterNamespaceResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	clusterName := request.GetClusterName()
	if clusterName == "" {
		return nil, serviceerror.NewInvalidArgument("cluster name is required")
	}
	namespaceRequest := &adminservice.GetNamespaceRequest{}
	switch selector := request.GetSelector().(type) {
	case *adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceName:
		if selector.RemoteNamespaceName == "" {
			return nil, serviceerror.NewInvalidArgument("namespace name is empty")
		}
		namespaceRequest.Attributes = &adminservice.GetNamespaceRequest_Namespace{Namespace: selector.RemoteNamespaceName}
	case *adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId:
		if selector.RemoteNamespaceId == "" {
			return nil, serviceerror.NewInvalidArgument("namespace ID is empty")
		}
		namespaceRequest.Attributes = &adminservice.GetNamespaceRequest_Id{Id: selector.RemoteNamespaceId}
	default:
		return nil, serviceerror.NewInvalidArgument("exactly one of namespace name or ID is required")
	}
	if clusterName == adh.clusterMetadata.GetCurrentClusterName() {
		return nil, serviceerror.NewInvalidArgumentf("cluster %q is the current cluster; read it with GetNamespace", clusterName)
	}
	// Eligibility depends on the connection, not on replication streams, so the
	// read still works while replication to the peer is paused.
	clusterInfo, ok := adh.clusterMetadata.GetAllClusterInfo()[clusterName]
	if !ok {
		return nil, serviceerror.NewFailedPreconditionf("unknown remote cluster %q", clusterName)
	}
	if !clusterInfo.Enabled {
		return nil, serviceerror.NewFailedPreconditionf("connection to remote cluster %q is disabled", clusterName)
	}
	if clusterInfo.RPCAddress == "" {
		return nil, serviceerror.NewFailedPreconditionf("remote cluster %q has no frontend address", clusterName)
	}

	remoteClient := adh.clientFactory.NewRemoteAdminClientWithTimeout(
		clusterInfo.RPCAddress,
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)
	describe, err := remoteClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return nil, remoteClusterRelayError(clusterName, err)
	}
	if describe.GetClusterName() != clusterName {
		return nil, serviceerror.NewFailedPreconditionf(
			"remote cluster identity mismatch: expected %q, got %q",
			clusterName,
			describe.GetClusterName(),
		)
	}
	namespaceResponse, err := remoteClient.GetNamespace(ctx, namespaceRequest)
	if err != nil {
		// The peer is verified, so its NamespaceNotFound means the namespace is
		// absent there and passes through unchanged.
		return nil, err
	}
	return &adminservice.GetRemoteClusterNamespaceResponse{
		ClusterName: describe.GetClusterName(),
		Namespace:   namespaceResponse,
	}, nil
}

// remoteClusterRelayError keeps a failure to reach or identify the peer from
// being read as a missing namespace.
func remoteClusterRelayError(clusterName string, err error) error {
	if serviceerror.ToStatus(err).Code() == codes.NotFound {
		return serviceerror.NewUnavailablef("describe remote cluster %q: %v", clusterName, err)
	}
	return err
}
