package replication

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination eager_namespace_refresher_mock.go

type (
	EagerNamespaceRefresher interface {
		SyncNamespaceFromSourceCluster(ctx context.Context, namespaceId namespace.ID, sourceCluster string) (*namespace.Namespace, error)
	}

	eagerNamespaceRefresherImpl struct {
		metadataManager         persistence.MetadataManager
		namespaceRegistry       namespace.Registry
		logger                  log.Logger
		lock                    sync.Mutex
		clientBean              client.Bean
		replicationTaskExecutor nsreplication.TaskExecutor
		currentCluster          string
		metricsHandler          metrics.Handler
	}
)

func NewEagerNamespaceRefresher(
	metadataManager persistence.MetadataManager,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	clientBean client.Bean,
	replicationTaskExecutor nsreplication.TaskExecutor,
	currentCluster string,
	metricsHandler metrics.Handler) EagerNamespaceRefresher {
	return &eagerNamespaceRefresherImpl{
		metadataManager:         metadataManager,
		namespaceRegistry:       namespaceRegistry,
		logger:                  logger,
		clientBean:              clientBean,
		replicationTaskExecutor: replicationTaskExecutor,
		currentCluster:          currentCluster,
		metricsHandler:          metricsHandler,
	}
}

func (e *eagerNamespaceRefresherImpl) SyncNamespaceFromSourceCluster(
	ctx context.Context,
	namespaceId namespace.ID,
	sourceCluster string,
) (*namespace.Namespace, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	adminClient, err := e.clientBean.GetRemoteAdminClient(sourceCluster)
	if err != nil {
		return nil, err
	}
	resp, err := adminClient.GetNamespace(ctx, &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	})
	if err != nil {
		return nil, err
	}
	if !resp.GetIsGlobalNamespace() {
		return nil, serviceerror.NewFailedPreconditionf("Not a global namespace: %v", namespaceId)
	}
	hasCurrentCluster := false
	for _, c := range resp.GetReplicationConfig().GetClusters() {
		if e.currentCluster == c.GetClusterName() {
			hasCurrentCluster = true
		}
	}
	if !hasCurrentCluster {
		metrics.ReplicationOutlierNamespace.With(e.metricsHandler).Record(1)
		return nil, serviceerror.NewFailedPrecondition("Namespace does not belong to current cluster")
	}
	_, err = e.namespaceRegistry.GetNamespaceByID(namespaceId)
	var operation enumsspb.NamespaceOperation
	switch err.(type) {
	case *serviceerror.NamespaceNotFound:
		operation = enumsspb.NAMESPACE_OPERATION_CREATE
	case nil:
		operation = enumsspb.NAMESPACE_OPERATION_UPDATE
	default:
		return nil, err
	}
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 resp.GetInfo().Id,
		Info:               resp.GetInfo(),
		Config:             resp.GetConfig(),
		ReplicationConfig:  resp.GetReplicationConfig(),
		ConfigVersion:      resp.GetConfigVersion(),
		FailoverVersion:    resp.GetFailoverVersion(),
		FailoverHistory:    resp.GetFailoverHistory(),
	}
	err = e.replicationTaskExecutor.Execute(ctx, task)
	if err != nil {
		return nil, err
	}
	return e.namespaceRegistry.RefreshNamespaceById(namespaceId)
}
