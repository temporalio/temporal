package replication

import (
	"context"
	"slices"
	"sync"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
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
		resolverFactory         namespace.ReplicationResolverFactory
		currentCluster          string
		metricsHandler          metrics.Handler
		eventLogger             otellog.Logger
	}
)

func NewEagerNamespaceRefresher(
	metadataManager persistence.MetadataManager,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	clientBean client.Bean,
	replicationTaskExecutor nsreplication.TaskExecutor,
	resolverFactory namespace.ReplicationResolverFactory,
	currentCluster string,
	metricsHandler metrics.Handler,
	eventLogger otellog.Logger,
) EagerNamespaceRefresher {
	return &eagerNamespaceRefresherImpl{
		metadataManager:         metadataManager,
		namespaceRegistry:       namespaceRegistry,
		logger:                  logger,
		clientBean:              clientBean,
		replicationTaskExecutor: replicationTaskExecutor,
		resolverFactory:         resolverFactory,
		currentCluster:          currentCluster,
		metricsHandler:          metricsHandler,
		eventLogger:             eventLogger,
	}
}

func (e *eagerNamespaceRefresherImpl) SyncNamespaceFromSourceCluster(
	ctx context.Context,
	namespaceId namespace.ID,
	sourceCluster string,
) (_ *namespace.Namespace, retErr error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	startTime := time.Now()
	event := wideevents.NamespaceEagerRefreshInput{
		NamespaceID:    namespaceId.String(),
		SourceCluster:  sourceCluster,
		CurrentCluster: e.currentCluster,
		Outcome:        wideevents.NamespaceEagerRefreshFailed,
	}
	defer func() {
		if retErr == nil {
			event.Outcome = wideevents.NamespaceEagerRefreshRefreshed
		}
		event.Err = retErr
		event.Duration = time.Since(startTime)
		wideevents.EmitNamespaceEagerRefresh(e.eventLogger, event)
	}()

	adminClient, err := e.clientBean.GetRemoteAdminClient(sourceCluster)
	if err != nil {
		event.Outcome = wideevents.NamespaceEagerRefreshFetchFailed
		return nil, err
	}
	resp, err := adminClient.GetNamespace(ctx, &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	})
	if err != nil {
		event.Outcome = wideevents.NamespaceEagerRefreshFetchFailed
		return nil, err
	}
	event.Namespace = resp.GetInfo().GetName()
	event.SourceFailoverVersion = resp.GetFailoverVersion()
	event.SourceConfigVersion = resp.GetConfigVersion()
	if !resp.GetIsGlobalNamespace() {
		event.Outcome = wideevents.NamespaceEagerRefreshNotGlobal
		return nil, serviceerror.NewFailedPreconditionf("Not a global namespace: %v", namespaceId)
	}
	// Membership comes from the namespace's replication resolver, the same way the registry
	// resolves the cluster list of a loaded namespace.
	resolver := e.resolverFactory(&persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   resp.GetInfo().GetId(),
			Name: resp.GetInfo().GetName(),
			Data: resp.GetInfo().GetData(),
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: resp.GetReplicationConfig().GetActiveClusterName(),
			Clusters:          nsreplication.ConvertClusterReplicationConfigFromProto(resp.GetReplicationConfig().GetClusters()),
		},
		FailoverVersion: resp.GetFailoverVersion(),
	})
	if !slices.Contains(resolver.ClusterNames(""), e.currentCluster) {
		metrics.ReplicationOutlierNamespace.With(e.metricsHandler).Record(1)
		event.Outcome = wideevents.NamespaceEagerRefreshNotMember
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
	event.Operation = operation.String()
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 resp.GetInfo().Id,
		Info:               resp.GetInfo(),
		Config:             resp.GetConfig(),
		ReplicationConfig:  resp.GetReplicationConfig(),
		ConfigVersion:      resp.GetConfigVersion(),
		FailoverVersion:    resp.GetFailoverVersion(),
		FailoverHistory:    resp.GetFailoverHistory(),
		RestoreInfo:        resp.GetRestoreInfo(),
	}
	if err := e.replicationTaskExecutor.Execute(ctx, task); err != nil {
		event.Outcome = wideevents.NamespaceEagerRefreshApplyFailed
		return nil, err
	}
	return e.namespaceRegistry.RefreshNamespaceById(namespaceId)
}
