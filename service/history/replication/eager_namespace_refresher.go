package replication

import (
	"context"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"sync"
)

type (
	EagerNamespaceRefresher interface {
		UpdateNamespaceFailoverVersion(namespaceId namespace.ID, targetFailoverVersion int64) error
	}

	eagerNamespaceRefresherImpl struct {
		metadataManager   persistence.MetadataManager
		namespaceRegistry namespace.Registry
		logger            log.Logger
		lock              sync.Mutex
	}
)

func (e *eagerNamespaceRefresherImpl) UpdateNamespaceFailoverVersion(namespaceId namespace.ID, targetFailoverVersion int64) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceId)
	switch err.(type) {
	case nil:
	case *serviceerror.NamespaceNotFound:
		return nil
	default:
		//do nothing as this is the best effort to update the namespace
		e.logger.Debug("Failed to get namespace from registry", tag.Error(err))
		return err
	}

	if ns.FailoverVersion() >= targetFailoverVersion {
		return nil
	}

	ctx := headers.SetCallerInfo(context.TODO(), headers.SystemPreemptableCallerInfo)
	resp, err := e.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		ID: namespaceId.String(),
	})
	if err != nil {
		e.logger.Debug("Failed to get namespace from persistent", tag.Error(err))
		return nil
	}

	currentFailoverVersion := resp.Namespace.FailoverVersion
	if currentFailoverVersion >= targetFailoverVersion {
		//DB may have a fresher version of namespace, so compare again
		return nil
	}

	metadata, err := e.metadataManager.GetMetadata(ctx)
	if err != nil {
		return err
	}

	request := &persistence.UpdateNamespaceRequest{
		Namespace:           resp.Namespace,
		NotificationVersion: metadata.NotificationVersion,
		IsGlobalNamespace:   resp.IsGlobalNamespace,
	}

	request.Namespace.FailoverVersion = targetFailoverVersion
	request.Namespace.FailoverNotificationVersion = metadata.NotificationVersion

	//Question: is it ok to only update failover version WITHOUT updating FailoverHistory?
	//request.Namespace.ReplicationConfig.FailoverHistory = ??

	if err := e.metadataManager.UpdateNamespace(ctx, request); err != nil {
		e.logger.Info("Failed to update namespace", tag.Error(err))
		return err
	}
	e.namespaceRegistry.RefreshNamespace(ns.ID())
	return nil
}
