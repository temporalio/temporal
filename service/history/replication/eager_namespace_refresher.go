// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package replication

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
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

func NewEagerNamespaceRefresher(
	metadataManager persistence.MetadataManager,
	namespaceRegistry namespace.Registry,
	logger log.Logger) EagerNamespaceRefresher {
	return &eagerNamespaceRefresherImpl{
		metadataManager:   metadataManager,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
	}
}

func (e *eagerNamespaceRefresherImpl) UpdateNamespaceFailoverVersion(namespaceId namespace.ID, targetFailoverVersion int64) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceId)
	switch err.(type) {
	case nil:
	case *serviceerror.NamespaceNotFound:
		// TODO: Handle NamespaceNotFound case, probably retrieve the namespace from the source cluster?
		return nil
	default:
		// do nothing as this is the best effort to update the namespace
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
		return err
	}

	currentFailoverVersion := resp.Namespace.FailoverVersion
	if currentFailoverVersion >= targetFailoverVersion {
		// DB may have a fresher version of namespace, so compare again
		return nil
	}

	metadata, err := e.metadataManager.GetMetadata(ctx)
	if err != nil {
		e.logger.Debug("Failed to get metadata", tag.Error(err))
		return err
	}

	request := &persistence.UpdateNamespaceRequest{
		Namespace:           resp.Namespace,
		NotificationVersion: metadata.NotificationVersion,
		IsGlobalNamespace:   resp.IsGlobalNamespace,
	}

	request.Namespace.FailoverVersion = targetFailoverVersion
	request.Namespace.FailoverNotificationVersion = metadata.NotificationVersion

	// Question: is it ok to only update failover version WITHOUT updating FailoverHistory?
	// request.Namespace.ReplicationConfig.FailoverHistory = ??

	if err := e.metadataManager.UpdateNamespace(ctx, request); err != nil {
		e.logger.Info("Failed to update namespace", tag.Error(err))
		return err
	}
	return nil
}
