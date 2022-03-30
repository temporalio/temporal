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

package reclaimresources

import (
	"context"

	"go.temporal.io/sdk/activity"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		metadataManager   persistence.MetadataManager
		metricsClient     metrics.Client
		logger            log.Logger
	}
)

func NewActivities(
	visibilityManager manager.VisibilityManager,
	metadataManager persistence.MetadataManager,
	metricsClient metrics.Client,
	logger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager: visibilityManager,
		metadataManager:   metadataManager,
		metricsClient:     metricsClient,
		logger:            logger,
	}
}

func (a *Activities) EnsureNoExecutionsActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	// TODO: remove this check after CountWorkflowExecutions is implemented in standard visibility.
	count := int64(0)
	if a.visibilityManager.GetName() == "elasticsearch" {
		req := &manager.CountWorkflowExecutionsRequest{
			NamespaceID: nsID,
			Namespace:   nsName,
		}
		resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, req)
		if err != nil {
			a.metricsClient.IncCounter(metrics.ReclaimResourcesWorkflowScope, metrics.ListExecutionsFailuresCount)
			a.logger.Error("Unable to count workflow executions.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
			return err
		}

		count = resp.Count
	} else {
		req := &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: nsID,
			Namespace:   nsName,
			PageSize:    1,
		}
		resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
		if err != nil {
			a.metricsClient.IncCounter(metrics.ReclaimResourcesWorkflowScope, metrics.ListExecutionsFailuresCount)
			a.logger.Error("Unable to count workflow executions using list.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
			return err
		}
		// If not 0, it will always be 1 due to PageSize set to 1.
		count = int64(len(resp.Executions))
	}

	if count > 0 {
		a.logger.Warn("Some workflow executions still exist.", tag.WorkflowNamespace(nsName.String()), tag.Counter(int(count)))
		activity.RecordHeartbeat(ctx, count)
		return errors.ErrExecutionsStillExist
	}
	return nil
}

func (a *Activities) DeleteNamespaceActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	deleteNamespaceRequest := &persistence.DeleteNamespaceByNameRequest{
		Name: nsName.String(),
	}

	err := a.metadataManager.DeleteNamespaceByName(ctx, deleteNamespaceRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.ReclaimResourcesWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to delete namespace from persistence.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	a.metricsClient.IncCounter(metrics.ReclaimResourcesWorkflowScope, metrics.DeleteNamespaceSuccessCount)
	a.logger.Info("Namespace is deleted.", tag.WorkflowNamespace(nsName.String()), tag.WorkflowNamespaceID(nsID.String()))
	return nil
}
