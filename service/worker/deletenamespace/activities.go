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

package deletenamespace

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	activities struct {
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
) *activities {
	return &activities{
		visibilityManager: visibilityManager,
		metadataManager:   metadataManager,
		metricsClient:     metricsClient,
		logger:            logger,
	}
}

func (a *activities) GetNamespaceIDActivity(_ context.Context, nsName namespace.Name) (namespace.ID, error) {
	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
	}

	getNamespaceResponse, err := a.metadataManager.GetNamespace(getNamespaceRequest)
	if err != nil {
		return namespace.EmptyID, err
	}

	if getNamespaceResponse.Namespace == nil || getNamespaceResponse.Namespace.Info == nil || getNamespaceResponse.Namespace.Info.Id == "" {
		return namespace.EmptyID, nil
	}

	return namespace.ID(getNamespaceResponse.Namespace.Info.Id), nil
}

func (a *activities) MarkNamespaceDeletedActivity(_ context.Context, nsName namespace.Name) error {
	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
	}

	metadata, err := a.metadataManager.GetMetadata()
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to get cluster metadata.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns, err := a.metadataManager.GetNamespace(getNamespaceRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to get namespace details.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns.Namespace.Info.State = enumspb.NAMESPACE_STATE_DELETED

	updateRequest := &persistence.UpdateNamespaceRequest{
		Namespace:           ns.Namespace,
		IsGlobalNamespace:   ns.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}

	err = a.metadataManager.UpdateNamespace(updateRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to update namespace state to Deleted.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}
	return nil
}

func (a *activities) CheckExecutionsExistActivity(_ context.Context, nsID namespace.ID, nsName namespace.Name) (bool, error) {
	if a.visibilityManager.GetName() == "elasticsearch" {
		req := &manager.CountWorkflowExecutionsRequest{
			NamespaceID: nsID,
			Namespace:   nsName,
		}
		resp, err := a.visibilityManager.CountWorkflowExecutions(req)
		if err != nil {
			a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
			a.logger.Error("Unable to count workflows.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
			return false, err
		}

		return resp.Count > 0, nil
	}

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: nsID,
		Namespace:   nsName,
		PageSize:    1,
	}
	resp, err := a.visibilityManager.ListWorkflowExecutions(req)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to count workflows using list.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return false, err
	}

	return len(resp.Executions) > 0, nil
}

func (a *activities) DeleteNamespaceActivity(_ context.Context, nsName namespace.Name, nsID namespace.ID) error {
	deleteNamespaceRequest := &persistence.DeleteNamespaceRequest{
		ID: nsID.String(),
	}

	err := a.metadataManager.DeleteNamespace(deleteNamespaceRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable delete namespace from persistence.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	a.logger.Info("Namespace is deleted.", tag.WorkflowNamespace(nsName.String()), tag.WorkflowNamespaceID(nsID.String()))
	return nil
}
