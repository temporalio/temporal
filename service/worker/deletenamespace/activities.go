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
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

type (
	activities struct {
		metadataManager persistence.MetadataManager
		metricsClient   metrics.Client
		logger          log.Logger
	}

	getNamespaceInfoResult struct {
		NamespaceID namespace.ID
		Namespace   namespace.Name
	}
)

func NewActivities(
	metadataManager persistence.MetadataManager,
	metricsClient metrics.Client,
	logger log.Logger,
) *activities {
	return &activities{
		metadataManager: metadataManager,
		metricsClient:   metricsClient,
		logger:          logger,
	}
}

func (a *activities) GetNamespaceInfoActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (getNamespaceInfoResult, error) {
	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
		ID:   nsID.String(),
	}

	getNamespaceResponse, err := a.metadataManager.GetNamespace(ctx, getNamespaceRequest)
	if err != nil {
		return getNamespaceInfoResult{}, err
	}

	if getNamespaceResponse.Namespace == nil || getNamespaceResponse.Namespace.Info == nil || getNamespaceResponse.Namespace.Info.Id == "" {
		return getNamespaceInfoResult{}, temporal.NewNonRetryableApplicationError("namespace info is corrupted", "", nil)
	}

	return getNamespaceInfoResult{
		NamespaceID: namespace.ID(getNamespaceResponse.Namespace.Info.Id),
		Namespace:   namespace.Name(getNamespaceResponse.Namespace.Info.Name),
	}, nil
}

func (a *activities) MarkNamespaceDeletedActivity(ctx context.Context, nsName namespace.Name) error {
	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
	}

	metadata, err := a.metadataManager.GetMetadata(ctx)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.ReadNamespaceFailuresCount)
		a.logger.Error("Unable to get cluster metadata.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns, err := a.metadataManager.GetNamespace(ctx, getNamespaceRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.ReadNamespaceFailuresCount)
		a.logger.Error("Unable to get namespace details.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns.Namespace.Info.State = enumspb.NAMESPACE_STATE_DELETED

	updateRequest := &persistence.UpdateNamespaceRequest{
		Namespace:           ns.Namespace,
		IsGlobalNamespace:   ns.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}

	err = a.metadataManager.UpdateNamespace(ctx, updateRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.UpdateNamespaceFailuresCount)
		a.logger.Error("Unable to update namespace state to Deleted.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}
	return nil
}

func (a *activities) GenerateDeletedNamespaceNameActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (namespace.Name, error) {
	const initialSuffixLength = 5

	for suffixLength := initialSuffixLength; suffixLength < len(nsID.String()); suffixLength++ { // Just in case. 5 chars from ID should be good enough.
		newName := fmt.Sprintf("%s-deleted-%s", nsName, nsID.String()[:suffixLength])

		_, err := a.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			Name: newName,
		})
		switch err.(type) {
		case nil:
			a.logger.Warn("Regenerate namespace name due to collision.", tag.WorkflowNamespace(nsName.String()), tag.WorkflowNamespace(newName))
		case *serviceerror.NamespaceNotFound:
			a.logger.Info("Generated new name for deleted namespace.", tag.WorkflowNamespace(nsName.String()), tag.WorkflowNamespace(newName))
			return namespace.Name(newName), nil
		default:
			a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.ReadNamespaceFailuresCount)
			a.logger.Error("Unable to get namespace details.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
			return namespace.EmptyName, err
		}
	}
	// Should never get here because namespace ID is guaranteed to be unique.
	panic(fmt.Sprintf("Unable to generate new name for deleted namespace %s. ID %q is not unique.", nsName, nsID))
}

func (a *activities) RenameNamespaceActivity(ctx context.Context, previousName namespace.Name, newName namespace.Name) error {
	renameNamespaceRequest := &persistence.RenameNamespaceRequest{
		PreviousName: previousName.String(),
		NewName:      newName.String(),
	}

	err := a.metadataManager.RenameNamespace(ctx, renameNamespaceRequest)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.RenameNamespaceFailuresCount)
		a.logger.Error("Unable to rename namespace.", tag.WorkflowNamespace(previousName.String()), tag.Error(err))
		return err
	}

	a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.RenameNamespaceSuccessCount)
	a.logger.Info("Namespace renamed successfully.", tag.WorkflowNamespace(previousName.String()), tag.WorkflowNamespace(newName.String()))
	return nil
}
