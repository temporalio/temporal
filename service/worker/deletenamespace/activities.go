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
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

type (
	localActivities struct {
		metadataManager persistence.MetadataManager
		logger          log.Logger
	}

	getNamespaceInfoResult struct {
		NamespaceID namespace.ID
		Namespace   namespace.Name
		Clusters    []string
	}
)

func newLocalActivities(
	metadataManager persistence.MetadataManager,
	logger log.Logger,
) *localActivities {
	return &localActivities{
		metadataManager: metadataManager,
		logger:          logger,
	}
}

func (a *localActivities) GetNamespaceInfoActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (getNamespaceInfoResult, error) {
	ctx = headers.SetCallerName(ctx, nsName.String())

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
		Clusters:    getNamespaceResponse.Namespace.ReplicationConfig.Clusters,
	}, nil
}

func (a *localActivities) MarkNamespaceDeletedActivity(ctx context.Context, nsName namespace.Name) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
	}

	metadata, err := a.metadataManager.GetMetadata(ctx)
	if err != nil {
		a.logger.Error("Unable to get cluster metadata.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns, err := a.metadataManager.GetNamespace(ctx, getNamespaceRequest)
	if err != nil {
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
		a.logger.Error("Unable to update namespace state to Deleted.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}
	return nil
}

func (a *localActivities) GenerateDeletedNamespaceNameActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (namespace.Name, error) {
	ctx = headers.SetCallerName(ctx, nsName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	const initialSuffixLength = 5

	for suffixLength := initialSuffixLength; suffixLength < len(nsID.String()); suffixLength++ { // Just in case. 5 chars from ID should be good enough.
		suffix := fmt.Sprintf("-deleted-%s", nsID.String()[:suffixLength])
		if strings.HasSuffix(nsName.String(), suffix) {
			logger.Info("Namespace is already renamed for deletion")
			return nsName, nil
		}
		newName := fmt.Sprintf("%s%s", nsName, suffix)

		_, err := a.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			Name: newName,
		})
		switch err.(type) {
		case nil:
			logger.Warn("Regenerate namespace name due to collision.", tag.NewStringTag("wf-new-namespace", newName))
		case *serviceerror.NamespaceNotFound:
			logger.Info("Generated new name for deleted namespace.", tag.NewStringTag("wf-new-namespace", newName))
			return namespace.Name(newName), nil
		default:
			logger.Error("Unable to get namespace details.", tag.Error(err))
			return namespace.EmptyName, err
		}
	}
	// Should never get here because namespace ID is guaranteed to be unique.
	panic(fmt.Sprintf("Unable to generate new name for deleted namespace %s. ID %q is not unique.", nsName, nsID))
}

func (a *localActivities) RenameNamespaceActivity(ctx context.Context, previousName namespace.Name, newName namespace.Name) error {
	if newName == previousName {
		return nil
	}

	ctx = headers.SetCallerName(ctx, previousName.String())

	renameNamespaceRequest := &persistence.RenameNamespaceRequest{
		PreviousName: previousName.String(),
		NewName:      newName.String(),
	}

	err := a.metadataManager.RenameNamespace(ctx, renameNamespaceRequest)
	if err != nil {
		a.logger.Error("Unable to rename namespace.", tag.WorkflowNamespace(previousName.String()), tag.Error(err))
		return err
	}

	a.logger.Info("Namespace renamed successfully.", tag.WorkflowNamespace(previousName.String()), tag.WorkflowNamespace(newName.String()))
	return nil
}
