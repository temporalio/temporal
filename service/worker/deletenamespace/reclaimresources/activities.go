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
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		metricsHandler    metrics.Handler
		logger            log.Logger
	}

	LocalActivities struct {
		visibilityManager manager.VisibilityManager
		metadataManager   persistence.MetadataManager
		metricsHandler    metrics.Handler
		logger            log.Logger
	}
)

func NewActivities(
	visibilityManager manager.VisibilityManager,
	logger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager: visibilityManager,
		logger:            logger,
	}
}

func NewLocalActivities(
	visibilityManager manager.VisibilityManager,
	metadataManager persistence.MetadataManager,
	logger log.Logger,
) *LocalActivities {
	return &LocalActivities{
		visibilityManager: visibilityManager,
		metadataManager:   metadataManager,
		logger:            logger,
	}
}

func (a *LocalActivities) IsAdvancedVisibilityActivity(_ context.Context, _ namespace.Name) (bool, error) {
	return true, nil
}

func (a *LocalActivities) CountExecutionsAdvVisibilityActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (int64, error) {
	ctx = headers.SetCallerName(ctx, nsName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	req := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: nsID,
		Namespace:   nsName,
		Query:       searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, req)
	if err != nil {
		logger.Error("Unable to count workflow executions.", tag.Error(err))
		return 0, err
	}

	if resp.Count > 0 {
		logger.Info("There are some workflows in namespace.", tag.Counter(int(resp.Count)))
	} else {
		logger.Info("There are no workflows in namespace.")
	}
	return resp.Count, err
}

func (a *Activities) EnsureNoExecutionsAdvVisibilityActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name, notDeletedCount int) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	req := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: nsID,
		Namespace:   nsName,
		Query:       searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, req)
	if err != nil {
		logger.Error("Unable to count workflow executions.", tag.Error(err))
		return err
	}

	count := int(resp.Count)
	if count > notDeletedCount {
		activityInfo := activity.GetInfo(ctx)
		// Starting from the 8th attempt (after ~127s), workflow executions deletion must show some progress.
		if activity.HasHeartbeatDetails(ctx) && activityInfo.Attempt > 7 {
			var previousAttemptCount int
			if err := activity.GetHeartbeatDetails(ctx, &previousAttemptCount); err != nil {
				logger.Error("Unable to get previous heartbeat details.", tag.Error(err))
				return err
			}
			if count == previousAttemptCount {
				// No progress was made. Something bad happened on the task processor side or new executions were created during deletion.
				// Return non-retryable error and workflow will try to delete executions again.
				logger.Warn("No progress was made.", tag.Attempt(activityInfo.Attempt), tag.Counter(count))
				return errors.NewNoProgress(count)
			}
		}

		logger.Warn("Some workflow executions still exist.", tag.Counter(count))
		activity.RecordHeartbeat(ctx, count)
		return errors.NewExecutionsStillExist(count)
	}

	if notDeletedCount > 0 {
		logger.Warn("Some workflow executions were not deleted and still exist.", tag.Counter(notDeletedCount))
		return errors.NewNotDeletedExecutionsStillExist(notDeletedCount)
	}

	logger.Info("All workflow executions are deleted successfully.")
	return nil
}

func (a *LocalActivities) DeleteNamespaceActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	deleteNamespaceRequest := &persistence.DeleteNamespaceByNameRequest{
		Name: nsName.String(),
	}

	err := a.metadataManager.DeleteNamespaceByName(ctx, deleteNamespaceRequest)
	if err != nil {
		logger.Error("Unable to delete namespace from persistence.", tag.Error(err))
		return err
	}

	logger.Info("Namespace is deleted.")
	return nil
}
