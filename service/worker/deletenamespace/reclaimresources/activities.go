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
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
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
	metricsHandler metrics.Handler,
	logger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager: visibilityManager,
		metricsHandler:    metricsHandler.WithTags(metrics.OperationTag(metrics.ReclaimResourcesWorkflowScope)),
		logger:            logger,
	}
}

func NewLocalActivities(
	visibilityManager manager.VisibilityManager,
	metadataManager persistence.MetadataManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *LocalActivities {
	return &LocalActivities{
		visibilityManager: visibilityManager,
		metadataManager:   metadataManager,
		metricsHandler:    metricsHandler.WithTags(metrics.OperationTag(metrics.ReclaimResourcesWorkflowScope)),
		logger:            logger,
	}
}

func (a *LocalActivities) IsAdvancedVisibilityActivity(_ context.Context, nsName namespace.Name) (bool, error) {
	switch a.visibilityManager.GetReadStoreName(nsName) {
	case elasticsearch.PersistenceName, mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
		return true, nil
	default:
		return false, nil
	}
}

func (a *LocalActivities) CountExecutionsAdvVisibilityActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (int64, error) {
	ctx = headers.SetCallerName(ctx, nsName.String())

	req := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: nsID,
		Namespace:   nsName,
		Query:       searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, req)
	if err != nil {
		metrics.CountExecutionsFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to count workflow executions.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return 0, err
	}

	if resp.Count > 0 {
		a.logger.Info("There are some workflows in namespace.", tag.WorkflowNamespace(nsName.String()), tag.Counter(int(resp.Count)))
	} else {
		a.logger.Info("There are no workflows in namespace.", tag.WorkflowNamespace(nsName.String()))
	}
	return resp.Count, err
}

func (a *Activities) EnsureNoExecutionsAdvVisibilityActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name, notDeletedCount int) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	req := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: nsID,
		Namespace:   nsName,
		Query:       searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, req)
	if err != nil {
		metrics.CountExecutionsFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to count workflow executions.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	count := int(resp.Count)
	if count > notDeletedCount {
		activityInfo := activity.GetInfo(ctx)
		// Starting from 8th attempt (after ~127s), workflow executions deletion must show some progress.
		if activity.HasHeartbeatDetails(ctx) && activityInfo.Attempt > 7 {
			var previousAttemptCount int
			if err := activity.GetHeartbeatDetails(ctx, &previousAttemptCount); err != nil {
				metrics.CountExecutionsFailuresCount.With(a.metricsHandler).Record(1)
				a.logger.Error("Unable to get previous heartbeat details.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
				return err
			}
			if count == previousAttemptCount {
				// No progress were made. Something bad happened on task processor side or new executions were created during deletion.
				// Return non-retryable error and workflow will try to delete executions again.
				a.logger.Warn("No progress were made.", tag.WorkflowNamespace(nsName.String()), tag.Attempt(activityInfo.Attempt), tag.Counter(count))
				return errors.NewNoProgressError(count)
			}
		}

		a.logger.Warn("Some workflow executions still exist.", tag.WorkflowNamespace(nsName.String()), tag.Counter(count))
		activity.RecordHeartbeat(ctx, count)
		return errors.NewExecutionsStillExistError(count)
	}

	if notDeletedCount > 0 {
		a.logger.Warn("Some workflow executions were not deleted and still exist.", tag.WorkflowNamespace(nsName.String()), tag.Counter(notDeletedCount))
		return errors.NewNotDeletedExecutionsStillExistError(notDeletedCount)
	}

	a.logger.Info("All workflow executions are deleted successfully.", tag.WorkflowNamespace(nsName.String()))
	return nil
}

func (a *Activities) EnsureNoExecutionsStdVisibilityActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	// Standard visibility does not support CountWorkflowExecutions but only supports ListWorkflowExecutions.
	// To prevent read of many records from DB, set PageSize to 1 and use this single record as indicator of workflow executions existence.
	// Unfortunately, this doesn't allow to report progress and retry is limited only by timeout.
	// TODO: remove this activity after CountWorkflowExecutions is implemented in standard visibility.

	ctx = headers.SetCallerName(ctx, nsName.String())

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: nsID,
		Namespace:   nsName,
		PageSize:    1,
		Query:       searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
	if err != nil {
		metrics.ListExecutionsFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to count workflow executions using list.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	if len(resp.Executions) > 0 {
		a.logger.Warn("Some workflow executions still exist.", tag.WorkflowNamespace(nsName.String()))
		return errors.NewSomeExecutionsStillExistError()
	}

	a.logger.Info("All workflow executions are deleted successfully.", tag.WorkflowNamespace(nsName.String()))
	return nil
}

func (a *LocalActivities) DeleteNamespaceActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	deleteNamespaceRequest := &persistence.DeleteNamespaceByNameRequest{
		Name: nsName.String(),
	}

	err := a.metadataManager.DeleteNamespaceByName(ctx, deleteNamespaceRequest)
	if err != nil {
		metrics.DeleteNamespaceFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to delete namespace from persistence.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	metrics.DeleteNamespaceSuccessCount.With(a.metricsHandler).Record(1)
	a.logger.Info("Namespace is deleted.", tag.WorkflowNamespace(nsName.String()), tag.WorkflowNamespaceID(nsID.String()))
	return nil
}
