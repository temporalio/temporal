package reclaimresources

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		logger            log.Logger
	}

	LocalActivities struct {
		visibilityManager manager.VisibilityManager
		metadataManager   persistence.MetadataManager

		namespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn

		logger log.Logger
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
	namespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn,
	logger log.Logger,
) *LocalActivities {
	return &LocalActivities{
		visibilityManager: visibilityManager,
		metadataManager:   metadataManager,
		logger:            logger,

		namespaceCacheRefreshInterval: namespaceCacheRefreshInterval,
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
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
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
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
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

func (a *LocalActivities) GetNamespaceCacheRefreshInterval(_ context.Context) (time.Duration, error) {
	return a.namespaceCacheRefreshInterval(), nil
}
