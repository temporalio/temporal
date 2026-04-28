package reclaimresources

import (
	"context"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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

// OriginalNameFromDeletedName derives the original namespace name from the deleted form
// produced by GenerateDeletedNamespaceNameActivity (e.g. "myns-deleted-ABCDE" to "myns").
// Returns the input unchanged if it does not match the deleted name pattern.
func OriginalNameFromDeletedName(deletedName namespace.Name, nsID namespace.ID) namespace.Name {
	for suffixLength := 5; suffixLength <= len(nsID.String()); suffixLength++ {
		suffix := fmt.Sprintf("-deleted-%s", nsID.String()[:suffixLength])
		if strings.HasSuffix(deletedName.String(), suffix) {
			return namespace.Name(strings.TrimSuffix(deletedName.String(), suffix))
		}
	}
	return deletedName
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

// RenameNamespaceBackActivity renames the namespace from its deleted form back to originalName
// while the namespace is still in DELETED state, preserving the state gate against new traffic.
// Idempotent: does nothing if the namespace is already named originalName or if deletedName == originalName.
func (a *LocalActivities) RenameNamespaceBackActivity(ctx context.Context, nsID namespace.ID, deletedName namespace.Name, originalName namespace.Name) error {
	if deletedName == originalName {
		return nil
	}
	ctx = headers.SetCallerName(ctx, deletedName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(deletedName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	ns, err := a.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: nsID.String()})
	if err != nil {
		logger.Error("Unable to get namespace details.", tag.Error(err))
		return err
	}
	if namespace.Name(ns.Namespace.Info.Name) == originalName {
		logger.Info("Namespace already renamed back to original, skipping.")
		return nil
	}

	if err = a.metadataManager.RenameNamespace(ctx, &persistence.RenameNamespaceRequest{
		PreviousName: deletedName.String(),
		NewName:      originalName.String(),
	}); err != nil {
		logger.Error("Unable to rename namespace back to original.", tag.Error(err))
		return err
	}
	logger.Info("Namespace renamed back to original.", tag.WorkflowNamespace(originalName.String()))
	return nil
}

// RestoreNamespaceStateActivity sets the namespace state from DELETED back to REGISTERED.
// It must be called only after RenameNamespaceBackActivity succeeds so that the original
// name is already in place when traffic is readmitted.
// Idempotent: does nothing if the namespace is already REGISTERED.
func (a *LocalActivities) RestoreNamespaceStateActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	metadata, err := a.metadataManager.GetMetadata(ctx)
	if err != nil {
		logger.Error("Unable to get cluster metadata.", tag.Error(err))
		return err
	}

	ns, err := a.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: nsID.String()})
	if err != nil {
		logger.Error("Unable to get namespace details.", tag.Error(err))
		return err
	}
	if ns.Namespace.Info.State != enumspb.NAMESPACE_STATE_DELETED {
		logger.Info("Namespace is not in deleted state, skipping restore.",
			tag.NewStringTag("state", ns.Namespace.Info.State.String()))
		return nil
	}

	ns.Namespace.Info.State = enumspb.NAMESPACE_STATE_REGISTERED
	if err = a.metadataManager.UpdateNamespace(ctx, &persistence.UpdateNamespaceRequest{
		Namespace:           ns.Namespace,
		IsGlobalNamespace:   ns.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}); err != nil {
		logger.Error("Unable to update namespace state to Registered.", tag.Error(err))
		return err
	}
	logger.Info("Namespace state restored to Registered.")
	return nil
}
