package frontend

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errRestoreSystemNamespace = serviceerror.NewFailedPrecondition("The system namespace can't be restored.")

// restoreActiveElsewhereError is returned when a namespace lifecycle call has to run on the
// namespace's active cluster. The restore record is written only there, by the lifecycle APIs and by
// the restore job, so one database orders the writes.
type restoreActiveElsewhereError struct {
	activeCluster string
}

func (e *restoreActiveElsewhereError) Error() string {
	return fmt.Sprintf("the namespace has a running restore and is active on cluster %s", e.activeCluster)
}

// startRestore moves the namespace to REGISTERED and records a running restore hosted on this
// cluster, in one UpdateNamespace write. If this cluster's restore is already running, as on a
// retry, it keeps it and replicates the namespace again. validatePeers runs after the local checks
// and before the write. It returns the namespace as written: its ConfigVersion is the restore
// version.
func (d *namespaceHandler) startRestore(
	ctx context.Context,
	nsName namespace.Name,
	nsID namespace.ID,
	validatePeers func(*persistencespb.NamespaceDetail) error,
) (*persistencespb.NamespaceDetail, error) {
	_, detail, err := d.updateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:  nsName.String(),
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{State: enumspb.NAMESPACE_STATE_REGISTERED},
	}, internalUpdateOptions{
		namespaceID:  nsID,
		allowRestore: true,
		mutate: func(existing *persistence.GetNamespaceResponse) (bool, error) {
			return d.recordRestore(existing, validatePeers)
		},
	})
	return detail, err
}

// recordRestore checks that the namespace can be restored on this cluster and records a running
// restore hosted here. It changes nothing if this cluster's restore is already running.
func (d *namespaceHandler) recordRestore(
	existing *persistence.GetNamespaceResponse,
	validatePeers func(*persistencespb.NamespaceDetail) error,
) (bool, error) {
	detail := existing.Namespace
	if detail.Info.Id == primitives.SystemNamespaceID {
		return false, errRestoreSystemNamespace
	}
	state := detail.Info.State
	if state != enumspb.NAMESPACE_STATE_REGISTERED && state != enumspb.NAMESPACE_STATE_SOFT_DELETED {
		return false, serviceerror.NewFailedPreconditionf(
			"Namespace %s is in state %s; only a %s or %s namespace can be restored.",
			detail.Info.Name, state, enumspb.NAMESPACE_STATE_SOFT_DELETED, enumspb.NAMESPACE_STATE_REGISTERED,
		)
	}
	// The job runs here, and stops itself once this cluster isn't active.
	currentCluster := d.clusterMetadata.GetCurrentClusterName()
	activeCluster, err := d.activeCluster(existing)
	if err != nil {
		return false, err
	}
	if activeCluster != currentCluster {
		return false, serviceerror.NewNamespaceNotActive(detail.Info.Name, currentCluster, activeCluster)
	}
	if err := validatePeers(detail); err != nil {
		return false, err
	}

	// A running restore hosted on another cluster was orphaned by a failover: its job stops once it
	// sees this cluster is active, so a new restore here replaces it.
	restore := detail.Info.GetRestoreInfo()
	if state == enumspb.NAMESPACE_STATE_REGISTERED &&
		restore.GetState() == enumspb.NAMESPACE_RESTORE_STATE_RUNNING &&
		restore.GetHostCluster() == currentCluster {
		return false, nil
	}
	detail.Info.RestoreInfo = &persistencespb.NamespaceRestoreInfo{
		RestoreId:   uuid.NewString(),
		HostCluster: currentCluster,
		State:       enumspb.NAMESPACE_RESTORE_STATE_RUNNING,
		StartTime:   timestamppb.New(d.timeSource.Now()),
	}
	return true, nil
}

// CloseNamespaceRestore moves the namespace's running restore to state, which is COMPLETED or
// FAILED. A repeated call with the same state replicates the namespace again.
func (d *namespaceHandler) CloseNamespaceRestore(
	ctx context.Context,
	nsID namespace.ID,
	restoreID string,
	state enumspb.NamespaceRestoreState,
) error {
	if state != enumspb.NAMESPACE_RESTORE_STATE_COMPLETED && state != enumspb.NAMESPACE_RESTORE_STATE_FAILED {
		return serviceerror.NewInvalidArgumentf("A restore can only be closed as %s or %s.",
			enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, enumspb.NAMESPACE_RESTORE_STATE_FAILED)
	}
	_, _, err := d.updateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{}, internalUpdateOptions{
		namespaceID: nsID,
		mutate: func(existing *persistence.GetNamespaceResponse) (bool, error) {
			restore := existing.Namespace.Info.GetRestoreInfo()
			if restore.GetRestoreId() != restoreID {
				return false, serviceerror.NewFailedPreconditionf("Restore %s is not the current restore of namespace %s.",
					restoreID, existing.Namespace.Info.Name)
			}
			switch restore.GetState() {
			case state:
				return false, nil
			case enumspb.NAMESPACE_RESTORE_STATE_RUNNING:
				restore.State = state
				restore.CloseTime = timestamppb.New(d.timeSource.Now())
				return true, nil
			default:
				return false, serviceerror.NewFailedPreconditionf("Restore %s of namespace %s is already %s.",
					restoreID, existing.Namespace.Info.Name, restore.GetState())
			}
		},
	})
	return err
}

// failRunningRestore marks the namespace's running restore FAILED, for a soft delete that abandons
// it. If the namespace is active on another cluster, it returns *restoreActiveElsewhereError, so the
// soft delete runs there instead. The restore's host may differ from the active cluster after a
// failover, and may be down.
func (d *namespaceHandler) failRunningRestore(existing *persistence.GetNamespaceResponse) (bool, error) {
	restore := existing.Namespace.Info.GetRestoreInfo()
	if restore.GetState() != enumspb.NAMESPACE_RESTORE_STATE_RUNNING {
		return false, nil
	}
	activeCluster, err := d.activeCluster(existing)
	if err != nil {
		return false, err
	}
	if activeCluster != d.clusterMetadata.GetCurrentClusterName() {
		return false, &restoreActiveElsewhereError{activeCluster: activeCluster}
	}
	restore.State = enumspb.NAMESPACE_RESTORE_STATE_FAILED
	restore.CloseTime = timestamppb.New(d.timeSource.Now())
	return true, nil
}

// activeCluster returns the namespace's active cluster. The restore covers the whole namespace, so
// this is the namespace-level active cluster.
func (d *namespaceHandler) activeCluster(existing *persistence.GetNamespaceResponse) (string, error) {
	detail := existing.Namespace
	ns, err := namespace.FromPersistentState(detail, d.replicationResolverFactory(detail), namespace.WithGlobalFlag(existing.IsGlobalNamespace))
	if err != nil {
		return "", err
	}
	return ns.ActiveClusterName(namespace.RoutingKey{}), nil
}
