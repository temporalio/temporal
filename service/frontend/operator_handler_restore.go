package frontend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/restorenamespace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// restoreForwardedHeader marks a call forwarded to another cluster, so it isn't forwarded again.
	restoreForwardedHeader = "temporal-namespace-restore-forwarded"

	restoreConvergencePollInterval = 200 * time.Millisecond
)

// RestoreNamespace returns a soft-deleted namespace to service: it writes REGISTERED and a running
// restore, waits for every cluster to apply them, and starts the restore job, which refreshes every
// execution.
func (h *OperatorHandlerImpl) RestoreNamespace(
	ctx context.Context,
	request *operatorservice.RestoreNamespaceRequest,
) (_ *operatorservice.RestoreNamespaceResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	detail, err := h.namespaceHandler.startRestore(
		ctx,
		namespace.Name(request.GetNamespace()),
		namespace.ID(request.GetNamespaceId()),
		func(detail *persistencespb.NamespaceDetail) error { return h.validateRestorePeers(ctx, detail) },
	)
	if err != nil {
		return nil, err
	}
	if err := h.awaitRestoreConvergence(ctx, detail); err != nil {
		return nil, err
	}

	restore := detail.GetInfo().GetRestoreInfo()
	_, err = h.sdkClientFactory.GetSystemClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			ID:                       restorenamespace.WorkflowID(detail.GetInfo().GetId(), restore.GetRestoreId()),
			TaskQueue:                primitives.DefaultWorkerTaskQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		},
		restorenamespace.WorkflowName,
		restorenamespace.RestoreJobInput{
			NamespaceID:    detail.GetInfo().GetId(),
			RestoreID:      restore.GetRestoreId(),
			RestoreVersion: detail.GetConfigVersion(),
		},
	)
	if err != nil {
		return nil, err
	}
	h.logger.Info("Namespace restore started.",
		tag.WorkflowNamespace(detail.GetInfo().GetName()),
		tag.WorkflowNamespaceID(detail.GetInfo().GetId()),
		tag.NewStringTag("restore-id", restore.GetRestoreId()))
	return &operatorservice.RestoreNamespaceResponse{}, nil
}

// DescribeNamespaceRestore reports the namespace's most recent restore. The record comes from the
// namespace, and progress from the job on the restore's host.
func (h *OperatorHandlerImpl) DescribeNamespaceRestore(
	ctx context.Context,
	request *operatorservice.DescribeNamespaceRestoreRequest,
) (_ *operatorservice.DescribeNamespaceRestoreResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	var ns *namespace.Namespace
	var err error
	if request.GetNamespaceId() != "" {
		ns, err = h.namespaceRegistry.GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
	} else {
		ns, err = h.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	}
	if err != nil {
		return nil, err
	}
	restore := ns.Info().GetRestoreInfo()
	if restore == nil {
		return nil, serviceerror.NewNotFoundf("Namespace %s has never been restored.", ns.Name())
	}

	if host := restore.GetHostCluster(); host != h.clusterMetadata.GetCurrentClusterName() && !restoreForwarded(ctx) {
		resp, err := h.forwardDescribeNamespaceRestore(ctx, host, ns.ID())
		if err == nil {
			return resp, nil
		}
		h.logger.Warn("Unable to read namespace restore progress from its host.",
			tag.WorkflowNamespace(ns.Name().String()),
			tag.ClusterName(host),
			tag.Error(err))
		return restoreRecordResponse(restore), nil
	}

	resp := restoreRecordResponse(restore)
	h.describeRestoreJob(ctx, ns, restore, resp)
	return resp, nil
}

func (h *OperatorHandlerImpl) forwardDescribeNamespaceRestore(
	ctx context.Context,
	host string,
	nsID namespace.ID,
) (*operatorservice.DescribeNamespaceRestoreResponse, error) {
	operatorClient, err := h.remoteOperatorClient(host)
	if err != nil {
		return nil, err
	}
	return operatorClient.DescribeNamespaceRestore(
		metadata.AppendToOutgoingContext(ctx, restoreForwardedHeader, "true"),
		&operatorservice.DescribeNamespaceRestoreRequest{NamespaceId: nsID.String()},
	)
}

// forwardSoftDeleteNamespace sends a soft delete to the namespace's active cluster while a restore
// is running, so that it and the restore job's writes go to one database.
func (h *OperatorHandlerImpl) forwardSoftDeleteNamespace(
	ctx context.Context,
	activeCluster string,
	request *operatorservice.SoftDeleteNamespaceRequest,
) (*operatorservice.SoftDeleteNamespaceResponse, error) {
	if restoreForwarded(ctx) {
		return nil, serviceerror.NewUnavailablef(
			"The namespace is active on cluster %s according to this cluster, which received the call forwarded from another; retry.", activeCluster)
	}
	operatorClient, err := h.remoteOperatorClient(activeCluster)
	if err != nil {
		return nil, err
	}
	return operatorClient.SoftDeleteNamespace(metadata.AppendToOutgoingContext(ctx, restoreForwardedHeader, "true"), request)
}

// describeRestoreJob fills in the job's progress. If the job can't be read, the progress is left
// unset.
func (h *OperatorHandlerImpl) describeRestoreJob(
	ctx context.Context,
	ns *namespace.Namespace,
	restore *persistencespb.NamespaceRestoreInfo,
	resp *operatorservice.DescribeNamespaceRestoreResponse,
) {
	workflowID := restorenamespace.WorkflowID(ns.ID().String(), restore.GetRestoreId())
	desc, err := h.sdkClientFactory.GetSystemClient().DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		// NotFound: RestoreNamespace hasn't started the job yet.
		if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
			h.logger.Warn("Unable to describe the namespace restore job.",
				tag.WorkflowNamespace(ns.Name().String()),
				tag.WorkflowID(workflowID),
				tag.Error(err))
		}
		return
	}
	info := desc.GetWorkflowExecutionInfo()
	resp.RunId = info.GetExecution().GetRunId()

	var stats restorenamespace.RefreshStats
	if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		for _, pending := range desc.GetPendingActivities() {
			if pending.GetActivityType().GetName() != restorenamespace.RefreshExecutionsActivityName || pending.GetHeartbeatDetails() == nil {
				continue
			}
			var hb restorenamespace.RefreshHeartbeat
			if err := payloads.Decode(pending.GetHeartbeatDetails(), &hb); err == nil {
				stats = hb.RefreshStats
			}
		}
	} else {
		if memo := info.GetMemo().GetFields()[restorenamespace.StatsMemoKey]; memo != nil {
			_ = payload.Decode(memo, &stats)
		}
		// Terminated, or failed without updating the record.
		if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED &&
			restore.GetState() == enumspb.NAMESPACE_RESTORE_STATE_RUNNING {
			resp.State = enumspb.NAMESPACE_RESTORE_STATE_FAILED
		}
	}

	resp.TotalExecutionCount = stats.TotalEstimate
	resp.ProcessedExecutionCount = stats.ProcessedCount
	if resp.State == enumspb.NAMESPACE_RESTORE_STATE_RUNNING {
		if eta, ok := estimateCompletion(stats, time.Now()); ok {
			resp.EstimatedCompletionTime = timestamppb.New(eta)
		}
	}
}

// estimateCompletion extrapolates the refresh rate so far over the executions remaining.
func estimateCompletion(stats restorenamespace.RefreshStats, now time.Time) (time.Time, bool) {
	elapsed := now.Sub(stats.RefreshStartTime)
	if stats.RefreshStartTime.IsZero() || stats.ProcessedCount <= 0 || elapsed <= 0 {
		return time.Time{}, false
	}
	remaining := max(stats.TotalEstimate-stats.ProcessedCount, 0)
	perExecution := elapsed / time.Duration(stats.ProcessedCount)
	return now.Add(perExecution * time.Duration(remaining)), true
}

func restoreRecordResponse(restore *persistencespb.NamespaceRestoreInfo) *operatorservice.DescribeNamespaceRestoreResponse {
	return &operatorservice.DescribeNamespaceRestoreResponse{
		State:       restore.GetState(),
		HostCluster: restore.GetHostCluster(),
		StartTime:   restore.GetStartTime(),
		CloseTime:   restore.GetCloseTime(),
	}
}

// validateRestorePeers checks that no other cluster has begun hard-deleting the namespace.
func (h *OperatorHandlerImpl) validateRestorePeers(ctx context.Context, detail *persistencespb.NamespaceDetail) error {
	currentCluster := h.clusterMetadata.GetCurrentClusterName()
	for _, cluster := range h.namespaceHandler.replicationClusters(detail) {
		if cluster == currentCluster {
			continue
		}
		state, _, err := h.namespaceStateOnCluster(ctx, cluster, detail.GetInfo().GetId())
		if err != nil {
			var notFound *serviceerror.NotFound
			var nsNotFound *serviceerror.NamespaceNotFound
			if errors.As(err, &notFound) || errors.As(err, &nsNotFound) {
				return serviceerror.NewFailedPreconditionf("Namespace %s doesn't exist on cluster %s.", detail.GetInfo().GetName(), cluster)
			}
			return serviceerror.NewUnavailablef("Unable to read namespace %s on cluster %s: %v", detail.GetInfo().GetName(), cluster, err)
		}
		if state == enumspb.NAMESPACE_STATE_DELETED {
			return serviceerror.NewFailedPreconditionf("Namespace %s is %s on cluster %s.", detail.GetInfo().GetName(), state, cluster)
		}
	}
	return nil
}

// awaitRestoreConvergence waits until every cluster has the namespace REGISTERED at the restore
// version or later. It reads each cluster's database, not its registry.
func (h *OperatorHandlerImpl) awaitRestoreConvergence(ctx context.Context, detail *persistencespb.NamespaceDetail) error {
	restoreVersion := detail.GetConfigVersion()
	clusters := h.namespaceHandler.replicationClusters(detail)
	deadline := time.Now().Add(h.config.NamespaceRestoreConvergenceWait())
	for {
		var lagging []string
		for _, cluster := range clusters {
			state, configVersion, err := h.namespaceStateOnCluster(ctx, cluster, detail.GetInfo().GetId())
			switch {
			case err != nil:
				lagging = append(lagging, fmt.Sprintf("%s: %v", cluster, err))
			case state != enumspb.NAMESPACE_STATE_REGISTERED || configVersion < restoreVersion:
				lagging = append(lagging, fmt.Sprintf("%s: %s, config_version %d < %d", cluster, state, configVersion, restoreVersion))
			default:
			}
		}
		if len(lagging) == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return serviceerror.NewUnavailablef("Namespace %s has not been restored on every cluster yet; retry. Lagging: %s",
				detail.GetInfo().GetName(), strings.Join(lagging, "; "))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(restoreConvergencePollInterval):
		}
	}
}

// namespaceStateOnCluster reads the namespace's state and config version from a cluster's database.
func (h *OperatorHandlerImpl) namespaceStateOnCluster(
	ctx context.Context,
	cluster string,
	nsID string,
) (enumspb.NamespaceState, int64, error) {
	if cluster == h.clusterMetadata.GetCurrentClusterName() {
		resp, err := h.namespaceHandler.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: nsID})
		if err != nil {
			return enumspb.NAMESPACE_STATE_UNSPECIFIED, 0, err
		}
		return resp.Namespace.GetInfo().GetState(), resp.Namespace.GetConfigVersion(), nil
	}
	adminClient, err := h.clientBean.GetRemoteAdminClient(cluster)
	if err != nil {
		return enumspb.NAMESPACE_STATE_UNSPECIFIED, 0, err
	}
	resp, err := adminClient.GetNamespace(ctx, &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{Id: nsID},
	})
	if err != nil {
		return enumspb.NAMESPACE_STATE_UNSPECIFIED, 0, err
	}
	return resp.GetInfo().GetState(), resp.GetConfigVersion(), nil
}

func (h *OperatorHandlerImpl) remoteOperatorClient(cluster string) (operatorservice.OperatorServiceClient, error) {
	conn, _, err := h.clientBean.GetRemoteFrontendClient(cluster)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("Unable to reach cluster %s: %v", cluster, err)
	}
	return operatorservice.NewOperatorServiceClient(conn), nil
}

func restoreForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(restoreForwardedHeader)) > 0
}
