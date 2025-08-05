package migration

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
	commonpb "go.temporal.io/api/common/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc/metadata"
)

type (
	SkippedWorkflowExecution struct {
		WorkflowExecution *commonpb.WorkflowExecution
		Reason            string
	}

	replicationTasksHeartbeatDetails struct {
		NextIndex                        int
		CheckPoint                       time.Time
		LastNotVerifiedWorkflowExecution *commonpb.WorkflowExecution
	}

	verifyStatus int
	verifyResult struct {
		status verifyStatus
		reason string
	}

	listWorkflowsResponse struct {
		Executions    []*commonpb.WorkflowExecution
		NextPageToken []byte
		Error         error

		// These can be used to help report progress of the force-replication scan
		LastCloseTime time.Time
		LastStartTime time.Time
	}

	countWorkflowResponse struct {
		WorkflowCount int64
	}

	generateReplicationTasksRequest struct {
		NamespaceID      string
		Executions       []*commonpb.WorkflowExecution
		RPS              float64
		GetParentInfoRPS float64
		TargetClusters   []string
	}

	verifyReplicationTasksRequest struct {
		Namespace             string
		NamespaceID           string
		TargetClusterEndpoint string
		TargetClusterName     string
		VerifyInterval        time.Duration `validate:"gte=0"`
		Executions            []*commonpb.WorkflowExecution
	}

	verifyReplicationTasksResponse struct {
		VerifiedWorkflowCount int64
	}

	metadataRequest struct {
		Namespace string
	}

	metadataResponse struct {
		ShardCount  int32
		NamespaceID string
	}

	waitCatchupRequest struct {
		TargetCluster  string
		CatchupCluster string
		Namespace      string
	}

	activities struct {
		historyShardCount                int32
		executionManager                 persistence.ExecutionManager
		taskManager                      persistence.TaskManager
		namespaceRegistry                namespace.Registry
		historyClient                    historyservice.HistoryServiceClient
		frontendClient                   workflowservice.WorkflowServiceClient
		adminClient                      adminservice.AdminServiceClient
		clientFactory                    serverClient.Factory
		clientBean                       serverClient.Bean
		logger                           log.Logger
		metricsHandler                   metrics.Handler
		forceReplicationMetricsHandler   metrics.Handler
		namespaceReplicationQueue        persistence.NamespaceReplicationQueue
		generateMigrationTaskViaFrontend dynamicconfig.BoolPropertyFn
	}
)

const (
	reasonZombieWorkflow           = "Zombie workflow"
	reasonWorkflowNotFound         = "Workflow not found"
	reasonWorkflowCloseToRetention = "Workflow close to retention"

	notVerified verifyStatus = 0
	verified    verifyStatus = 1
	skipped     verifyStatus = 2
)

func (r verifyResult) isVerified() bool {
	return r.status == verified || r.status == skipped
}

// TODO: CallerTypePreemptablee should be set in activity background context for all migration activities.
// However, activity background context is per-worker, which means once set, all activities processed by the
// worker will use CallerType Preemptable, including those not related to migration. This is not ideal.
// Using a different task queue and a dedicated worker for migration can solve the issue but requires
// changing all existing tooling around namespace migration to start workflows & activities on the new task queue.
// Another approach is to use separate workers for workflow tasks and activities and keep existing tooling unchanged.

// GetMetadata returns history shard count and namespaceID for requested namespace.
func (a *activities) GetMetadata(_ context.Context, request metadataRequest) (*metadataResponse, error) {
	nsEntry, err := a.namespaceRegistry.GetNamespace(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}

	return &metadataResponse{
		ShardCount:  a.historyShardCount,
		NamespaceID: string(nsEntry.ID()),
	}, nil
}

// GetMaxReplicationTaskIDs returns max replication task id per shard
func (a *activities) GetMaxReplicationTaskIDs(ctx context.Context) (*replicationStatus, error) {
	ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{})
	if err != nil {
		return nil, err
	}
	result := &replicationStatus{MaxReplicationTaskIds: make(map[int32]int64)}
	for _, shard := range resp.Shards {
		result.MaxReplicationTaskIds[shard.ShardId] = shard.MaxReplicationTaskId
	}
	return result, nil
}

func (a *activities) WaitReplication(ctx context.Context, waitRequest waitReplicationRequest) error {
	ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)

	for {
		done, err := a.checkReplicationOnce(ctx, waitRequest)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		// keep waiting and check again
		time.Sleep(time.Second)
		activity.RecordHeartbeat(ctx, nil)
	}
}

// Check if remote cluster has caught up on all shards on replication tasks
func (a *activities) checkReplicationOnce(ctx context.Context, waitRequest waitReplicationRequest) (bool, error) {

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters: []string{waitRequest.RemoteCluster},
	})
	if err != nil {
		return false, err
	}
	if int(waitRequest.ShardCount) != len(resp.Shards) {
		return false, fmt.Errorf("GetReplicationStatus returns %d shards, expecting %d", len(resp.Shards), waitRequest.ShardCount)
	}

	// check that every shard has caught up
	readyShardCount := 0
	logged := false

	sort.SliceStable(resp.Shards, func(i, j int) bool {
		return resp.Shards[i].ShardId < resp.Shards[j].ShardId
	})

	for _, shard := range resp.Shards {
		clusterInfo, hasClusterInfo := shard.RemoteClusters[waitRequest.RemoteCluster]
		if hasClusterInfo {
			// WE are all caught up
			if shard.MaxReplicationTaskId == clusterInfo.AckedTaskId {
				readyShardCount++
				continue
			}

			// Caught up to the last checked IDs, and within allowed lagging range
			if clusterInfo.AckedTaskId >= waitRequest.WaitForTaskIds[shard.ShardId] &&
				(shard.MaxReplicationTaskId-clusterInfo.AckedTaskId <= waitRequest.AllowedLaggingTasks ||
					shard.MaxReplicationTaskVisibilityTime.AsTime().Sub(clusterInfo.AckedTaskVisibilityTime.AsTime()) <= waitRequest.AllowedLagging) {
				readyShardCount++
				continue
			}
		}

		// shard is not ready, log first non-ready shard
		if !logged {
			logged = true
			if !hasClusterInfo {
				a.logger.Info("Wait catchup missing remote cluster info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.RemoteCluster))
				// this is not expected, so fail activity to surface the error, but retryPolicy will keep retrying.
				return false, fmt.Errorf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.RemoteCluster)
			}
			a.logger.Info("Wait catchup not ready",
				tag.NewInt32("ShardId", shard.ShardId),
				tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
				tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
				tag.NewInt64("WaitForTaskId", waitRequest.WaitForTaskIds[shard.ShardId]),
				tag.NewDurationTag("AllowedLagging", waitRequest.AllowedLagging),
				tag.NewDurationTag("ActualLagging", shard.MaxReplicationTaskVisibilityTime.AsTime().Sub(clusterInfo.AckedTaskVisibilityTime.AsTime())),
				tag.NewInt64("MaxReplicationTaskId", shard.MaxReplicationTaskId),
				tag.NewTimeTag("MaxReplicationTaskVisibilityTime", shard.MaxReplicationTaskVisibilityTime.AsTime()),
				tag.NewTimeTag("AckedTaskVisibilityTime", clusterInfo.AckedTaskVisibilityTime.AsTime()),
				tag.NewInt64("AllowedLaggingTasks", waitRequest.AllowedLaggingTasks),
				tag.NewInt64("ActualLaggingTasks", shard.MaxReplicationTaskId-clusterInfo.AckedTaskId),
			)
		}
	}

	// emit metrics about how many shards are ready
	a.metricsHandler.Gauge(metrics.CatchUpReadyShardCountGauge.Name()).Record(
		float64(readyShardCount),
		metrics.OperationTag(metrics.MigrationWorkflowScope),
		metrics.TargetClusterTag(waitRequest.RemoteCluster))

	return readyShardCount == len(resp.Shards), nil
}

func (a *activities) WaitHandover(ctx context.Context, waitRequest waitHandoverRequest) error {
	// Use the highest priority caller type for checking handover state
	// since during handover state namespace has no availability
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(waitRequest.Namespace, headers.CallerTypeAPI, ""))

	for {
		done, err := a.checkHandoverOnce(ctx, waitRequest)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		// keep waiting and check again
		time.Sleep(time.Second)
		activity.RecordHeartbeat(ctx, nil)
	}
}

// Check if remote cluster has caught up on all shards on replication tasks
func (a *activities) checkHandoverOnce(ctx context.Context, waitRequest waitHandoverRequest) (bool, error) {

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters: []string{waitRequest.RemoteCluster},
	})
	if err != nil {
		return false, err
	}
	if int(waitRequest.ShardCount) != len(resp.Shards) {
		return false, fmt.Errorf("GetReplicationStatus returns %d shards, expecting %d", len(resp.Shards), waitRequest.ShardCount)
	}

	readyShardCount := 0
	logged := false
	// check that every shard is ready to handover
	for _, shard := range resp.Shards {
		clusterInfo, hasClusterInfo := shard.RemoteClusters[waitRequest.RemoteCluster]
		handoverInfo, hasHandoverInfo := shard.HandoverNamespaces[waitRequest.Namespace]
		if hasClusterInfo && hasHandoverInfo {
			if clusterInfo.AckedTaskId >= handoverInfo.HandoverReplicationTaskId {
				readyShardCount++
				continue
			}
		}
		// shard is not ready, log first non-ready shard
		if !logged {
			logged = true
			if !hasClusterInfo {
				a.logger.Info("Wait handover missing remote cluster info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.RemoteCluster))
				// this is not expected, so fail activity to surface the error, but retryPolicy will keep retrying.
				return false, fmt.Errorf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.RemoteCluster)
			}

			if !hasHandoverInfo {
				// this could happen before namespace cache refresh
				a.logger.Info("Wait handover missing handover namespace info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.RemoteCluster), tag.WorkflowNamespace(waitRequest.Namespace))
			} else {
				a.logger.Info("Wait handover not ready",
					tag.NewInt32("ShardId", shard.ShardId),
					tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
					tag.NewInt64("HandoverTaskId", handoverInfo.HandoverReplicationTaskId),
					tag.NewStringTag("Namespace", waitRequest.Namespace),
					tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
					tag.NewInt64("MaxReplicationTaskId", shard.MaxReplicationTaskId),
				)
			}
		}
	}

	// emit metrics about how many shards are ready
	a.metricsHandler.Gauge(metrics.HandoverReadyShardCountGauge.Name()).Record(
		float64(readyShardCount),
		metrics.OperationTag(metrics.MigrationWorkflowScope),
		metrics.TargetClusterTag(waitRequest.RemoteCluster),
		metrics.NamespaceTag(waitRequest.Namespace))
	a.logger.Info("Wait handover ready shard count.",
		tag.NewInt("ReadyShards", readyShardCount),
		tag.NewStringTag("Namespace", waitRequest.Namespace),
		tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster))

	return readyShardCount == len(resp.Shards), nil
}

func (a *activities) generateWorkflowReplicationTask(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	namespaceName string,
	namespaceID string,
	we *commonpb.WorkflowExecution,
	targetClusters []string,
	generateViaFrontend bool,
) error {
	if err := rateLimiter.WaitN(ctx, 1); err != nil {
		return err
	}

	// will generate replication task
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	var stateTransitionCount, historyLength int64
	if generateViaFrontend {
		resp, err := a.adminClient.GenerateLastHistoryReplicationTasks(ctx, &adminservice.GenerateLastHistoryReplicationTasksRequest{
			Namespace:      namespaceName,
			Execution:      we,
			TargetClusters: targetClusters,
		})
		if err != nil {
			return err
		}
		stateTransitionCount = resp.StateTransitionCount
		historyLength = resp.HistoryLength
	} else {
		resp, err := a.historyClient.GenerateLastHistoryReplicationTasks(ctx, &historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId:    namespaceID,
			Execution:      we,
			TargetClusters: targetClusters,
		})
		if err != nil {
			return err
		}
		stateTransitionCount = resp.StateTransitionCount
		historyLength = resp.HistoryLength
	}

	// If workflow has many activity retries (bug in activity code e.g.,), the state transition count can be
	// large but the number of actual state transition that is applied on target cluster can be very small.
	// Take the minimum between StateTransitionCount and HistoryLength as heuristic to avoid unnecessary throttling
	// in such situation.
	count := min(stateTransitionCount, historyLength)
	for count > 0 {
		token := min(int(count), rateLimiter.Burst())
		count -= int64(token)
		_ = rateLimiter.ReserveN(time.Now(), token)
	}

	return nil
}

func (a *activities) UpdateNamespaceState(ctx context.Context, req updateStateRequest) error {
	// Use the highest priority caller type for updating namespace config
	// since during handover state, namespace has no availability
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(req.Namespace, headers.CallerTypeAPI, ""))

	descResp, err := a.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: req.Namespace,
	})
	if err != nil {
		return err
	}
	if descResp.ReplicationConfig.State == req.NewState {
		return nil
	}

	_, err = a.frontendClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			State: req.NewState,
		},
	})

	return err
}

func (a *activities) UpdateActiveCluster(ctx context.Context, req updateActiveClusterRequest) error {
	// Use the highest priority caller type for updating namespace config
	// since when both clusters think namespace are standby, namespace has no availability
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(req.Namespace, headers.CallerTypeAPI, ""))

	descResp, err := a.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: req.Namespace,
	})
	if err != nil {
		return err
	}
	if descResp.ReplicationConfig.GetActiveClusterName() == req.ActiveCluster {
		return nil
	}

	_, err = a.frontendClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: req.ActiveCluster,
		},
	})

	return err
}

func (a *activities) ListWorkflows(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(request.Namespace, headers.CallerTypePreemptable, ""))
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(interceptor.DCRedirectionContextHeaderName, "false"))

	// TODO: Use CHASM system List API when it is available.
	// For now, ListWorkflowExecutions is still compatible with non-workflow archetypes.
	resp, err := a.frontendClient.ListWorkflowExecutions(ctx, request)
	if err != nil {
		return nil, err
	}
	var lastCloseTime, lastStartTime time.Time

	executions := make([]*commonpb.WorkflowExecution, len(resp.Executions))
	for i, e := range resp.Executions {
		executions[i] = e.Execution

		if e.CloseTime != nil {
			lastCloseTime = e.CloseTime.AsTime()
		}

		if e.StartTime != nil {
			lastStartTime = e.StartTime.AsTime()
		}
	}
	return &listWorkflowsResponse{Executions: executions, NextPageToken: resp.NextPageToken, LastCloseTime: lastCloseTime, LastStartTime: lastStartTime}, nil
}

func (a *activities) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*countWorkflowResponse, error) {
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(request.Namespace, headers.CallerTypePreemptable, ""))

	// TODO: Use CHASM system Count API when it is available.
	// For now, ListWorkflowExecutions is still compatible with non-workflow archetypes.
	resp, err := a.frontendClient.CountWorkflowExecutions(ctx, request)
	if err != nil {
		return nil, err
	}
	return &countWorkflowResponse{
		WorkflowCount: resp.Count,
	}, nil
}

func (a *activities) GenerateReplicationTasks(ctx context.Context, request *generateReplicationTasksRequest) error {
	ctx = a.setCallerInfoForServerAPI(ctx, namespace.ID(request.NamespaceID))
	rateLimiter := quotas.NewRateLimiter(request.RPS, int(math.Ceil(request.RPS)))

	start := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.Timer(metrics.GenerateReplicationTasksLatency.Name()).Record(time.Since(start))
	}()

	startIndex := 0
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &startIndex); err == nil {
			startIndex = startIndex + 1 // start from next one
		}
	}

	namespaceName, err := a.namespaceRegistry.GetNamespaceName(namespace.ID(request.NamespaceID))
	if err != nil {
		a.logger.Error("force-replication failed to translate namespaceID to name", tag.WorkflowNamespaceID(request.NamespaceID))
		return err
	}

	generateViaFrontend := a.generateMigrationTaskViaFrontend()
	for i := startIndex; i < len(request.Executions); i++ {
		we := request.Executions[i]
		if err := a.generateWorkflowReplicationTask(
			ctx,
			rateLimiter,
			namespaceName.String(),
			request.NamespaceID,
			we,
			request.TargetClusters,
			generateViaFrontend,
		); err != nil {
			if !common.IsNotFoundError(err) {
				a.logger.Error("force-replication failed to generate replication task",
					tag.WorkflowNamespaceID(request.NamespaceID),
					tag.WorkflowID(we.GetWorkflowId()),
					tag.WorkflowRunID(we.GetRunId()),
					tag.Error(err))
				return err
			}

			a.logger.Warn("force-replication ignore replication task due to NotFoundServiceError",
				tag.WorkflowNamespaceID(request.NamespaceID),
				tag.WorkflowID(we.GetWorkflowId()),
				tag.WorkflowRunID(we.GetRunId()),
				tag.Error(err))
		}
		activity.RecordHeartbeat(ctx, i)
	}

	return nil
}

func (a *activities) setCallerInfoForServerAPI(
	ctx context.Context,
	namespaceID namespace.ID,
) context.Context {
	nsName, err := a.namespaceRegistry.GetNamespaceName(namespaceID)
	if err != nil {
		a.logger.Error("Failed to get namespace name when generating replication task",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.Error(err),
		)
		nsName = namespace.EmptyName
	}
	return headers.SetCallerInfo(ctx, headers.NewPreemptableCallerInfo(nsName.String()))
}

type seedReplicationQueueWithUserDataEntriesHeartbeatDetails struct {
	NextPageToken []byte
	IndexInPage   int
}

func (a *activities) SeedReplicationQueueWithUserDataEntries(ctx context.Context, params TaskQueueUserDataReplicationParamsWithNamespace) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("namespace is required", "InvalidArgument", nil)
	}
	if params.PageSize == 0 {
		params.PageSize = defaultPageSizeForTaskQueueUserDataReplication
	}
	if params.RPS == 0 {
		params.RPS = defaultRPSForTaskQueueUserDataReplication
	}

	describeResponse, err := a.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: params.Namespace,
	})
	if err != nil {
		return err
	}

	rateLimiter := quotas.NewRateLimiter(params.RPS, int(math.Ceil(params.RPS)))
	heartbeatDetails := seedReplicationQueueWithUserDataEntriesHeartbeatDetails{}

	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &heartbeatDetails); err != nil {
			return temporal.NewNonRetryableApplicationError("failed to load previous heartbeat details", "TypeError", err)
		}
	}

	for {
		if err := rateLimiter.Wait(ctx); err != nil {
			return err
		}

		request := &persistence.ListTaskQueueUserDataEntriesRequest{
			NamespaceID:   describeResponse.GetNamespaceInfo().Id,
			NextPageToken: heartbeatDetails.NextPageToken,
			PageSize:      params.PageSize,
		}
		response, err := a.taskManager.ListTaskQueueUserDataEntries(ctx, request)
		if err != nil {
			a.logger.Error("List task queue user data failed", tag.WorkflowNamespaceID(request.NamespaceID), tag.Error(err))
			return err
		}
		for idx, entry := range response.Entries {
			if heartbeatDetails.IndexInPage > idx {
				continue
			}
			heartbeatDetails.IndexInPage = idx
			activity.RecordHeartbeat(ctx, heartbeatDetails)
			err = a.namespaceReplicationQueue.Publish(ctx, &replicationspb.ReplicationTask{
				TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA,
				Attributes: &replicationspb.ReplicationTask_TaskQueueUserDataAttributes{
					TaskQueueUserDataAttributes: &replicationspb.TaskQueueUserDataAttributes{
						NamespaceId:   request.NamespaceID,
						TaskQueueName: entry.TaskQueue,
						UserData:      entry.UserData.GetData(),
					},
				},
			})
			if err != nil {
				a.logger.Error("Inserting into namespace replication queue failed", tag.WorkflowNamespaceID(request.NamespaceID), tag.Error(err))
				return err
			}
		}
		if len(response.NextPageToken) == 0 {
			return nil
		}
		heartbeatDetails.NextPageToken = response.NextPageToken
		heartbeatDetails.IndexInPage = 0
		activity.RecordHeartbeat(ctx, heartbeatDetails)
	}
}

func (a *activities) checkSkipWorkflowExecution(
	ctx context.Context,
	request *verifyReplicationTasksRequest,
	we *commonpb.WorkflowExecution,
	ns *namespace.Namespace,
) (verifyResult, error) {
	namespaceID := request.NamespaceID
	tags := []tag.Tag{tag.WorkflowNamespaceID(namespaceID), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId)}
	resp, err := a.historyClient.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId:     namespaceID,
		Execution:       we,
		SkipForceReload: true,
	})

	if err != nil {
		if common.IsNotFoundError(err) {
			// The outstanding workflow execution may be deleted (due to retention) on source cluster after replication tasks were generated.
			// Since retention runs on both source/target clusters, such execution may also be deleted (hence not found) from target cluster.
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.EncounterNotFoundWorkflowCount.Name()).Record(1)
			return verifyResult{
				status: skipped,
				reason: reasonWorkflowNotFound,
			}, nil
		}

		return verifyResult{
			status: notVerified,
		}, err
	}

	// Zombie workflow should be a transient state. However, if there is Zombie workflow on the source cluster,
	// it is skipped to avoid such workflow being processed on the target cluster.
	if resp.GetDatabaseMutableState().GetExecutionState().GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.EncounterZombieWorkflowCount.Name()).Record(1)
		a.logger.Info("createReplicationTasks skip Zombie workflow", tags...)
		return verifyResult{
			status: skipped,
			reason: reasonZombieWorkflow,
		}, nil
	}

	// Skip verifying workflow which has already passed retention time.
	if closeTime := resp.GetDatabaseMutableState().GetExecutionInfo().GetCloseTime(); closeTime != nil && ns != nil && ns.Retention() > 0 {
		deleteTime := closeTime.AsTime().Add(ns.Retention())
		if deleteTime.Before(time.Now()) {
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.EncounterPassRetentionWorkflowCount.Name()).Record(1)
			return verifyResult{
				status: skipped,
				reason: reasonWorkflowCloseToRetention,
			}, nil
		}
	}

	return verifyResult{
		status: notVerified,
	}, nil
}

func (a *activities) verifySingleReplicationTask(
	ctx context.Context,
	request *verifyReplicationTasksRequest,
	remotAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	we *commonpb.WorkflowExecution,
) (verifyResult, error) {
	s := time.Now()
	// Check if execution exists on remote cluster
	_, err := remotAdminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace:       request.Namespace,
		Execution:       we,
		SkipForceReload: true,
	})
	a.forceReplicationMetricsHandler.Timer(metrics.VerifyDescribeMutableStateLatency.Name()).Record(time.Since(s))

	switch err.(type) {
	case nil:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskSuccess.Name()).Record(1)
		return verifyResult{
			status: verified,
		}, nil

	case *serviceerror.NotFound:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskNotFound.Name()).Record(1)
		// Calling checkSkipWorkflowExecution for every NotFound is sub-optimal as most common case to skip is workflow being deleted due to retention.
		// A better solution is to only check the existence for workflow which is close to retention period.
		return a.checkSkipWorkflowExecution(ctx, request, we, ns)

	case *serviceerror.NamespaceNotFound:
		return verifyResult{
			status: notVerified,
		}, temporal.NewNonRetryableApplicationError("failed to describe workflow from the remote cluster", "NamespaceNotFound", err)

	default:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace), metrics.ServiceErrorTypeTag(err)).
			Counter(metrics.VerifyReplicationTaskFailed.Name()).Record(1)

		return verifyResult{
			status: notVerified,
		}, errors.WithMessage(err, "failed to describe workflow from the remote cluster")
	}
}

func (a *activities) verifyReplicationTasks(
	ctx context.Context,
	request *verifyReplicationTasksRequest,
	details *replicationTasksHeartbeatDetails,
	remotAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	heartbeat func(details replicationTasksHeartbeatDetails),
) (bool, error) {
	start := time.Now()
	progress := false
	defer func() {
		if progress {
			// Update CheckPoint when there is a progress
			details.CheckPoint = time.Now()
		}

		heartbeat(*details)
		a.forceReplicationMetricsHandler.Timer(metrics.VerifyReplicationTasksLatency.Name()).Record(time.Since(start))
	}()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(interceptor.DCRedirectionContextHeaderName, "false"))

	for ; details.NextIndex < len(request.Executions); details.NextIndex++ {
		we := request.Executions[details.NextIndex]
		r, err := a.verifySingleReplicationTask(ctx, request, remotAdminClient, ns, we)
		if err != nil {
			return false, err
		}

		if !r.isVerified() {
			details.LastNotVerifiedWorkflowExecution = we
			return false, nil
		}

		heartbeat(*details)
		progress = true
	}

	return true, nil
}

const (
	defaultNoProgressNotRetryableTimeout = 30 * time.Minute
)

func (a *activities) VerifyReplicationTasks(ctx context.Context, request *verifyReplicationTasksRequest) (verifyReplicationTasksResponse, error) {
	var response verifyReplicationTasksResponse
	var details replicationTasksHeartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &details); err != nil {
			return response, err
		}
	} else {
		details.NextIndex = 0
		details.CheckPoint = time.Now()
		activity.RecordHeartbeat(ctx, details)
	}

	remotAdminClient, err := a.clientBean.GetRemoteAdminClient(request.TargetClusterName)
	if err != nil {
		return response, err
	}

	nsEntry, err := a.namespaceRegistry.GetNamespace(namespace.Name(request.Namespace))
	if err != nil {
		return response, err
	}

	// Verify if replication tasks exist on target cluster. There are several cases where execution was not found on target cluster.
	//  1. replication lag
	//  2. Zombie workflow execution
	//  3. workflow execution was deleted (due to retention) after replication task was created
	//  4. workflow execution was not applied successfully on target cluster (i.e, bug)
	//
	// The verification step is retried for every VerifyInterval to handle #1. Verification progress
	// is recorded in activity heartbeat. The verification is considered of making progress if there was at least one new execution
	// being verified. If no progress is made for long enough, then
	//  - more than checkSkipThreshold, it checks if outstanding workflow execution can be skipped locally (#2 and #3)
	//  - more than NonRetryableTimeout, it means potentially #4. The activity returns
	//    non-retryable error and force-replication will fail.
	for {
		// Since replication has a lag, sleep first.
		time.Sleep(request.VerifyInterval)

		verified, err := a.verifyReplicationTasks(ctx, request, &details, remotAdminClient, nsEntry,
			func(d replicationTasksHeartbeatDetails) {
				activity.RecordHeartbeat(ctx, d)
			})
		if err != nil {
			return response, err
		}

		if verified {
			response.VerifiedWorkflowCount = int64(len(request.Executions))
			return response, nil
		}

		diff := time.Since(details.CheckPoint)
		if diff > defaultNoProgressNotRetryableTimeout {
			// Potentially encountered a missing execution, return non-retryable error
			return response, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("verifyReplicationTasks was not able to make progress for more than %v minutes (not retryable): could not find WorkflowExecution: '%v' in TargetCluster: '%s': Checkpoint: '%v', ",
					diff.Minutes(),
					details.LastNotVerifiedWorkflowExecution, request.TargetClusterName, details.CheckPoint),
				"",
				nil)
		}
	}
}

// WaitCatchup waits for the CatchupCluster to catch necessary data from the current cluster,
// ensuring it has caught up to the TargetCluster's ack level for the specified namespace.
func (a *activities) WaitCatchup(ctx context.Context, params CatchUpParams) error {
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(params.Namespace, headers.CallerTypeAPI, ""))

	descResp, err := a.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: params.Namespace,
	})
	if err != nil {
		return err
	}

	targetCluster := params.TargetCluster
	if targetCluster == "" {
		targetCluster = descResp.ReplicationConfig.GetActiveClusterName()
	}

	waitCatchupRequest := waitCatchupRequest{
		Namespace:      params.Namespace,
		CatchupCluster: params.CatchupCluster,
		TargetCluster:  targetCluster,
	}

	targetAckIDOnShard, err := a.getTargetClusterReplicationStatus(ctx, waitCatchupRequest)
	if err != nil {
		return err
	}

	for {
		done, err := a.checkReplicationOnRemoteCluster(ctx, waitCatchupRequest, targetAckIDOnShard)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		// keep waiting and check again
		time.Sleep(time.Second)
		activity.RecordHeartbeat(ctx, nil)
	}
}

// Check if remote cluster has caught up on all shards on replication tasks from target replica.
func (a *activities) getTargetClusterReplicationStatus(ctx context.Context, waitRequest waitCatchupRequest) (map[int32]int64, error) {
	targetAckIDOnShard := make(map[int32]int64)

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters: []string{waitRequest.TargetCluster},
	})
	if err != nil {
		return targetAckIDOnShard, err
	}

	// record the acked task id from active for each shard
	for _, shard := range resp.Shards {
		activeInfo, hasActiveInfo := shard.RemoteClusters[waitRequest.TargetCluster]
		if hasActiveInfo {
			targetAckIDOnShard[shard.ShardId] = activeInfo.AckedTaskId
		}
	}

	return targetAckIDOnShard, nil
}

// Check if remote cluster has caught up on all shards on replication tasks from target replica.
func (a *activities) checkReplicationOnRemoteCluster(ctx context.Context, waitRequest waitCatchupRequest, targetAckIDOnShard map[int32]int64) (bool, error) {

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters: []string{waitRequest.CatchupCluster},
	})
	if err != nil {
		return false, err
	}

	expectedShardCount := len(targetAckIDOnShard)

	readyShardCount := 0
	logged := false
	// check that on every shard, all source clusters have caught up with target cluster
	for _, shard := range resp.Shards {
		clusterInfo, hasClusterInfo := shard.RemoteClusters[waitRequest.CatchupCluster]
		if hasClusterInfo {
			value, exists := targetAckIDOnShard[shard.ShardId]
			// If the target acked task ID is not found, the shard is considered ready, as the remote ack level
			// is assumed to be more up-to-date than the active ack level.
			if !exists {
				readyShardCount++
				continue
			}
			// WE are all caught up
			if clusterInfo.AckedTaskId >= shard.MaxReplicationTaskId {
				readyShardCount++
				continue
			}
			if clusterInfo.AckedTaskId >= value {
				readyShardCount++
				continue
			}
		}

		// shard is not ready, log first non-ready shard
		if !logged {
			logged = true
			if !hasClusterInfo {
				a.logger.Info("Wait catchup missing remote cluster info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.CatchupCluster))
				// this is not expected, so fail activity to surface the error, but retryPolicy will keep retrying.
				return false, temporal.NewNonRetryableApplicationError(fmt.Sprintf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.CatchupCluster), "", nil)
			}

			a.logger.Info("Wait catchup not ready",
				tag.NewInt32("ShardId", shard.ShardId),
				tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
				tag.NewStringTag("Namespace", waitRequest.Namespace),
				tag.NewStringTag("CatchupCluster", waitRequest.CatchupCluster),
				tag.NewStringTag("TargetCluster", waitRequest.TargetCluster),
				tag.NewInt64("targetAckIDOnShard", targetAckIDOnShard[shard.ShardId]),
				tag.NewInt64("MaxReplicationTaskId", shard.MaxReplicationTaskId),
				tag.NewDurationTag("ActualLagging", shard.MaxReplicationTaskVisibilityTime.AsTime().Sub(clusterInfo.AckedTaskVisibilityTime.AsTime())),
				tag.NewTimeTag("MaxReplicationTaskVisibilityTime", shard.MaxReplicationTaskVisibilityTime.AsTime()),
				tag.NewTimeTag("AckedTaskVisibilityTime", clusterInfo.AckedTaskVisibilityTime.AsTime()),
				tag.NewInt64("ActualLaggingTasks", shard.MaxReplicationTaskId-clusterInfo.AckedTaskId),
			)

		}

	}

	return readyShardCount == expectedShardCount, nil
}
