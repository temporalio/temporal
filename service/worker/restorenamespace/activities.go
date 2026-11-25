package restorenamespace

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/util"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	listPageSize = 1000

	// errTypeRestoreStopped marks a refresh that must not be retried: the restore was abandoned,
	// superseded, or failed over.
	errTypeRestoreStopped = "NamespaceRestoreStopped"

	registryPollInterval = 500 * time.Millisecond
	// stuckAttempt is the attempt at which a refresh still failing on a cluster is logged, so an
	// operator can find what is holding the restore.
	stuckAttempt = 10
)

var errRegistryBehind = errors.New("namespace registry is behind the restore")

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		namespaceRegistry namespace.Registry
		clientBean        client.Bean
		currentCluster    string
		refreshRPS        dynamicconfig.IntPropertyFnWithNamespaceFilter
		concurrency       dynamicconfig.IntPropertyFn
		metricsHandler    metrics.Handler
		logger            log.Logger
		// throttledLogger logs per-execution problems, which can number in the millions.
		throttledLogger log.Logger

		minRetryInterval  time.Duration
		maxRetryInterval  time.Duration
		heartbeatInterval time.Duration
		// How long the refresh waits at start for this host's namespace registry to have the
		// restore, which it normally does within one registry refresh interval.
		registryWait time.Duration
	}

	// refreshState is the refresh's checkpoint, shared with the heartbeat goroutine.
	refreshState struct {
		sync.Mutex
		hb RefreshHeartbeat
	}
)

func NewActivities(
	visibilityManager manager.VisibilityManager,
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	currentCluster string,
	refreshRPS dynamicconfig.IntPropertyFnWithNamespaceFilter,
	concurrency dynamicconfig.IntPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
	throttledLogger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager: visibilityManager,
		namespaceRegistry: namespaceRegistry,
		clientBean:        clientBean,
		currentCluster:    currentCluster,
		refreshRPS:        refreshRPS,
		concurrency:       concurrency,
		metricsHandler:    metricsHandler,
		logger:            logger,
		throttledLogger:   throttledLogger,
		minRetryInterval:  time.Second,
		maxRetryInterval:  time.Minute,
		heartbeatInterval: refreshHeartbeatTimeout / 4,
		registryWait:      30 * time.Second,
	}
}

// RefreshExecutions refreshes every execution of the namespace on every cluster that holds it,
// the execution's active cluster first. It works one page of the listing at a time and moves to the
// next page once every execution on it is done: refreshed, gone, or skipped because retrying can't
// help. Anything else is retried until it succeeds, so an unreachable cluster holds the refresh on
// one page until the cluster is back or removed from the namespace. It stops without retrying if
// the restore is no longer the namespace's running restore, or if this cluster is no longer active
// for the namespace.
func (a *Activities) RefreshExecutions(ctx context.Context, input RestoreJobInput) (RefreshStats, error) {
	state := &refreshState{}
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &state.hb); err != nil {
			a.logger.Warn("Unable to read the restore refresh checkpoint; starting over.",
				tag.WorkflowNamespaceID(input.NamespaceID),
				tag.NewStringTag("restore-id", input.RestoreID),
				tag.Error(err))
			state.hb = RefreshHeartbeat{}
		}
	}

	ns, err := a.awaitRegistry(ctx, input, state)
	if err != nil {
		return RefreshStats{}, err
	}
	ctx = headers.SetCallerInfo(ctx, headers.NewPreemptableCallerInfo(ns.Name().String()))
	if state.hb.RefreshStartTime.IsZero() {
		resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
			NamespaceID: ns.ID(),
			Namespace:   ns.Name(),
			Query:       sadefs.QueryWithAnyNamespaceDivision(""),
		})
		if err != nil {
			return RefreshStats{}, err
		}
		state.hb.TotalEstimate = resp.Count
		state.hb.RefreshStartTime = time.Now().UTC()
	}

	// A refresh retries until it succeeds, so the watcher is what stops it once the restore stops.
	refreshCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	go a.watch(refreshCtx, stop, input, state)

	limiter := quotas.NewDefaultOutgoingRateLimiter(func() float64 { return float64(a.refreshRPS(ns.Name().String())) })
	for !state.listingDone() {
		if err := a.refreshNextPage(refreshCtx, ns, input, state, limiter); err != nil {
			if cause := context.Cause(refreshCtx); cause != nil {
				return RefreshStats{}, cause
			}
			return RefreshStats{}, err
		}
	}

	state.Lock()
	defer state.Unlock()
	return state.hb.RefreshStats, nil
}

// refreshNextPage lists the page at the checkpoint, refreshes it, and moves the checkpoint past it.
func (a *Activities) refreshNextPage(
	ctx context.Context,
	ns *namespace.Namespace,
	input RestoreJobInput,
	state *refreshState,
	limiter quotas.RateLimiter,
) error {
	state.Lock()
	pageToken := state.hb.PageToken
	state.Unlock()
	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   ns.ID(),
		Namespace:     ns.Name(),
		PageSize:      listPageSize,
		NextPageToken: pageToken,
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	})
	if err != nil {
		return err
	}
	skipped, err := a.refreshPage(ctx, input, resp.Executions, limiter)
	if err != nil {
		return err
	}
	state.Lock()
	state.hb.PageToken = resp.NextPageToken
	state.hb.ListingDone = len(resp.NextPageToken) == 0
	state.hb.ProcessedCount += int64(len(resp.Executions))
	state.hb.SkippedCount += skipped
	state.Unlock()
	a.recordHeartbeat(ctx, state)
	return nil
}

// refreshPage refreshes a page of executions concurrently and returns how many were skipped on at
// least one cluster. It returns an error only if ctx is done, in which case the page isn't done.
func (a *Activities) refreshPage(
	ctx context.Context,
	input RestoreJobInput,
	executions []*workflowpb.WorkflowExecutionInfo,
	limiter quotas.RateLimiter,
) (int64, error) {
	archetypeIDs := make([]chasm.ArchetypeID, len(executions))
	for i, info := range executions {
		archetypeID, err := workercommon.ArchetypeIDFromExecutionInfo(info)
		if err != nil {
			return 0, fmt.Errorf("execution %s: %w", info.GetExecution().GetWorkflowId(), err)
		}
		archetypeIDs[i] = archetypeID
	}

	var skipped atomic.Int64
	sem := make(chan struct{}, max(1, a.concurrency()))
	var wg sync.WaitGroup
	for i, info := range executions {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Go(func() {
			defer func() { <-sem }()
			if a.refreshExecution(ctx, input, limiter, info.GetExecution(), archetypeIDs[i]) {
				skipped.Add(1)
			}
		})
	}
	wg.Wait()
	return skipped.Load(), ctx.Err()
}

// refreshExecution refreshes the execution on every cluster that holds it, the active cluster first,
// and reports whether any cluster was skipped. It retries a cluster until the refresh succeeds or
// fails in a way retrying can't fix. It reads the execution's clusters before each attempt, so a
// failover reorders them and removing a cluster from the namespace stops refreshes to it. It returns
// early only if ctx is done.
func (a *Activities) refreshExecution(
	ctx context.Context,
	input RestoreJobInput,
	limiter quotas.RateLimiter,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
) (skipped bool) {
	var done []string
	var cluster string
	var attempt int
	interval := a.minRetryInterval
	for {
		next, err := a.nextCluster(input.NamespaceID, execution.GetWorkflowId(), done)
		if err == nil {
			if next == "" {
				return skipped
			}
			if next != cluster {
				cluster, attempt, interval = next, 0, a.minRetryInterval
			}
			if err = limiter.Wait(ctx); err != nil {
				return skipped
			}
			attempt++
			err = a.refresh(ctx, input, cluster, execution, archetypeID)
			var notFound *serviceerror.NotFound
			switch {
			case err == nil, errors.As(err, &notFound):
				// NotFound: the cluster has no copy, usually because the execution was deleted after it
				// was listed.
				done = append(done, cluster)
				continue
			case isTerminal(err):
				metrics.NamespaceRestoreSkippedCount.With(a.metricsHandler).Record(1, metrics.TargetClusterTag(cluster))
				a.throttledLogger.Error("Namespace restore skipped an execution it can't refresh.",
					executionTags(input, cluster, execution, err)...)
				done = append(done, cluster)
				skipped = true
				continue
			default:
				// Counted, not logged: a cluster that is down fails every execution.
				metrics.NamespaceRestoreRefreshFailureCount.With(a.metricsHandler).Record(1, metrics.TargetClusterTag(cluster))
				if attempt == stuckAttempt {
					a.throttledLogger.Warn("Namespace restore is still retrying an execution.",
						executionTags(input, cluster, execution, err)...)
				}
			}
		}
		if util.InterruptibleSleep(ctx, interval) != nil {
			return skipped
		}
		interval = min(2*interval, a.maxRetryInterval)
	}
}

// nextCluster returns the cluster to refresh the execution on next: its active cluster if not yet
// done, then the rest of the clusters that hold it. It returns "" once every cluster is done.
func (a *Activities) nextCluster(namespaceID string, businessID string, done []string) (string, error) {
	ns, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return "", err
	}
	clusters := ns.ClusterNames(businessID)
	if active := ns.ActiveClusterName(namespace.RoutingKey{ID: businessID}); slices.Contains(clusters, active) && !slices.Contains(done, active) {
		return active, nil
	}
	for _, cluster := range clusters {
		if !slices.Contains(done, cluster) {
			return cluster, nil
		}
	}
	return "", nil
}

// isTerminal reports whether retrying a refresh can't help: the cluster can't read the execution's
// mutable state, or rejects the request.
func isTerminal(err error) bool {
	switch err.(type) {
	case *serviceerror.DataLoss, *serviceerror.InvalidArgument:
		return true
	default:
		return false
	}
}

func executionTags(input RestoreJobInput, cluster string, execution *commonpb.WorkflowExecution, err error) []tag.Tag {
	return []tag.Tag{
		tag.WorkflowNamespaceID(input.NamespaceID),
		tag.WorkflowID(execution.GetWorkflowId()),
		tag.WorkflowRunID(execution.GetRunId()),
		tag.ClusterName(cluster),
		tag.Error(err),
	}
}

func (a *Activities) refresh(
	ctx context.Context,
	input RestoreJobInput,
	cluster string,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
) error {
	adminClient, err := a.clientBean.GetRemoteAdminClient(cluster)
	if err != nil {
		return err
	}
	_, err = adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId:               input.NamespaceID,
		Execution:                 execution,
		ArchetypeId:               archetypeID,
		MinNamespaceConfigVersion: input.RestoreVersion,
	})
	return err
}

// CompleteRestore marks the restore COMPLETED. If this cluster lost the namespace while the refresh
// ran, the refresh may have missed tasks on the new active cluster, so it marks the restore FAILED.
func (a *Activities) CompleteRestore(ctx context.Context, input RestoreJobInput) error {
	// A retry after the restore was marked COMPLETED.
	if ns, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(input.NamespaceID)); err == nil {
		if restore := ns.Info().GetRestoreInfo(); restore.GetRestoreId() == input.RestoreID &&
			restore.GetState() == enumspb.NAMESPACE_RESTORE_STATE_COMPLETED {
			return nil
		}
	}
	if _, err := a.checkRestore(ctx, input); err != nil {
		return err
	}
	err := a.closeRestore(ctx, input, a.currentCluster, enumspb.NAMESPACE_RESTORE_STATE_COMPLETED)
	if failedPrecondition, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
		return temporal.NewNonRetryableApplicationError(failedPrecondition.Message, errTypeRestoreStopped, err)
	}
	return err
}

// awaitRegistry runs checkRestore, waiting up to registryWait for this host's registry to catch up
// with the restore.
func (a *Activities) awaitRegistry(ctx context.Context, input RestoreJobInput, state *refreshState) (*namespace.Namespace, error) {
	deadline := time.Now().Add(a.registryWait)
	for {
		ns, err := a.checkRestore(ctx, input)
		if !errors.Is(err, errRegistryBehind) || !time.Now().Before(deadline) {
			return ns, err
		}
		// Keeps the checkpoint loaded from the previous attempt.
		a.recordHeartbeat(ctx, state)
		if err := util.InterruptibleSleep(ctx, registryPollInterval); err != nil {
			return nil, err
		}
	}
}

// checkRestore reads the namespace from the registry and checks that this restore is still the
// namespace's running restore, and that this cluster is still the namespace's active cluster. If
// the namespace failed over, it marks the restore FAILED.
func (a *Activities) checkRestore(ctx context.Context, input RestoreJobInput) (*namespace.Namespace, error) {
	ns, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(input.NamespaceID))
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			return nil, temporal.NewNonRetryableApplicationError("namespace not found", errTypeRestoreStopped, err)
		}
		return nil, err
	}
	// The job can start before this host's registry has the restore.
	if ns.ConfigVersion() < input.RestoreVersion {
		return nil, fmt.Errorf("%w: config version %d, restore version %d",
			errRegistryBehind, ns.ConfigVersion(), input.RestoreVersion)
	}
	restore := ns.Info().GetRestoreInfo()
	if restore.GetRestoreId() != input.RestoreID {
		return nil, stopped("restore %s was superseded by restore %s", input.RestoreID, restore.GetRestoreId())
	}
	if restore.GetState() != enumspb.NAMESPACE_RESTORE_STATE_RUNNING {
		return nil, stopped("restore %s is %s", input.RestoreID, restore.GetState())
	}
	// The restore record is written only on the namespace's active cluster, so that the job's
	// writes and the lifecycle APIs' are ordered by one database.
	if activeCluster := ns.ActiveClusterName(namespace.RoutingKey{}); activeCluster != a.currentCluster {
		err := a.closeRestore(ctx, input, activeCluster, enumspb.NAMESPACE_RESTORE_STATE_FAILED)
		var failedPrecondition *serviceerror.FailedPrecondition
		if err != nil && !errors.As(err, &failedPrecondition) {
			return nil, err
		}
		return nil, stopped("cluster %s is no longer active for namespace %s", a.currentCluster, ns.Name())
	}
	return ns, nil
}

func (a *Activities) closeRestore(ctx context.Context, input RestoreJobInput, cluster string, state enumspb.NamespaceRestoreState) error {
	adminClient, err := a.clientBean.GetRemoteAdminClient(cluster)
	if err != nil {
		return err
	}
	_, err = adminClient.CloseNamespaceRestore(ctx, &adminservice.CloseNamespaceRestoreRequest{
		NamespaceId: input.NamespaceID,
		RestoreId:   input.RestoreID,
		State:       state,
	})
	return err
}

// watch heartbeats the checkpoint and checks the restore until ctx is done. Once the restore
// stops, it cancels ctx with the reason.
func (a *Activities) watch(ctx context.Context, stop context.CancelCauseFunc, input RestoreJobInput, state *refreshState) {
	ticker := time.NewTicker(a.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		a.recordHeartbeat(ctx, state)
		if _, err := a.checkRestore(ctx, input); err != nil {
			stop(err)
			return
		}
	}
}

func (a *Activities) recordHeartbeat(ctx context.Context, state *refreshState) {
	state.Lock()
	hb := state.hb
	state.Unlock()
	activity.RecordHeartbeat(ctx, hb)
}

func (s *refreshState) listingDone() bool {
	s.Lock()
	defer s.Unlock()
	return s.hb.ListingDone
}

func stopped(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), errTypeRestoreStopped, nil)
}
