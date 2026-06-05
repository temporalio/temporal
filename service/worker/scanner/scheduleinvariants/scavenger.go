package scheduleinvariants

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
)

const (
	// scanInterval is how often each scanner activity kicks off a fresh scan pass.
	scanInterval = 15 * time.Minute
	// heartbeatInterval is how often the background heartbeat goroutine pings the
	// activity. It must be comfortably below the activity's HeartbeatTimeout.
	heartbeatInterval = 10 * time.Second
	// scheduleListPageSize controls pagination of ListChasmExecutions inside
	// ScanOverdueNextActionTime. Each page is also one unit of rate-limited progress.
	scheduleListPageSize             = 100
	scheduleIdleTimeBufferMultiplier = 2
	namespaceListPageSize            = 100
	// the point at which the scanner gives up because either there's too many
	// false positives or there's too many actual hits
	overdueScheduleCap = 100
)

// Activities holds shared dependencies for all three schedule-invariants scanner activities.
type Activities struct {
	logger             log.Logger
	metricsHandler     metrics.Handler
	metadataManager    persistence.MetadataManager
	visibilityManager  manager.VisibilityManager
	namespaceRegistry  namespace.Registry
	sdkClientFactory   sdk.ClientFactory
	currentClusterName string
	timeSource         clock.TimeSource

	overdueTolerance dynamicconfig.DurationPropertyFn
	rateLimiter      quotas.RateLimiter
}

func NewActivities(
	logger log.Logger,
	metricsHandler metrics.Handler,
	metadataManager persistence.MetadataManager,
	visibilityManager manager.VisibilityManager,
	namespaceRegistry namespace.Registry,
	sdkClientFactory sdk.ClientFactory,
	currentClusterName string,
	timeSource clock.TimeSource,
	visibilityRPS dynamicconfig.FloatPropertyFn,
	overdueTolerance dynamicconfig.DurationPropertyFn,
) *Activities {
	return &Activities{
		logger:             logger,
		metricsHandler:     metricsHandler.WithTags(metrics.OperationTag(metrics.ScheduleInvariantsScannerScope)),
		metadataManager:    metadataManager,
		visibilityManager:  visibilityManager,
		namespaceRegistry:  namespaceRegistry,
		sdkClientFactory:   sdkClientFactory,
		currentClusterName: currentClusterName,
		timeSource:         timeSource,
		overdueTolerance:   overdueTolerance,
		rateLimiter:        quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(visibilityRPS)),
	}
}

// ScanOverdueNextActionTime is a long-running activity that periodically lists
// schedules whose TemporalScheduleNextActionTime is in the past beyond the configured
// tolerance, calls DescribeSchedule on each candidate, and filters out schedules that
// are paused or legitimately blocked from firing by a BUFFER_ONE / BUFFER_ALL overlap
// policy with a prior workflow still running. Whatever remains is counted as an
// anomaly per namespace and logged for triage.
func (a *Activities) ScanOverdueNextActionTime(ctx context.Context) error {
	return a.runForeverWithInterval(ctx, func(scanCtx context.Context) error {
		threshold := a.timeSource.Now().UTC().Add(-a.overdueTolerance()).Format(time.RFC3339Nano)
		// the query's intention is:
		// show running schedules where ScheduleNextActionTime is in the past (plus a small buffer)
		query := fmt.Sprintf(
			`%s < "%s" AND TemporalSchedulePaused = false AND ExecutionStatus = "Running"`,
			scheduler.ScheduleNextActionTimeName,
			threshold,
		)
		return a.runOverdueScan(scanCtx, query)
	})
}

// ScanStuckOpen is a long-running activity that periodically scans for schedules whose
// ScheduleIdleCloseTime deadline has already passed (beyond the configured buffer).
// ScheduleIdleCloseTime is only emitted while a schedule is open, so an overdue value
// means the idle-close task never fired and the schedule is stuck open. Paused schedules
// are excluded since they are intentionally held open and do not idle-close.
func (a *Activities) ScanStuckOpen(ctx context.Context) error {
	return a.runForeverWithInterval(ctx, func(scanCtx context.Context) error {
		threshold := a.timeSource.Now().UTC().Add(-(scheduler.DefaultTweakables.IdleTime * scheduleIdleTimeBufferMultiplier)).Format(time.RFC3339Nano)
		query := fmt.Sprintf(
			`%s < "%s" AND TemporalSchedulePaused = false AND ExecutionStatus = "Running"`,
			scheduler.ScheduleIdleCloseTimeName,
			threshold,
		)
		return a.runScan(scanCtx, "stuck_open", query, metrics.ScheduleInvariantsScannerStuckOpenCount.Name())
	})
}

// ScanUnknownState is a long-running activity that periodically scans for running,
// unpaused schedules with no TemporalScheduleNextActionTime set.
func (a *Activities) ScanUnknownState(ctx context.Context) error {
	return a.runForeverWithInterval(ctx, func(scanCtx context.Context) error {
		query := fmt.Sprintf(`%s IS NULL AND TemporalSchedulePaused = false AND ExecutionStatus = "Running"`, scheduler.ScheduleNextActionTimeName)
		a.logger.Info("schedule-invariants: starting unknown_state scan",
			tag.NewStringTag("query", query))
		return a.runScan(scanCtx, "unknown_state", query, metrics.ScheduleInvariantsScannerUnknownStateCount.Name())
	})
}

// runForeverWithInterval runs scanFn immediately, then again on every tick of scanInterval, until
// the activity context is canceled or scanFn returns an error. A background goroutine
// emits heartbeats so the parent activity stays alive between scans. All timing flows
// through the injected TimeSource so tests can drive the loop deterministically.
func (a *Activities) runForeverWithInterval(ctx context.Context, scanFn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go a.heartbeatLoop(ctx)

	if err := scanFn(ctx); err != nil {
		return err
	}

	ch, timer := a.timeSource.NewTimer(scanInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			if err := scanFn(ctx); err != nil {
				return err
			}
			timer.Reset(scanInterval)
		}
	}
}

func (a *Activities) heartbeatLoop(ctx context.Context) {
	ch, timer := a.timeSource.NewTimer(heartbeatInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			activity.RecordHeartbeat(ctx)
			timer.Reset(heartbeatInterval)
		}
	}
}

// runScan ties the per-scanner pieces together: list namespaces, fan out a single
// visibility query per namespace, and emit per-namespace metrics. Cluster-wide
// totals are computed by the metrics backend by summing across the namespace tag.
// Used by the count-only scanners (StuckOpen, UnknownState).
func (a *Activities) runScan(ctx context.Context, subScanner, query, metricName string) error {
	namespaces, err := a.ListAllNamespaces(ctx)
	if err != nil {
		return err
	}

	for _, nsName := range namespaces {
		err := a.forEachNamespace(ctx, nsName, query, func(count int64) {
			a.emitCount(metricName, nsName, count)
		})
		if err != nil {
			a.recordScanError(nsName, subScanner, err)
			continue
		}
	}
	return nil
}

// runOverdueScan lists individual matching schedules per namespace, calls
// DescribeSchedule on each, and filters out schedules that are paused or
// expected to not be able to fire (BUFFER_ONE / BUFFER_ALL waiting on a prior
// running workflow). What remains is counted as an anomaly.
func (a *Activities) runOverdueScan(ctx context.Context, query string) error {
	const subScanner = "overdue_next_action_time"
	metricName := metrics.ScheduleInvariantsScannerOverdueNextActionTimeCount.Name()

	namespaces, err := a.ListAllNamespaces(ctx)
	if err != nil {
		return err
	}

	for _, nsName := range namespaces {
		var nsAnomalies int64
		err := a.forEachScheduleInNamespace(ctx, nsName, query, func(scheduleID string) {
			if a.scheduleIsExpectedNotToFire(ctx, nsName, scheduleID) {
				return
			}
			nsAnomalies++
			a.logger.Warn("schedule-invariants - the schedule was expected to fire but doesn't appear to have done so",
				tag.WorkflowNamespace(nsName),
				tag.ScheduleID(scheduleID))
		})
		if err != nil {
			a.recordScanError(nsName, subScanner, err)
			continue
		}
		a.emitCount(metricName, nsName, nsAnomalies)
	}
	return nil
}

// ListAllNamespaces returns the names of every namespace that exists in the cluster
// metadata store and is active in the current cluster. Pagination is handled internally.
func (a *Activities) ListAllNamespaces(ctx context.Context) ([]string, error) {
	var names []string
	var pageToken []byte
	for {
		nsResp, err := a.metadataManager.ListNamespaces(ctx, &persistence.ListNamespacesRequest{
			PageSize:       namespaceListPageSize,
			NextPageToken:  pageToken,
			IncludeDeleted: false,
		})
		if err != nil {
			return nil, err
		}
		for _, ns := range nsResp.Namespaces {
			nsEntry, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(ns.Namespace.Info.Id))
			if err != nil {
				a.logger.Warn("namespace lookup failed, skipping",
					tag.WorkflowNamespaceID(ns.Namespace.Info.Id), tag.Error(err))
				continue
			}
			// Only the active cluster for this namespace should report; otherwise
			// global namespaces would be reported once per cluster.
			if !nsEntry.ActiveInCluster(a.currentClusterName) {
				continue
			}
			names = append(names, nsEntry.Name().String())
		}
		pageToken = nsResp.NextPageToken
		if len(pageToken) == 0 {
			break
		}
	}
	return names, nil
}

// forEachNamespace runs `query` against one namespace (scoped to the Scheduler archetype)
// and passes the resulting count to resultsFilter. Rate-limited by the shared limiter.
// Intended to be called once per namespace returned by ListAllNamespaces.
//
// Note: the parameter is named `nsName` rather than `namespace` to avoid shadowing the
// imported `namespace` package used internally.
func (a *Activities) forEachNamespace(
	ctx context.Context,
	nsName string,
	query string,
	resultsFilter func(count int64),
) error {
	if err := a.rateLimiter.Wait(ctx); err != nil {
		if common.IsContextDeadlineExceededErr(err) {
			return context.DeadlineExceeded
		}
		return err
	}
	nsID, err := a.namespaceRegistry.GetNamespaceID(namespace.Name(nsName))
	if err != nil {
		return err
	}
	resp, err := a.visibilityManager.CountChasmExecutions(ctx, &visibilityservice.CountChasmExecutionsRequest{
		ArchetypeId: chasm.SchedulerArchetypeID,
		NamespaceId: nsID.String(),
		Namespace:   nsName,
		Query:       query,
	})
	if err != nil {
		return err
	}
	resultsFilter(resp.GetCount())
	return nil
}

// forEachScheduleInNamespace paginates ListChasmExecutions for the given namespace
// (scoped to the Scheduler archetype) and invokes visit for every schedule entity in
// the result. The rate limiter is consumed per page.
func (a *Activities) forEachScheduleInNamespace(
	ctx context.Context,
	nsName string,
	query string,
	visit func(scheduleID string),
) error {
	nsID, err := a.namespaceRegistry.GetNamespaceID(namespace.Name(nsName))
	if err != nil {
		return err
	}

	var pageToken []byte
	for {
		if err := a.rateLimiter.Wait(ctx); err != nil {
			if common.IsContextDeadlineExceededErr(err) {
				return context.DeadlineExceeded
			}
			return err
		}
		resp, err := a.visibilityManager.ListChasmExecutions(ctx, &visibilityservice.ListChasmExecutionsRequest{
			ArchetypeId:   chasm.SchedulerArchetypeID,
			NamespaceId:   nsID.String(),
			Namespace:     nsName,
			Query:         query,
			PageSize:      scheduleListPageSize,
			NextPageToken: pageToken,
		})
		if err != nil {
			return err
		}
		for _, exec := range resp.GetExecutions() {
			visit(exec.GetBusinessId())
		}
		pageToken = resp.GetNextPageToken()
		if len(pageToken) == 0 {
			return nil
		}
	}
}

// scheduleIsExpectedNotToFire returns true when a schedule flagged as "overdue" in
// visibility is actually in a state that legitimately explains the stale NextActionTime:
// either the schedule is paused, or it's holding under BUFFER_ONE / BUFFER_ALL with a
// prior workflow still running.
func (a *Activities) scheduleIsExpectedNotToFire(ctx context.Context, nsName, scheduleID string) bool {
	if err := a.rateLimiter.Wait(ctx); err != nil {
		a.logger.Warn("rate-limiter wait failed during DescribeSchedule; counting schedule as anomaly",
			tag.WorkflowNamespace(nsName), tag.NewStringTag("schedule_id", scheduleID), tag.Error(err))
		return false
	}
	desc, err := a.sdkClientFactory.GetSystemClient().WorkflowService().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: scheduleID,
	})
	if err != nil {
		a.logger.Warn("DescribeSchedule failed",
			tag.WorkflowNamespace(nsName), tag.NewStringTag("schedule_id", scheduleID), tag.Error(err))
		return false
	}

	if desc.GetSchedule().GetState().GetPaused() {
		return true
	}
	overlap := desc.GetSchedule().GetPolicies().GetOverlapPolicy()
	if (overlap == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || overlap == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL) &&
		len(desc.GetInfo().GetRunningWorkflows()) > 0 {
		return true
	}
	return false
}

func (a *Activities) emitCount(metricName, namespaceTagValue string, count int64) {
	if count <= 0 {
		return
	}
	a.metricsHandler.
		WithTags(metrics.NamespaceTag(namespaceTagValue)).
		Counter(metricName).
		Record(count)
}

func (a *Activities) recordScanError(nsName, subScanner string, err error) {
	a.logger.Warn("schedule-invariants scan failed for namespace",
		tag.WorkflowNamespace(nsName),
		tag.NewStringTag("sub_scanner", subScanner),
		tag.Error(err))
	metrics.ScheduleInvariantsScannerErrorCount.With(a.metricsHandler.WithTags(
		metrics.NamespaceTag(nsName),
		metrics.StringTag("sub_scanner", subScanner),
	)).Record(1)
}
