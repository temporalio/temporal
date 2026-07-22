package scheduleinvariants

import (
	"context"
	"fmt"
	"iter"
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
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
)

const (
	// heartbeatInterval is how often the background heartbeat goroutine pings the
	// activity. It must be comfortably below the activity's HeartbeatTimeout.
	heartbeatInterval = 10 * time.Second
	// scheduleListPageSize controls pagination of ListChasmExecutions inside
	// ScanOverdueNextActionTime. Each page is also one unit of rate-limited progress.
	scheduleListPageSize = 100
)

// Activities holds shared dependencies for all three schedule-invariants scanner activities.
type Activities struct {
	logger             log.Logger
	metricsHandler     metrics.Handler
	visibilityManager  manager.VisibilityManager
	namespaceRegistry  namespace.Registry
	sdkClientFactory   sdk.ClientFactory
	currentClusterName string
	timeSource         clock.TimeSource

	// opts is the live schedule-invariants scanner config, re-read on each scan pass.
	opts        dynamicconfig.TypedPropertyFn[dynamicconfig.ScheduleInvariantsScannerParams]
	rateLimiter quotas.RateLimiter
}

func NewActivities(
	logger log.Logger,
	metricsHandler metrics.Handler,
	visibilityManager manager.VisibilityManager,
	namespaceRegistry namespace.Registry,
	sdkClientFactory sdk.ClientFactory,
	currentClusterName string,
	timeSource clock.TimeSource,
	opts dynamicconfig.TypedPropertyFn[dynamicconfig.ScheduleInvariantsScannerParams],
) *Activities {
	return &Activities{
		logger:             log.With(logger, tag.Operation(metrics.ScheduleInvariantsScannerScope)),
		metricsHandler:     metricsHandler.WithTags(metrics.OperationTag(metrics.ScheduleInvariantsScannerScope)),
		visibilityManager:  visibilityManager,
		namespaceRegistry:  namespaceRegistry,
		sdkClientFactory:   sdkClientFactory,
		currentClusterName: currentClusterName,
		timeSource:         timeSource,
		opts:               opts,
		rateLimiter:        quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(func() float64 { return opts().VisibilityRPS })),
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
		threshold := a.timeSource.Now().UTC().Add(-a.opts().OverdueNextActionTimeTolerance).Format(time.RFC3339Nano)
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
		// two weeks ago (IdleTime, scaled by the configured buffer multiplier)
		buffer := scheduler.DefaultTweakables.IdleTime * time.Duration(a.opts().StuckOpenIdleTimeBufferMultiplier)
		threshold := a.timeSource.Now().UTC().Add(-buffer).Format(time.RFC3339Nano)

		// select schedules which went idle before two weeks ago
		// and which are not paused and still appear to be running
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
//
// The ScheduleIdleCloseTime IS NULL clause excludes schedules that have legitimately
// run out of actions and are sitting in the idle/retention window before idle-close:
// those are still Running and unpaused with no next action time, but carry a (future)
// ScheduleIdleCloseTime. Without this clause every exhausted-but-retained schedule would
// be a false positive for up to the retention window, and genuinely stuck-open schedules
// (past ScheduleIdleCloseTime) would be double-counted with ScanStuckOpen.
func (a *Activities) ScanUnknownState(ctx context.Context) error {
	return a.runForeverWithInterval(ctx, func(scanCtx context.Context) error {
		query := fmt.Sprintf(
			`%s IS NULL AND %s IS NULL AND TemporalSchedulePaused = false AND ExecutionStatus = "Running"`,
			scheduler.ScheduleNextActionTimeName,
			scheduler.ScheduleIdleCloseTimeName,
		)
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

	ch, timer := a.timeSource.NewTimer(a.opts().ScanInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			if err := scanFn(ctx); err != nil {
				return err
			}
			timer.Reset(a.opts().ScanInterval)
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
	for _, nsName := range a.ListAllNamespaces() {
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

	// maxChecks bounds the number of per-schedule DescribeSchedule calls per namespace
	// per scan pass, so a namespace with a large backlog of overdue schedules can't
	// hammer the frontend. Schedules beyond the cap are left unchecked for this pass.
	maxChecks := a.opts().OverdueNextActionTimeMaxChecksPerNamespace
	for _, nsName := range a.ListAllNamespaces() {
		var nsAnomalies, checked int64
		var scanErr error
		for scheduleID, err := range a.schedulesInNamespace(ctx, nsName, query) {
			if err != nil {
				scanErr = err
				break
			}
			if checked >= int64(maxChecks) {
				a.logger.Warn("overdue scan hit per-namespace check cap; remaining schedules left unchecked this pass",
					tag.WorkflowNamespace(nsName),
					tag.NewInt("cap", maxChecks))
				metrics.ScheduleInvariantsScannerOverdueNextActionTimeCapHitCount.With(
					a.metricsHandler.WithTags(metrics.NamespaceTag(nsName))).Record(1)
				break
			}
			checked++
			if a.scheduleIsExpectedNotToFire(ctx, nsName, scheduleID) {
				continue
			}
			nsAnomalies++
			a.logger.Warn("schedule was expected to fire but doesn't appear to have done so",
				tag.WorkflowNamespace(nsName),
				tag.ScheduleID(scheduleID))
		}
		if scanErr != nil {
			a.recordScanError(nsName, subScanner, scanErr)
			continue
		}
		a.emitCount(metricName, nsName, nsAnomalies)
	}
	return nil
}

// ListAllNamespaces returns the names of every registered namespace in the current cluster,
// from the namespace registry's in-memory snapshot. The snapshot may lag persistence by
// up to the registry's refresh interval, which is acceptable for a best-effort scanner.
//
// Only namespaces in the REGISTERED (active) state are returned; namespaces being
// deprecated, deleted, or in any other state are skipped, since scanning them for
// schedule invariants is not meaningful.
func (a *Activities) ListAllNamespaces() []string {
	var names []string
	for _, ns := range a.namespaceRegistry.GetAllNamespaces() {
		if ns.State() != enumspb.NAMESPACE_STATE_REGISTERED {
			continue
		}
		names = append(names, ns.Name().String())
	}
	return names
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

// schedulesInNamespace returns an iterator over the business IDs of every schedule
// matching query in the given namespace (scoped to the Scheduler archetype), paginating
// ListChasmExecutions transparently and consuming the rate limiter per page. Each step
// yields either a schedule ID (with a nil error) or a terminal error, after which the
// iterator stops; the consumer stops early by breaking out of the range.
func (a *Activities) schedulesInNamespace(ctx context.Context, nsName, query string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		nsID, err := a.namespaceRegistry.GetNamespaceID(namespace.Name(nsName))
		if err != nil {
			yield("", err)
			return
		}

		var pageToken []byte
		for {
			if err := a.rateLimiter.Wait(ctx); err != nil {
				if common.IsContextDeadlineExceededErr(err) {
					err = context.DeadlineExceeded
				}
				yield("", err)
				return
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
				yield("", err)
				return
			}
			for _, exec := range resp.GetExecutions() {
				if !yield(exec.GetBusinessId(), nil) {
					return
				}
			}
			pageToken = resp.GetNextPageToken()
			if len(pageToken) == 0 {
				return
			}
		}
	}
}

// scheduleIsExpectedNotToFire returns true when a schedule flagged as "overdue" in
// visibility is actually in a state that legitimately explains the stale NextActionTime:
// either the schedule is paused, or it's holding under BUFFER_ONE / BUFFER_ALL with a
// prior workflow still running.
//
// This deliberately relies on DescribeSchedule rather than the visibility memo: a stuck
// schedule is exactly the case where the memo's paused flag and running-workflow list go
// stale, so visibility can't be trusted to rule out an anomaly. DescribeSchedule reads
// authoritative state. Callers bound the number of these checks per namespace.
func (a *Activities) scheduleIsExpectedNotToFire(ctx context.Context, nsName, scheduleID string) bool {
	if err := a.rateLimiter.Wait(ctx); err != nil {
		a.logger.Warn("rate-limiter wait failed during DescribeSchedule; counting schedule as anomaly",
			tag.WorkflowNamespace(nsName), tag.ScheduleID(scheduleID), tag.Error(err))
		return false
	}
	desc, err := a.sdkClientFactory.GetSystemClient().WorkflowService().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: scheduleID,
	})
	if err != nil {
		a.logger.Warn("DescribeSchedule failed",
			tag.WorkflowNamespace(nsName), tag.ScheduleID(scheduleID), tag.Error(err))
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
