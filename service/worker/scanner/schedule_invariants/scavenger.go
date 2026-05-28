package schedule_invariants

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
)

// totalNamespaceTag is the value used on the cluster-wide aggregate metric emission.
const totalNamespaceTag = "__total__"

// Activities holds shared dependencies for all three schedule-invariants scanner activities.
type Activities struct {
	logger             log.Logger
	metricsHandler     metrics.Handler
	metadataManager    persistence.MetadataManager
	visibilityManager  manager.VisibilityManager
	namespaceRegistry  namespace.Registry
	currentClusterName string

	overdueTolerance      dynamicconfig.DurationPropertyFn
	stuckOpenBuffer       dynamicconfig.DurationPropertyFn
	namespaceListPageSize dynamicconfig.IntPropertyFn
	visibilityRPS         dynamicconfig.FloatPropertyFn
}

func NewActivities(
	logger log.Logger,
	metricsHandler metrics.Handler,
	metadataManager persistence.MetadataManager,
	visibilityManager manager.VisibilityManager,
	namespaceRegistry namespace.Registry,
	currentClusterName string,
	overdueTolerance dynamicconfig.DurationPropertyFn,
	stuckOpenBuffer dynamicconfig.DurationPropertyFn,
	namespaceListPageSize dynamicconfig.IntPropertyFn,
	visibilityRPS dynamicconfig.FloatPropertyFn,
) *Activities {
	return &Activities{
		logger:                logger,
		metricsHandler:        metricsHandler.WithTags(metrics.OperationTag(metrics.ScheduleInvariantsScannerScope)),
		metadataManager:       metadataManager,
		visibilityManager:     visibilityManager,
		namespaceRegistry:     namespaceRegistry,
		currentClusterName:    currentClusterName,
		overdueTolerance:      overdueTolerance,
		stuckOpenBuffer:       stuckOpenBuffer,
		namespaceListPageSize: namespaceListPageSize,
		visibilityRPS:         visibilityRPS,
	}
}

type heartbeatDetails struct {
	NamespaceNextPageToken []byte
}

// ScanOverdueNextActionTime flags schedules whose TemporalScheduleNextActionTime is in the
// past beyond the configured tolerance. Schedules with BUFFER_ONE / BUFFER_ALL overlap
// policy whose prior workflow is still running may show up as false positives; the
// tolerance buffer is the primary defense against that until per-candidate DescribeSchedule
// filtering or a new overlap-policy search attribute is added.
func (a *Activities) ScanOverdueNextActionTime(ctx context.Context) error {
	threshold := time.Now().Add(-a.overdueTolerance()).UTC().Format(time.RFC3339Nano)
	query := fmt.Sprintf(
		`TemporalScheduleNextActionTime < "%s" AND TemporalSchedulePaused = false AND ExecutionStatus = "Running"`,
		threshold,
	)
	return a.scan(ctx, "overdue_next_action_time", query, metrics.ScheduleInvariantsScannerOverdueNextActionTimeCount.Name())
}

// ScanStuckOpen flags schedules with ExecutionStatus=Completed and CloseTime older than
// the configured buffer.
func (a *Activities) ScanStuckOpen(ctx context.Context) error {
	threshold := time.Now().Add(-a.stuckOpenBuffer()).UTC().Format(time.RFC3339Nano)
	query := fmt.Sprintf(`ExecutionStatus = "Completed" AND CloseTime < "%s"`, threshold)
	return a.scan(ctx, "stuck_open", query, metrics.ScheduleInvariantsScannerStuckOpenCount.Name())
}

// ScanUnknownState flags running, unpaused schedules that have no
// TemporalScheduleNextActionTime set.
func (a *Activities) ScanUnknownState(ctx context.Context) error {
	query := `TemporalScheduleNextActionTime IS NULL AND TemporalSchedulePaused = false AND ExecutionStatus = "Running"`
	return a.scan(ctx, "unknown_state", query, metrics.ScheduleInvariantsScannerUnknownStateCount.Name())
}

// scan walks every active namespace in this cluster, issues one CountChasmExecutions
// per namespace scoped to the Scheduler archetype, and emits per-namespace + cluster
// aggregate counter metrics. The visibility call is rate-limited.
func (a *Activities) scan(ctx context.Context, subScanner, query, metricName string) error {
	var heartbeat heartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &heartbeat); err != nil {
			return temporal.NewNonRetryableApplicationError(
				"failed to load previous heartbeat details", "TypeError", err)
		}
	}

	rateLimiter := quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(a.visibilityRPS))
	pageSize := a.namespaceListPageSize()
	var total int64

	for {
		nsResp, err := a.metadataManager.ListNamespaces(ctx, &persistence.ListNamespacesRequest{
			PageSize:       pageSize,
			NextPageToken:  heartbeat.NamespaceNextPageToken,
			IncludeDeleted: false,
		})
		if err != nil {
			return err
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

			count, err := a.countWithLimit(ctx, rateLimiter, nsEntry, query)
			if err != nil {
				a.recordScanError(nsEntry, subScanner, err)
				continue
			}
			a.emitCount(metricName, nsEntry.Name().String(), count)
			total += count

			activity.RecordHeartbeat(ctx, heartbeat)
		}

		heartbeat.NamespaceNextPageToken = nsResp.NextPageToken
		if len(heartbeat.NamespaceNextPageToken) == 0 {
			break
		}
		activity.RecordHeartbeat(ctx, heartbeat)
	}

	a.emitCount(metricName, totalNamespaceTag, total)
	return nil
}

func (a *Activities) countWithLimit(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	ns *namespace.Namespace,
	query string,
) (int64, error) {
	if err := rateLimiter.Wait(ctx); err != nil {
		if common.IsContextDeadlineExceededErr(err) {
			return 0, context.DeadlineExceeded
		}
		return 0, err
	}
	resp, err := a.visibilityManager.CountChasmExecutions(ctx, &visibilityservice.CountChasmExecutionsRequest{
		ArchetypeId: chasm.SchedulerArchetypeID,
		NamespaceId: ns.ID().String(),
		Namespace:   ns.Name().String(),
		Query:       query,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
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

func (a *Activities) recordScanError(ns *namespace.Namespace, subScanner string, err error) {
	a.logger.Warn("schedule-invariants scan failed for namespace",
		tag.WorkflowNamespace(ns.Name().String()),
		tag.NewStringTag("sub_scanner", subScanner),
		tag.Error(err))
	metrics.ScheduleInvariantsScannerErrorCount.With(a.metricsHandler.WithTags(
		metrics.NamespaceTag(ns.Name().String()),
		metrics.StringTag("sub_scanner", subScanner),
	)).Record(1)
}
