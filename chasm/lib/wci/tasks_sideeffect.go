package wci

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	wciconfig "go.temporal.io/auto-scaled-workers/wci/client"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

type SnapshotTaskHandlerOptions struct {
	fx.In

	FrontendClient workflowservice.WorkflowServiceClient
	Logger         log.Logger
	MetricsHandler metrics.Handler
	StaticConfig   *wciconfig.StaticConfig `optional:"true"`
	DynamicConfig  *dynamicconfig.Collection
}

type versionRef struct {
	namespace      string
	deploymentName string
	buildID        string
}

func readVersion(ctx context.Context, ref chasm.ComponentRef) (versionRef, error) {
	return chasm.ReadComponent(ctx, ref, func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (versionRef, error) {
		return versionRef{
			namespace:      cctx.NamespaceEntry().Name().String(),
			deploymentName: w.DeploymentName,
			buildID:        w.BuildId,
		}, nil
	}, nil)
}

// fetchSnapshot is the only I/O on the decision path; it runs without the execution lock.
func fetchSnapshot(ctx context.Context, client workflowservice.WorkflowServiceClient, v versionRef) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	return client.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace:            v.namespace,
		DeploymentVersion:    &deploymentpb.WorkerDeploymentVersion{DeploymentName: v.deploymentName, BuildId: v.buildID},
		ReportTaskQueueStats: true,
	})
}

// MetricsPollTimerTaskHandler fires at next_poll_time, polls metrics, then runs ProcessMetricsPoll, which re-arms it.
// A side-effect timer, so a standby keeps it for ChasmStandbyTaskDiscardDelay rather than the shorter timer delay.
type MetricsPollTimerTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.MetricsPollTimerTask]
	frontendClient workflowservice.WorkflowServiceClient
	logger         log.Logger
	hostRegion     dynamicconfig.StringPropertyFn
}

func NewMetricsPollTimerTaskHandler(opts SnapshotTaskHandlerOptions) *MetricsPollTimerTaskHandler {
	return &MetricsPollTimerTaskHandler{frontendClient: opts.FrontendClient, logger: opts.Logger, hostRegion: hostRegionFn(opts.StaticConfig, opts.DynamicConfig)}
}

func (h *MetricsPollTimerTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.MetricsPollTimerTask,
) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_RUNNING && proto.Equal(task.GetPollTime(), w.NextPollTime), nil
}

func (h *MetricsPollTimerTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *wcipb.MetricsPollTimerTask,
) error {
	v, err := readVersion(ctx, ref)
	if err != nil {
		return err
	}
	var snapshot *scalingalgorithm.ScalingMetricsSnapshot
	var observed []*wcipb.ObservedTaskQueue
	resp, err := fetchSnapshot(ctx, h.frontendClient, v)
	if err != nil {
		// As in the workflow: skip this poll and retry at the max interval.
		h.logger.Warn("Failed to fetch WCI metrics snapshot", tag.Error(err))
	} else {
		snapshot = snapshotFromDescribe(resp)
		observed = observedTaskQueuesFromDescribe(resp)
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		// The poll was re-armed while this one fetched; the newer timer applies instead.
		if w.Status != wcipb.WCI_STATUS_RUNNING || !proto.Equal(task.GetPollTime(), w.NextPollTime) {
			return struct{}{}, nil
		}
		w.recordHostRegion(h.hostRegion())
		h := metricsFor(mctx, w, "")
		if snapshot == nil {
			recordOperation(h, wcimetrics.OperationTypePullStats, wcimetrics.ErrorTypeDescribeWorkerDeploymentVersionFailed)
		} else {
			recordOperation(h, wcimetrics.OperationTypePullStats, wcimetrics.ErrorTypeNone)
			recordBacklog(h, snapshot)
		}
		return struct{}{}, w.ProcessMetricsPoll(mctx, snapshot, observed)
	}, nil)
	return err
}

// DeferredDecisionTaskHandler fetches a snapshot, then runs ProcessDeferredScalingDecision.
type DeferredDecisionTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.DeferredDecisionTask]
	frontendClient workflowservice.WorkflowServiceClient
	logger         log.Logger
	metricsHandler metrics.Handler
	hostRegion     dynamicconfig.StringPropertyFn
}

func NewDeferredDecisionTaskHandler(opts SnapshotTaskHandlerOptions) *DeferredDecisionTaskHandler {
	return &DeferredDecisionTaskHandler{frontendClient: opts.FrontendClient, logger: opts.Logger, metricsHandler: opts.MetricsHandler, hostRegion: hostRegionFn(opts.StaticConfig, opts.DynamicConfig)}
}

func (h *DeferredDecisionTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.DeferredDecisionTask,
) (bool, error) {
	_, ok := w.ScalingGroupSpecs[task.GetScalingGroupId()]
	return w.Status == wcipb.WCI_STATUS_RUNNING && ok, nil
}

func (h *DeferredDecisionTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *wcipb.DeferredDecisionTask,
) error {
	v, err := readVersion(ctx, ref)
	if err != nil {
		return err
	}
	resp, err := fetchSnapshot(ctx, h.frontendClient, v)
	if err != nil {
		// As in the workflow: drop the decision; the next metrics poll covers it.
		h.logger.Warn("Failed to fetch snapshot for deferred scaling decision", tag.Error(err))
		wcimetrics.DeferredScalingDecisionMetricsPullFailedCount.With(
			wciMetrics(h.metricsHandler.WithTags(metrics.NamespaceTag(v.namespace)), v.deploymentName, v.buildID, ""),
		).Record(1)
		return nil
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		w.recordHostRegion(h.hostRegion())
		provider := w.ScalingGroupSpecs[task.GetScalingGroupId()].GetCompute().GetProviderType()
		recordOperation(metricsFor(mctx, w, provider), wcimetrics.OperationTypeDeferredScalingDecision, wcimetrics.ErrorTypeNone)
		return struct{}{}, w.ProcessDeferredScalingDecision(mctx, task.GetScalingGroupId(), task.GetTaskAdd(), task.GetEffectiveTaskTypes(), snapshotFromDescribe(resp))
	}, nil)
	return err
}
