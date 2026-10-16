package wci

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

type SnapshotTaskHandlerOptions struct {
	fx.In

	FrontendClient workflowservice.WorkflowServiceClient
	Logger         log.Logger
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
func fetchSnapshot(ctx context.Context, client workflowservice.WorkflowServiceClient, v versionRef) (*scalingalgorithm.ScalingMetricsSnapshot, error) {
	resp, err := client.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace:            v.namespace,
		DeploymentVersion:    &deploymentpb.WorkerDeploymentVersion{DeploymentName: v.deploymentName, BuildId: v.buildID},
		ReportTaskQueueStats: true,
	})
	if err != nil {
		return nil, err
	}
	return snapshotFromDescribe(resp), nil
}

// FetchSnapshotTaskHandler polls metrics, then runs ProcessMetricsPoll and re-arms the timer.
type FetchSnapshotTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.FetchSnapshotTask]
	frontendClient workflowservice.WorkflowServiceClient
	logger         log.Logger
}

func NewFetchSnapshotTaskHandler(opts SnapshotTaskHandlerOptions) *FetchSnapshotTaskHandler {
	return &FetchSnapshotTaskHandler{frontendClient: opts.FrontendClient, logger: opts.Logger}
}

func (h *FetchSnapshotTaskHandler) Validate(
	_ chasm.Context,
	w *WorkerControllerInstance,
	_ chasm.TaskInvocation,
	task *wcipb.FetchSnapshotTask,
) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_RUNNING && proto.Equal(task.GetPollTime(), w.NextPollTime), nil
}

func (h *FetchSnapshotTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *wcipb.FetchSnapshotTask,
) error {
	v, err := readVersion(ctx, ref)
	if err != nil {
		return err
	}
	snapshot, err := fetchSnapshot(ctx, h.frontendClient, v)
	if err != nil {
		// As in the workflow: skip this poll and retry at the max interval.
		h.logger.Warn("Failed to fetch WCI metrics snapshot", tag.Error(err))
		snapshot = nil
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		return struct{}{}, w.ProcessMetricsPoll(mctx, snapshot)
	}, nil)
	return err
}

// DeferredDecisionTaskHandler fetches a snapshot, then runs ProcessDeferredScalingDecision.
type DeferredDecisionTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.DeferredDecisionTask]
	frontendClient workflowservice.WorkflowServiceClient
	logger         log.Logger
}

func NewDeferredDecisionTaskHandler(opts SnapshotTaskHandlerOptions) *DeferredDecisionTaskHandler {
	return &DeferredDecisionTaskHandler{frontendClient: opts.FrontendClient, logger: opts.Logger}
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
	snapshot, err := fetchSnapshot(ctx, h.frontendClient, v)
	if err != nil {
		// As in the workflow: drop the decision; the next metrics poll covers it.
		h.logger.Warn("Failed to fetch snapshot for deferred scaling decision", tag.Error(err))
		return nil
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		return struct{}{}, w.ProcessDeferredScalingDecision(mctx, task.GetScalingGroupId(), task.GetTaskAdd(), snapshot)
	}, nil)
	return err
}
