package wci

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/chasm/lib/wci/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Same cadence as the workflow (ASW wci/workflow).
const (
	minPollInterval    = 30 * time.Second
	maxPollInterval    = 5 * time.Minute
	validationInterval = 6 * time.Hour
)

// spec returns the root spec as the iface type the scaling algorithms use.
func (w *WorkerControllerInstance) spec() *iface.WorkerControllerInstanceSpec {
	spec := &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: make(map[string]iface.ScalingGroupSpec, len(w.ScalingGroupSpecs))}
	for id, s := range w.ScalingGroupSpecs {
		spec.ScalingGroupSpecs[id] = migration.SpecToWorkflow(s)
	}
	return spec
}

// algorithmContext gives the algorithms the CHASM logger and clock.
func (w *WorkerControllerInstance) algorithmContext(ctx chasm.Context) context.Context {
	algoCtx := scalingalgorithm.WithLogger(context.Background(), log.NewSdkLogger(ctx.Logger()))
	return scalingalgorithm.WithClock(algoCtx, func() time.Time { return ctx.Now(w) })
}

func algorithmFor(ctx context.Context, spec iface.ScalingGroupSpec) (scalingalgorithm.ScalingAlgorithm, iface.ScalingAlgorithmConfig, error) {
	var algo scalingalgorithm.ScalingAlgorithm
	var err error
	if spec.Scaling == nil {
		algo, err = scalingalgorithm.GetDefaultScalingAlgorithmForComputeProvider(ctx, spec.Compute.ProviderType)
	} else {
		algo, err = scalingalgorithm.GetScalingAlgorithmWithoutValidation(ctx, spec.Scaling.ScalingAlgorithm)
	}
	if err != nil {
		return nil, nil, err
	}
	if algo == nil {
		return nil, nil, fmt.Errorf("no scaling algorithm for group (algorithm %v, provider %q)", spec.Scaling, spec.Compute.ProviderType)
	}
	var config iface.ScalingAlgorithmConfig
	if spec.Scaling != nil {
		config = map[string]any{}
		if err := sdk.PreferProtoDataConverter.FromPayload(spec.Scaling.Config, &config); err != nil {
			return nil, nil, fmt.Errorf("invalid scaling config: %w", err)
		}
	}
	return algo, config, nil
}

func (g *ScalingGroup) algorithmStatus() (iface.ScalingAlgorithmStatus, error) {
	if g.AlgorithmStatus == nil {
		return nil, nil
	}
	var status iface.ScalingAlgorithmStatus
	if err := sdk.PreferProtoDataConverter.FromPayload(g.AlgorithmStatus, &status); err != nil {
		return nil, err
	}
	return status, nil
}

func (g *ScalingGroup) setAlgorithmStatus(status iface.ScalingAlgorithmStatus) error {
	payload, err := sdk.PreferProtoDataConverter.ToPayload(status)
	if err != nil {
		return err
	}
	g.AlgorithmStatus = payload
	return nil
}

// ProcessTaskAdd runs the matching group's algorithm on a task-add, inline in the RecordTaskAdd transaction.
func (w *WorkerControllerInstance) ProcessTaskAdd(ctx chasm.MutableContext, event *wcipb.TaskAddEvent) error {
	spec := w.spec()
	request := *migration.TaskAddToWorkflow(event)
	for id, groupSpec := range spec.ScalingGroupSpecs {
		if !slices.Contains(spec.EffectiveTaskTypesForGroup(id), request.TaskQueueType) {
			continue
		}
		group, ok := w.ScalingGroups[id].TryGet(ctx)
		if !ok {
			return nil
		}
		algoCtx := w.algorithmContext(ctx)
		algo, config, err := algorithmFor(algoCtx, groupSpec)
		if err != nil {
			ctx.Logger().Warn("Scaling algorithm unavailable for task add", tag.Error(err))
			return nil
		}
		status, err := group.algorithmStatus()
		if err != nil {
			return err
		}
		resp, err := algo.ProcessTaskAdd(algoCtx, config, status, request)
		if err != nil || resp == nil {
			ctx.Logger().Warn("Scaling algorithm failed on task add", tag.Error(err))
			return nil
		}
		// Status is written with the actions it emits: it is intent, not confirmation.
		if err := group.setAlgorithmStatus(resp.Status); err != nil {
			return err
		}
		return w.applyActions(ctx, id, groupSpec, group, resp.Actions, event)
	}
	return nil
}

// ProcessMetricsPoll runs every group's algorithm on a snapshot and re-arms the poll timer at
// the earliest requested time. A nil snapshot (fetch failed) only re-arms at the max interval.
func (w *WorkerControllerInstance) ProcessMetricsPoll(ctx chasm.MutableContext, snapshot *scalingalgorithm.ScalingMetricsSnapshot) error {
	nextPoll := maxPollInterval
	if snapshot != nil {
		spec := w.spec()
		algoCtx := w.algorithmContext(ctx)
		for id, groupSpec := range spec.ScalingGroupSpecs {
			group, ok := w.ScalingGroups[id].TryGet(ctx)
			if !ok {
				continue
			}
			algo, config, err := algorithmFor(algoCtx, groupSpec)
			if err != nil {
				ctx.Logger().Warn("Scaling algorithm unavailable for metrics poll", tag.Error(err))
				continue
			}
			status, err := group.algorithmStatus()
			if err != nil {
				return err
			}
			resp, err := algo.ProcessMetricsPoll(algoCtx, config, status, *filterSnapshot(snapshot, spec.EffectiveTaskTypesForGroup(id)))
			if err != nil || resp == nil {
				ctx.Logger().Warn("Scaling algorithm failed on metrics poll", tag.Error(err))
				continue
			}
			if err := group.setAlgorithmStatus(resp.Status); err != nil {
				return err
			}
			if err := w.applyActions(ctx, id, groupSpec, group, withoutDeferred(ctx, resp.Actions), nil); err != nil {
				return err
			}
			if resp.NextPoll != nil {
				nextPoll = max(minPollInterval, min(nextPoll, *resp.NextPoll))
			}
		}
	}
	w.armPollTimer(ctx, ctx.Now(w).Add(nextPoll))
	return nil
}

// ProcessDeferredScalingDecision completes a deferred task-add decision with a pre-fetched snapshot.
func (w *WorkerControllerInstance) ProcessDeferredScalingDecision(
	ctx chasm.MutableContext,
	groupID string,
	event *wcipb.TaskAddEvent,
	snapshot *scalingalgorithm.ScalingMetricsSnapshot,
) error {
	spec := w.spec()
	groupSpec, ok := spec.ScalingGroupSpecs[groupID]
	if !ok {
		return nil
	}
	group, ok := w.ScalingGroups[groupID].TryGet(ctx)
	if !ok {
		return nil
	}
	taskTypes := spec.EffectiveTaskTypesForGroup(groupID)
	request := *migration.TaskAddToWorkflow(event)
	if !slices.Contains(taskTypes, request.TaskQueueType) {
		return nil
	}
	algoCtx := w.algorithmContext(ctx)
	algo, config, err := algorithmFor(algoCtx, groupSpec)
	if err != nil {
		ctx.Logger().Warn("Scaling algorithm unavailable for deferred decision", tag.Error(err))
		return nil
	}
	status, err := group.algorithmStatus()
	if err != nil {
		return err
	}
	// The snapshot was fetched before taking the lock; the getter never does I/O here.
	getSnapshot := func() (*scalingalgorithm.ScalingMetricsSnapshot, error) {
		return filterSnapshot(snapshot, taskTypes), nil
	}
	resp, err := algo.ProcessDeferredScalingDecision(algoCtx, config, status, request, getSnapshot)
	if err != nil || resp == nil {
		ctx.Logger().Warn("Scaling algorithm failed on deferred decision", tag.Error(err))
		return nil
	}
	if err := group.setAlgorithmStatus(resp.Status); err != nil {
		return err
	}
	return w.applyActions(ctx, groupID, groupSpec, group, withoutDeferred(ctx, resp.Actions), nil)
}

// applyActions turns algorithm actions into scaling-group transitions and tasks. Deferral needs the
// originating task-add (event); only ProcessTaskAdd supplies one.
func (w *WorkerControllerInstance) applyActions(
	ctx chasm.MutableContext,
	groupID string,
	groupSpec iface.ScalingGroupSpec,
	group *ScalingGroup,
	actions []scalingalgorithm.ScalingAction,
	event *wcipb.TaskAddEvent,
) error {
	providerType := string(groupSpec.Compute.ProviderType)
	for _, action := range actions {
		switch action.Action {
		case scalingalgorithm.ActionTypeUpdateWorkerSetSize:
			if action.Count == nil {
				ctx.Logger().Warn("UpdateWorkerSetSize without a count; dropping")
				continue
			}
			if err := group.Decide(ctx, *action.Count, providerType); err != nil {
				return err
			}
		case scalingalgorithm.ActionTypeDeferredScalingDecision:
			if event == nil {
				ctx.Logger().Warn("Deferred decision without a source task-add; dropping")
				continue
			}
			ctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.DeferredDecisionTask{ScalingGroupId: groupID, TaskAdd: event})
		case scalingalgorithm.ActionTypeInvokeWorker:
			w.requestInvoke(ctx, group, providerType)
		default:
			ctx.Logger().Warn("Unknown scaling action; dropping", tag.NewStringTag("action", string(action.Action)))
		}
	}
	return nil
}

// withoutDeferred drops deferrals from paths that cannot chain into more deferred work.
func withoutDeferred(ctx chasm.Context, actions []scalingalgorithm.ScalingAction) []scalingalgorithm.ScalingAction {
	return slices.DeleteFunc(slices.Clone(actions), func(a scalingalgorithm.ScalingAction) bool {
		if a.Action == scalingalgorithm.ActionTypeDeferredScalingDecision {
			ctx.Logger().Warn("Nested deferred scaling decision; dropping")
			return true
		}
		return false
	})
}

// armPollTimer replaces the poll timer; the new next_poll_time invalidates any older timer or fetch.
func (w *WorkerControllerInstance) armPollTimer(ctx chasm.MutableContext, at time.Time) {
	w.NextPollTime = timestamppb.New(at)
	ctx.AddTask(w, chasm.TaskAttributes{ScheduledTime: at}, &wcipb.MetricsPollTimerTask{PollTime: common.CloneProto(w.NextPollTime)})
}

func (w *WorkerControllerInstance) armValidationTimer(ctx chasm.MutableContext, at time.Time) {
	w.NextValidationTime = timestamppb.New(at)
	ctx.AddTask(w, chasm.TaskAttributes{ScheduledTime: at}, &wcipb.PeriodicValidationTimerTask{ValidationTime: common.CloneProto(w.NextValidationTime)})
}
