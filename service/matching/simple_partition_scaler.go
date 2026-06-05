package matching

import (
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/anypb"
)

type scalerFactoryCfg = dynamicconfig.TypedPropertyFnWithTaskQueueFilter[dynamicconfig.SimplePartitionScalerSettings]
type scalerCfg = dynamicconfig.TypedPropertyFn[dynamicconfig.SimplePartitionScalerSettings]

// simplePartitionScalerFactory creates simplePartitionScalers.
type simplePartitionScalerFactory struct {
	cfg scalerFactoryCfg
}

func newSimplePartitionScalerFactory(cfg scalerFactoryCfg) *simplePartitionScalerFactory {
	return &simplePartitionScalerFactory{cfg: cfg}
}

func (s *simplePartitionScalerFactory) New(
	nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType,
) PartitionScaler {
	cfg := func() dynamicconfig.SimplePartitionScalerSettings { return s.cfg(nsName.String(), tqName, tqType) }
	return newSimplePartitionScaler(cfg, clock.NewRealTimeSource())
}

// simplePartitionScaler uses task add rates to scale partitions.
type simplePartitionScaler struct {
	cfg      scalerCfg
	ts       clock.TimeSource
	trackers sync.Map // time.Duration -> *taskTracker
}

func newSimplePartitionScaler(cfg scalerCfg, ts clock.TimeSource) *simplePartitionScaler {
	return &simplePartitionScaler{
		cfg: cfg,
		ts:  ts,
	}
}

func (s *simplePartitionScaler) getTracker(interval time.Duration) *taskTracker {
	if t, _ := s.trackers.Load(interval); t != nil {
		return t.(*taskTracker) //nolint:revive
	}
	newT := newTaskTracker(s.ts, interval/10, interval)
	t, _ := s.trackers.LoadOrStore(interval, newT)
	return t.(*taskTracker)
}

func (s *simplePartitionScaler) OnTasks(in PartitionScalerInput) PartitionScalerDecision {
	cfg := s.cfg()

	if !cfg.Enabled {
		return PartitionScalerDecision{NewTarget: 0}
	} else if cfg.Fixed > 0 {
		return PartitionScalerDecision{NewTarget: int(cfg.Fixed)}
	}

	// TODO(dp): optimization: use one tracker and query it for different intervals.
	// TODO(dp): clean up trackers that are unused after config change.
	s.trackers.Range(func(_, t any) bool {
		t.(*taskTracker).inc(in.NumTasks)
		return true
	})

	// unmarshal our state
	var state persistencespb.SimplePartitionScalerState
	if in.PrivateState.UnmarshalTo(&state) != nil {
		// initialize from scaler if unset
		state.AddTarget = int32(in.CurrentTarget)
	}

	// update add target based on rates
	addTarget := s.updateAddTarget(cfg, int(state.AddTarget))
	state.AddTarget = int32(addTarget)

	totalTarget := addTarget
	if cfg.Min > 0 {
		totalTarget = max(totalTarget, int(cfg.Min))
	}
	if cfg.Max > 0 {
		totalTarget = min(totalTarget, int(cfg.Max))
	}

	privateState, _ := anypb.New(&state) // ignore error, just use nil
	return PartitionScalerDecision{
		NewTarget:    totalTarget,
		PrivateState: privateState,
	}
}

func (*simplePartitionScaler) Stop() {
}

func (s *simplePartitionScaler) updateAddTarget(
	cfg dynamicconfig.SimplePartitionScalerSettings,
	target int,
) int {
	for _, down := range cfg.Downs {
		if !validateSimplePartitionScalerThreshold(down) {
			continue
		}
		rate := s.getTracker(down.Window).rate()
		// decrease target so that each partition is ~= target rate
		target = max(1, min(
			target,
			int(rate/float32(down.TargetRate)+0.5),
		))
	}

	for _, up := range cfg.Ups {
		if !validateSimplePartitionScalerThreshold(up) {
			continue
		}
		rate := s.getTracker(up.Window).rate()
		// increase target so that each partition is ~= target rate
		target = max(
			target,
			int(rate/float32(up.TargetRate)+0.5),
			1,
		)
	}

	return target
}

func validateSimplePartitionScalerThreshold(t dynamicconfig.SimplePartitionScalerThreshold) bool {
	return t.Window >= 100*time.Millisecond && t.TargetRate >= 1
}
