package matching

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/number"
	"google.golang.org/protobuf/types/known/anypb"
)

// number of buckets used for task tracker
const simplePartitionScalerTrackerBuckets = 10

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
	trackers map[time.Duration]*taskTracker
}

func newSimplePartitionScaler(cfg scalerCfg, ts clock.TimeSource) *simplePartitionScaler {
	return &simplePartitionScaler{
		cfg:      cfg,
		ts:       ts,
		trackers: make(map[time.Duration]*taskTracker),
	}
}

func (s *simplePartitionScaler) getTracker(interval time.Duration) *taskTracker {
	if t, ok := s.trackers[interval]; ok {
		return t
	}
	t := newTaskTracker(s.ts, interval/simplePartitionScalerTrackerBuckets, interval)
	s.trackers[interval] = t
	return t
}

func (s *simplePartitionScaler) OnTasks(in PartitionScalerInput) PartitionScalerDecision {
	cfg := s.cfg()

	if !cfg.Enabled {
		return PartitionScalerDecision{NewTarget: 0}
	} else if cfg.Fixed > 0 {
		return PartitionScalerDecision{NewTarget: int(cfg.Fixed), BacklogCap: int(cfg.BacklogCap)}
	}

	// init trackers in use
	for _, down := range cfg.Downs {
		_ = s.getTracker(down.Window)
	}
	for _, up := range cfg.Ups {
		_ = s.getTracker(up.Window)
	}

	// TODO(dp): optimization: use one tracker and query it for different intervals.
	// TODO(dp): clean up trackers that are unused after config change.
	for _, t := range s.trackers {
		t.inc(in.NumTasks)
	}

	// unmarshal our state
	var state persistencespb.SimplePartitionScalerState
	if in.PrivateState.UnmarshalTo(&state) != nil {
		// initialize from scaler if unset
		state.AddTarget = int32(in.CurrentTarget)
	}

	// update add target based on rates
	addTarget, fullInterval := s.updateAddTarget(cfg, int(state.AddTarget))
	if !fullInterval {
		return PartitionScalerDecision{NoChange: true}
	}
	state.AddTarget = int32(addTarget)

	// update backlog target based on counts
	backlogTarget := updateBacklogTarget(cfg, in.BacklogCounts, (*bitSet)(&state.BacklogTarget))

	// add them and clamp
	totalTarget := addTarget + backlogTarget
	if cfg.Min > 0 {
		totalTarget = max(totalTarget, int(cfg.Min))
	}
	if cfg.Max > 0 {
		totalTarget = min(totalTarget, int(cfg.Max))
	}

	privateState, _ := anypb.New(&state) // ignore error, just use nil
	return PartitionScalerDecision{
		NewTarget:    totalTarget,
		BacklogCap:   int(cfg.BacklogCap),
		PrivateState: privateState,
	}
}

func (*simplePartitionScaler) Stop() {
}

func (s *simplePartitionScaler) updateAddTarget(
	cfg dynamicconfig.SimplePartitionScalerSettings,
	target int,
) (int, bool) {
	// TODO(dp): we should return some information about which window made the change and put
	// a log of those in the scale state
	for _, down := range cfg.Downs {
		rate, full := s.getTracker(down.Window).rateAndFull()
		if !full {
			return 0, false
		}
		// decrease target so that each partition is ~= target rate
		target = max(1, min(
			target,
			int(rate/float32(down.TargetRate)+0.5),
		))
	}

	for _, up := range cfg.Ups {
		rate, full := s.getTracker(up.Window).rateAndFull()
		if !full {
			return 0, false
		}
		// increase target so that each partition is ~= target rate
		target = max(
			target,
			int(rate/float32(up.TargetRate)+0.5),
			1,
		)
	}

	// always keep min of 1 even if we have no Ups/Downs
	target = max(target, 1)

	return target, true
}

func updateBacklogTarget(
	cfg dynamicconfig.SimplePartitionScalerSettings,
	counts []byte,
	bs *bitSet,
) int {
	if cfg.BacklogBase <= 0 || cfg.BacklogReset <= 0 {
		return 0
	}

	target := 0
	for i, v := range counts {
		count := int32(number.DecodeCompact8(v))
		if count > cfg.BacklogBase {
			*bs = bs.set(int32(i))
		} else if count < cfg.BacklogReset {
			*bs = bs.clear(int32(i))
		}

		if bs.get(int32(i)) {
			target++
		}
	}
	return target
}
