package matching

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
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
	cfg         scalerFactoryCfg
	legacyCount dynamicconfig.IntPropertyFnWithTaskQueueFilter
}

func newSimplePartitionScalerFactory(
	cfg scalerFactoryCfg,
	legacyCount dynamicconfig.IntPropertyFnWithTaskQueueFilter,
) *simplePartitionScalerFactory {
	return &simplePartitionScalerFactory{cfg: cfg, legacyCount: legacyCount}
}

func (s *simplePartitionScalerFactory) New(
	nsName namespace.Name,
	tqName string,
	tqType enumspb.TaskQueueType,
	logger log.Logger,
	metricsHandler metrics.Handler,
) PartitionScaler {
	cfg := func() dynamicconfig.SimplePartitionScalerSettings { return s.cfg(nsName.String(), tqName, tqType) }
	legacyCount := func() int { return s.legacyCount(nsName.String(), tqName, tqType) }
	return newSimplePartitionScaler(cfg, legacyCount, clock.NewRealTimeSource(), logger, metricsHandler)
}

// simplePartitionScaler uses task add rates to scale partitions.
type simplePartitionScaler struct {
	cfg scalerCfg
	// legacyCount returns the "legacy" static partition count that the *AsMultipleOfLegacy
	// settings are relative to. May be nil, which disables those settings.
	legacyCount               dynamicconfig.IntPropertyFn
	ts                        clock.TimeSource
	logger                    log.Logger
	metricsHandler            metrics.Handler
	upTrackers                map[time.Duration]*taskTracker
	downTrackers              map[time.Duration]*taskTracker
	lastLoggedTargetBeforeMax int
	lastCall                  time.Time
}

func newSimplePartitionScaler(
	cfg scalerCfg,
	legacyCount dynamicconfig.IntPropertyFn,
	ts clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *simplePartitionScaler {
	return &simplePartitionScaler{
		cfg:            cfg,
		legacyCount:    legacyCount,
		ts:             ts,
		logger:         logger,
		metricsHandler: metricsHandler,
		upTrackers:     make(map[time.Duration]*taskTracker),
		downTrackers:   make(map[time.Duration]*taskTracker),
	}
}

func (s *simplePartitionScaler) getTracker(trackers map[time.Duration]*taskTracker, interval time.Duration) *taskTracker {
	if t, ok := trackers[interval]; ok {
		return t
	}
	t := newTaskTracker(s.ts, interval/simplePartitionScalerTrackerBuckets, interval)
	trackers[interval] = t
	return t
}

func (s *simplePartitionScaler) OnTasks(in PartitionScalerInput) PartitionScalerDecision {
	// in.NumTasks arrived since the previous call, so track that interval even across calls
	// that return early.
	now := s.ts.Now()
	prevCall := s.lastCall
	if prevCall.IsZero() {
		prevCall = now
	}
	s.lastCall = now

	cfg := s.cfg()

	if !cfg.Enabled {
		s.lastLoggedTargetBeforeMax = 0
		return PartitionScalerDecision{NewTarget: 0, Reason: "disabled"}
	}

	var legacyCount int // read at most once per call
	multiplied := func(setting float32) int {
		if setting <= 0 || s.legacyCount == nil {
			return 0
		}
		if legacyCount == 0 {
			legacyCount = max(1, s.legacyCount())
		}
		return max(1, int(setting*float32(legacyCount)+0.5))
	}

	if cfg.Fixed > 0 {
		s.lastLoggedTargetBeforeMax = 0
		return PartitionScalerDecision{NewTarget: int(cfg.Fixed), BacklogCap: int(cfg.BacklogCap), Reason: "fixed"}
	} else if fixed := multiplied(cfg.FixedAsMultipleOfLegacy); fixed > 0 {
		s.lastLoggedTargetBeforeMax = 0
		return PartitionScalerDecision{NewTarget: fixed, BacklogCap: int(cfg.BacklogCap), Reason: "fixed as multiple of legacy"}
	}

	// init trackers in use
	for _, down := range cfg.Downs {
		_ = s.getTracker(s.downTrackers, down.Window)
	}
	for _, up := range cfg.Ups {
		_ = s.getTracker(s.upTrackers, up.Window)
	}

	// TODO(dp): optimization: use one tracker and query it for different intervals.
	// TODO(dp): clean up trackers that are unused after config change.
	for _, t := range s.upTrackers {
		t.incOver(in.NumTasks, prevCall)
	}
	for _, t := range s.downTrackers {
		t.incOver(in.NumTasks, prevCall)
	}

	// unmarshal our state
	var state persistencespb.SimplePartitionScalerState
	if in.PrivateState.UnmarshalTo(&state) != nil {
		// initialize from scaler if unset
		state.AddTarget = int32(in.CurrentTarget)
	}

	// update add target based on rates
	addTarget, addReason, fullInterval := s.updateAddTarget(cfg, int(state.AddTarget))
	if !fullInterval {
		return PartitionScalerDecision{NoChange: true}
	}
	state.AddTarget = int32(addTarget)

	// update backlog target based on counts
	backlogTarget := updateBacklogTarget(cfg, in.BacklogCounts, (*bitSet)(&state.BacklogTarget))

	// add them and clamp. note all mins are applied before all maxes, so a max wins if the
	// two are in conflict.
	totalTarget := addTarget + backlogTarget
	if cfg.Min > 0 {
		totalTarget = max(totalTarget, int(cfg.Min))
	}
	if multipliedMin := multiplied(cfg.MinAsMultipleOfLegacy); multipliedMin > 0 {
		totalTarget = max(totalTarget, multipliedMin)
	}
	targetBeforeMax := totalTarget
	if cfg.Max > 0 {
		totalTarget = min(totalTarget, int(cfg.Max))
	}
	if multipliedMax := multiplied(cfg.MaxAsMultipleOfLegacy); multipliedMax > 0 {
		totalTarget = min(totalTarget, multipliedMax)
	}
	if totalTarget >= targetBeforeMax {
		s.lastLoggedTargetBeforeMax = 0
	} else if targetBeforeMax != s.lastLoggedTargetBeforeMax {
		s.lastLoggedTargetBeforeMax = targetBeforeMax
		metrics.PartitionScaleMaxClamped.With(s.metricsHandler).Record(1)
		s.logger.Info("partition scale target clamped by maximum",
			tag.Int("target-before-max", targetBeforeMax),
			tag.Int("target", totalTarget))
	}

	privateState, _ := anypb.New(&state) // ignore error, just use nil
	return PartitionScalerDecision{
		NewTarget:    totalTarget,
		BacklogCap:   int(cfg.BacklogCap),
		PrivateState: privateState,
		Reason:       addReason,
	}
}

func (*simplePartitionScaler) Stop() {
}

// windowRate is the rate measured in one Ups or Downs window and the add target it implies.
type windowRate struct {
	kind   string // "ups" or "downs"
	index  int
	window time.Duration
	rate   float32
	want   int
}

// String formats the window for logging, e.g. "ups[1] (10s) rate=550.0 wants=6".
func (w windowRate) String() string {
	return fmt.Sprintf("%s[%d] (%v) rate=%.1f wants=%d", w.kind, w.index, w.window, w.rate, w.want)
}

// updateAddTarget returns the new add target, a description of which window determined it
// (for logging), and whether all windows were full.
func (s *simplePartitionScaler) updateAddTarget(
	cfg dynamicconfig.SimplePartitionScalerSettings,
	prevTarget int,
) (int, string, bool) {
	target := prevTarget
	// decider is the window that moved the target. if no window moved it, closestDown and
	// closestUp are the windows nearest to moving it, i.e. what held it in place.
	var decider, closestDown, closestUp *windowRate

	for i, down := range cfg.Downs {
		rate, full := s.getTracker(s.downTrackers, down.Window).rateAndFull()
		if !full {
			// A decrease requires a full window of data since the last increase or since startup.
			continue
		}
		// decrease target so that each partition is ~= target rate
		wr := &windowRate{kind: "downs", index: i, window: down.Window, rate: rate,
			want: max(1, int(rate/float32(down.TargetRate)+0.5))}
		if closestDown == nil || wr.want < closestDown.want {
			closestDown = wr
		}
		if wr.want < target {
			target, decider = wr.want, wr
		}
	}

	for i, up := range cfg.Ups {
		rate, full := s.getTracker(s.upTrackers, up.Window).rateAndFull()
		if !full {
			return 0, "", false
		}
		// increase target so that each partition is ~= target rate
		wr := &windowRate{kind: "ups", index: i, window: up.Window, rate: rate,
			want: max(1, int(rate/float32(up.TargetRate)+0.5))}
		if closestUp == nil || wr.want > closestUp.want {
			closestUp = wr
		}
		if wr.want > target {
			target, decider = wr.want, wr
		}
	}

	if prevTarget > 0 && target > prevTarget {
		// Down windows include load from before this increase, so they would immediately
		// pull the target back down. Restart them now (not lazily on the next call, which may
		// be many seconds later) so a decrease needs exactly one window of new data.
		clear(s.downTrackers)
		for _, down := range cfg.Downs {
			_ = s.getTracker(s.downTrackers, down.Window)
		}
	}

	// always keep min of 1 even if we have no Ups/Downs
	if target < 1 {
		target = 1
		if decider == nil && closestDown == nil && closestUp == nil {
			return target, "no rate windows, floor of 1", true
		}
	}

	switch {
	case decider != nil:
		return target, "set by " + decider.String(), true
	case closestDown != nil && closestUp != nil:
		return target, "held between " + closestDown.String() + " and " + closestUp.String(), true
	case closestDown != nil:
		return target, "held by " + closestDown.String(), true
	case closestUp != nil:
		return target, "held by " + closestUp.String(), true
	default:
		return target, "no rate windows", true
	}
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
