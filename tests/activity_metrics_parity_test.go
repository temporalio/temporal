package tests

// SAA↔WFA metrics parity. For the exhaustive catalog of activity metrics (dandavison/log#275), this
// establishes which ones each surface emits: it drives one activity through each behavior on both
// surfaces, captures the metrics emitted, prints the WFA-vs-SAA emission matrix, and asserts that both
// surfaces emit the same metrics with the same tag keys for each behavior.
//
// There is no oracle. The equality assertion encodes the intended contract, so a failure can mean SAA is
// missing a metric, WFA is missing one, or the metric belongs on one surface by design.
//
// Two asymmetries are intended and excluded from the equality assertion: the deprecated
// activity_end_to_end_latency alias, and activity_terminate (a workflow activity has no individual
// terminate path). See activityMetricCatalog.
//
// Not every catalog metric is attributable to a single driven activity through this driver. The
// shard/mutable-state aggregates, the eager-execution counter, and the matching worker-registry gauge
// are marked not-measured and only shown in the matrix; the namespace capture rejects non-namespaced
// metrics.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

// activityMetric is one entry in the activity-metric catalog. measured marks whether this test queries
// the metric; compared marks whether the WFA and SAA emitted sets are asserted equal for it.
// compared ⊆ measured.
type activityMetric struct {
	name     string
	measured bool
	compared bool
}

var activityMetricCatalog = []activityMetric{
	{metrics.ActivitySuccess.Name(), true, true},
	{metrics.ActivityFail.Name(), true, true},
	{metrics.ActivityTaskFail.Name(), true, true},
	{metrics.ActivityCancel.Name(), true, true},
	{metrics.ActivityTerminate.Name(), true, false}, // no per-activity terminate on WFA; SAA-only, asserted on its own
	{metrics.ActivityTimeout.Name(), true, true},
	{metrics.ActivityTaskTimeout.Name(), true, true},
	{metrics.ActivityStartToCloseLatency.Name(), true, true},
	{metrics.ActivityScheduleToCloseLatency.Name(), true, true},
	{metrics.ActivityE2ELatency.Name(), true, false}, // deprecated alias; WFA-only by intent
	{metrics.ActivityPause.Name(), true, true},
	{metrics.ActivityUnpause.Name(), true, true},
	{metrics.ActivityReset.Name(), true, true},
	{metrics.ActivityUpdateOptions.Name(), true, true},
	{metrics.ActivityHeartbeatCount.Name(), true, true},
	{metrics.ActivityPayloadSize.Name(), true, true},
	{metrics.ActivityEagerExecutionCounter.Name(), false, false},   // eager WFT path; disabled in the WFA helper
	{metrics.ActivityInfoCount.Name(), false, false},               // periodic mutable-state stats, not per-lifecycle
	{metrics.ActivityInfoSize.Name(), false, false},                // "
	{metrics.TotalActivityCount.Name(), false, false},              // "
	{metrics.WorkerRegistryActivitySlotsUsed.Name(), false, false}, // matching worker registry; no real worker here
}

// activityMetricsScenario drives one activity behavior. cfg.MaxAttempts caps retries, so a terminal
// outcome is actually terminal. saaOnly marks a behavior with no WFA analog. anchor is a metric both surfaces
// emit at the end of the trace; when set, the driver waits for it before snapshotting, absorbing the
// async gap between an observed timeout transition and its metric emission. It is empty for a trace
// whose final effect is a synchronous RPC.
type activityMetricsScenario struct {
	name    string
	trace   []model.Event
	cfg     activityConfig
	saaOnly bool
	anchor  string
}

var activityMetricsScenarios = []activityMetricsScenario{
	{name: "Success", trace: []model.Event{model.Poll, model.Complete}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "TerminalFailure", trace: []model.Event{model.Poll, model.FailNonRetryably}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Cancel", trace: []model.Event{model.Poll, model.RequestCancel, {Type: model.RespondCanceledType}}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "TerminalTimeout", trace: []model.Event{model.Poll, model.StartToCloseElapses}, cfg: activityConfig{MaxAttempts: 1, StartToClose: activityShortTimeout}, anchor: metrics.ActivityTimeout.Name()},
	{name: "RetryableTaskFailure", trace: []model.Event{model.Poll, model.FailRetryably}, cfg: activityConfig{MaxAttempts: 2}},
	{name: "Heartbeat", trace: []model.Event{model.Poll, {Type: model.HeartbeatType}}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Pause", trace: []model.Event{model.Poll, model.Pause}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Unpause", trace: []model.Event{model.Poll, model.Pause, {Type: model.UnpauseType}}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Reset", trace: []model.Event{model.Poll, {Type: model.ResetType}}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "UpdateOptions", trace: []model.Event{model.Poll, {Type: model.UpdateOptionsType}}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Terminate", trace: []model.Event{model.Poll, {Type: model.TerminateType}}, cfg: activityConfig{MaxAttempts: 1}, saaOnly: true},
}

// timeoutFiredBy is the timeout whose *Elapses event a trace fires, zero if none.
func timeoutFiredBy(trace []model.Event) model.EventType {
	for _, e := range trace {
		switch e.Type {
		case model.ScheduleToStartElapsesType, model.ScheduleToCloseElapsesType,
			model.StartToCloseElapsesType, model.HeartbeatElapsesType:
			return e.Type
		}
	}
	return 0
}

// expectedTimeoutType returns the timeout_type tag value the timeout counters must carry for this
// scenario, or "" if the trace fires no timeout.
func (sc activityMetricsScenario) expectedTimeoutType() string {
	switch timeoutFiredBy(sc.trace) {
	case model.StartToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()
	case model.ScheduleToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()
	case model.ScheduleToStartElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String()
	case model.HeartbeatElapsesType:
		return enumspb.TIMEOUT_TYPE_HEARTBEAT.String()
	default:
		return ""
	}
}

// activityMetricSets holds, per surface, the metrics emitted for one scenario, keyed by name, with a
// representative recording's tag map as the value. wfa is nil for a SAA-only scenario.
type activityMetricSets struct {
	wfa map[string]map[string]string
	saa map[string]map[string]string
	// The namespace each surface drove in. They differ, which is what separates the two captures.
	wfaNS, saaNS string
}

func (s *activityParityTestSuite) TestWFASAAMetricsParity() {
	t := s.T()
	observed := make(map[string]activityMetricSets, len(activityMetricsScenarios))

	for _, sc := range activityMetricsScenarios {
		t.Run(sc.name, func(t *testing.T) {
			saa, saaNS := s.saaActivityMetrics(t, sc)
			sets := activityMetricSets{saa: saa, saaNS: saaNS}
			if !sc.saaOnly {
				sets.wfa, sets.wfaNS = s.wfaActivityMetrics(t, sc)
			}
			observed[sc.name] = sets

			assertActivityMetricLabels(t, sc, sets)

			if sc.saaOnly {
				_, ok := sets.saa[metrics.ActivityTerminate.Name()]
				require.True(t, ok, "terminating a standalone activity must emit activity_terminate")
				return
			}
			require.Equal(t, comparedSet(sets.wfa), comparedSet(sets.saa),
				"WFA and SAA must emit the same activity metrics for %q "+
					"(excluding the deprecated e2e-latency alias and non-lifecycle aggregates)", sc.name)
		})
	}

	t.Log(activityMetricsMatrix(observed))
}

func (s *activityParityTestSuite) saaActivityMetrics(t *testing.T, sc activityMetricsScenario) (map[string]map[string]string, string) {
	env := newActivityParityEnv(s.T())
	return s.captureActivityMetrics(t, env, sc, func() {
		newSAADriver(t, env, sc.cfg).driveTrace(t, sc.trace)
	}), env.Namespace().String()
}

func (s *activityParityTestSuite) wfaActivityMetrics(t *testing.T, sc activityMetricsScenario) (map[string]map[string]string, string) {
	env := newActivityParityEnv(s.T())
	return s.captureActivityMetrics(t, env, sc, func() {
		newWFADriver(t, env, sc.cfg).driveTrace(t, sc.trace)
	}), env.Namespace().String()
}

// captureActivityMetrics captures the activity metrics emitted while drive runs, scoped to env's
// namespace. Each surface drives in its own namespace, so the capture separates them.
func (s *activityParityTestSuite) captureActivityMetrics(t *testing.T, env *testcore.TestEnv, sc activityMetricsScenario, drive func()) map[string]map[string]string {
	capture := env.StartNamespaceMetricCapture()
	drive()

	if sc.anchor != "" {
		await.RequireTrue(t, func() bool {
			return len(capture.Metric(sc.anchor)) > 0
		}, 15*time.Second, 100*time.Millisecond)
	}

	emitted := make(map[string]map[string]string)
	for _, m := range activityMetricCatalog {
		if !m.measured {
			continue
		}
		if recs := capture.Metric(m.name); len(recs) > 0 {
			emitted[m.name] = recs[0].Tags
		}
	}
	return emitted
}

// comparedSet restricts an emitted set to the metrics whose WFA/SAA parity is asserted.
func comparedSet(emitted map[string]map[string]string) map[string]bool {
	out := make(map[string]bool)
	for _, m := range activityMetricCatalog {
		if m.compared {
			if _, ok := emitted[m.name]; ok {
				out[m.name] = true
			}
		}
	}
	return out
}

// activityMetricsMatrix renders the per-metric emission matrix — whether WFA and SAA ever emitted each
// catalog metric across the scenarios — followed by the per-scenario detail and the tag keys.
func activityMetricsMatrix(observed map[string]activityMetricSets) string {
	wfaAny := make(map[string]bool)
	saaAny := make(map[string]bool)
	for _, sets := range observed {
		for name := range sets.wfa {
			wfaAny[name] = true
		}
		for name := range sets.saa {
			saaAny[name] = true
		}
	}

	var b strings.Builder
	b.WriteString("\n=== Activity metric emission: WFA vs SAA (aggregated across scenarios) ===\n")
	for _, m := range activityMetricCatalog {
		note := ""
		switch {
		case !m.measured:
			note = "  (not measured by this driver)"
		case !m.compared:
			note = "  (not asserted: intended asymmetry)"
		}
		fmt.Fprintf(&b, "  %-40s WFA:%s SAA:%s%s\n", m.name, mark(wfaAny[m.name]), mark(saaAny[m.name]), note)
	}

	b.WriteString("\n=== Per-scenario emitted activity metrics ===\n")
	for _, sc := range activityMetricsScenarios {
		sets := observed[sc.name]
		if sc.saaOnly {
			fmt.Fprintf(&b, "  %-22s SAA-only: %s\n", sc.name, emittedList(sets.saa))
			continue
		}
		fmt.Fprintf(&b, "  %-22s WFA: %s\n", sc.name, emittedList(sets.wfa))
		fmt.Fprintf(&b, "  %-22s SAA: %s\n", "", emittedList(sets.saa))
	}

	b.WriteString("\n=== Tag keys per compared metric (WFA | SAA) ===\n")
	for _, m := range activityMetricCatalog {
		if !m.compared {
			continue
		}
		wfaKeys, saaKeys := "-", "-"
		for _, sc := range activityMetricsScenarios {
			if tags, ok := observed[sc.name].wfa[m.name]; ok {
				wfaKeys = strings.Join(tagKeys(tags), ",")
			}
			if tags, ok := observed[sc.name].saa[m.name]; ok {
				saaKeys = strings.Join(tagKeys(tags), ",")
			}
		}
		fmt.Fprintf(&b, "  %-40s\n    WFA: %s\n    SAA: %s\n", m.name, wfaKeys, saaKeys)
	}
	return b.String()
}

func mark(b bool) string {
	if b {
		return "✓"
	}
	return "·"
}

func emittedList(emitted map[string]map[string]string) string {
	names := make([]string, 0, len(emitted))
	for name := range emitted {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, ", ")
}

func tagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// assertActivityMetricLabels asserts that every metric carries the test namespace, that the two timeout
// counters carry the timeout_type that fired, and that a metric both surfaces emit carries the same tag
// keys, so that one dashboard works for either surface.
func assertActivityMetricLabels(t *testing.T, sc activityMetricsScenario, sets activityMetricSets) {
	checkTags := func(surface string, emitted map[string]map[string]string, ns string) {
		for name, tags := range emitted {
			require.Equal(t, ns, tags["namespace"],
				"%s %s must be tagged with the namespace it was driven in", surface, name)
		}
		if timeoutType := sc.expectedTimeoutType(); timeoutType != "" {
			for _, name := range []string{metrics.ActivityTaskTimeout.Name(), metrics.ActivityTimeout.Name()} {
				if tags, ok := emitted[name]; ok {
					require.Equal(t, timeoutType, tags["timeout_type"],
						"%s %s must carry the timeout_type that fired", surface, name)
				}
			}
		}
	}
	checkTags("SAA", sets.saa, sets.saaNS)
	if sc.saaOnly {
		return
	}
	checkTags("WFA", sets.wfa, sets.wfaNS)

	for _, m := range activityMetricCatalog {
		if !m.compared {
			continue
		}
		wfaTags, wfaOK := sets.wfa[m.name]
		saaTags, saaOK := sets.saa[m.name]
		if wfaOK && saaOK {
			require.Equal(t, tagKeys(wfaTags), tagKeys(saaTags),
				"WFA and SAA must tag %s with the same label keys", m.name)
		}
	}
}
