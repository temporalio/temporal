package testcore

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"time"
	"weak"

	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.uber.org/fx"
)

// ClusterLeakRefs holds weak pointers to a cluster's key objects. Capture it
// via [TestEnv.LeakRefs] inside a subtest before teardown runs; after GC, pass
// a slice of these to [CheckLeakRefs] to detect objects that survived.
type ClusterLeakRefs struct {
	label    string
	cluster  weak.Pointer[TestCluster]
	testBase weak.Pointer[persistencetests.TestBase]
	host     weak.Pointer[TemporalImpl]
	fxApps   []weak.Pointer[fx.App]
	probes   []leakRefProbe
}

type leakRefProbe struct {
	name     string
	retained func() bool
}

func newLeakRefProbe[T any](name string, ptr *T) leakRefProbe {
	if ptr == nil {
		return leakRefProbe{name: name}
	}
	w := weak.Make(ptr)
	return leakRefProbe{
		name: name,
		retained: func() bool {
			return w.Value() != nil
		},
	}
}

// LeakRefs captures weak pointers to the cluster's key server objects. It must
// be called inside the subtest that created the env, before the subtest's
// cleanup (and therefore cluster teardown) runs.
func (e *TestEnv) LeakRefs(label string) ClusterLeakRefs {
	tc := e.cluster
	fxApps := make([]weak.Pointer[fx.App], 0, len(tc.Host().fxApps))
	for _, app := range tc.Host().fxApps {
		fxApps = append(fxApps, weak.Make(app))
	}
	return ClusterLeakRefs{
		label:    label,
		cluster:  weak.Make(tc),
		testBase: weak.Make(tc.TestBase()),
		host:     weak.Make(tc.Host()),
		fxApps:   fxApps,
		probes: append(
			[]leakRefProbe{newLeakRefProbe("dynamic config memory client", tc.Host().dcClient)},
			tc.Host().leakRefProbes...,
		),
	}
}

// CheckLeakRefs runs GC, waits for outstanding finalizers, then checks whether
// any cluster objects survived. Returns failure strings for objects that should
// have been collected:
//   - TestCluster and TestBase must always reach zero — the pool drops its ref
//     and nothing else should hold them.
//   - TemporalImpl must reach zero too; a retained host pins the stopped server
//     graph and any heap it still references.
//   - fx.App instances and named probes must reach zero for all but the newest
//     cluster. The newest test goroutine can keep its last stack frame live, but
//     older retained apps indicate accumulating heap.
func CheckLeakRefs(refs []ClusterLeakRefs) []string {
	SettleLeakCheck()
	var failures []string
	for i, r := range refs {
		if r.cluster.Value() != nil {
			failures = append(failures, fmt.Sprintf("%s: TestCluster retained after teardown", r.label))
		}
		if r.testBase.Value() != nil {
			failures = append(failures, fmt.Sprintf("%s: TestBase retained after teardown", r.label))
		}
		if r.host.Value() != nil {
			failures = append(failures, fmt.Sprintf("%s: TemporalImpl retained after teardown", r.label))
		}
		if i == len(refs)-1 {
			continue
		}
		for appIdx, app := range r.fxApps {
			if app.Value() != nil {
				failures = append(failures, fmt.Sprintf("%s: fx app %d retained after teardown", r.label, appIdx))
			}
		}
		for _, probe := range r.probes {
			if probe.retained != nil && probe.retained() {
				failures = append(failures, fmt.Sprintf("%s: %s retained after teardown", r.label, probe.name))
			}
		}
	}
	return failures
}

// SettleLeakCheck gives stopped services, runtime timers, and GC enough time to
// release teardown-only references before leak probes sample object reachability.
func SettleLeakCheck() {
	for range 200 {
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(20 * time.Millisecond)
	}
}
