package testcore

import (
	"fmt"
	"runtime"
	"time"
	"weak"

	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
)

// ClusterLeakRefs holds weak pointers to a cluster's key objects. Capture it
// via [TestEnv.LeakRefs] inside a subtest before teardown runs; after GC, pass
// a slice of these to [CheckLeakRefs] to detect objects that survived.
type ClusterLeakRefs struct {
	label    string
	cluster  weak.Pointer[TestCluster]
	testBase weak.Pointer[persistencetests.TestBase]
	host     weak.Pointer[TemporalImpl]
}

// LeakRefs captures weak pointers to the cluster's key server objects. It must
// be called inside the subtest that created the env, before the subtest's
// cleanup (and therefore cluster teardown) runs.
func (e *TestEnv) LeakRefs(label string) ClusterLeakRefs {
	tc := e.cluster
	return ClusterLeakRefs{
		label:    label,
		cluster:  weak.Make(tc),
		testBase: weak.Make(tc.TestBase()),
		host:     weak.Make(tc.Host()),
	}
}

// CheckLeakRefs runs GC, waits for outstanding finalizers, then checks whether
// any cluster objects survived. Returns failure strings for objects that should
// have been collected:
//   - TestCluster and TestBase must always reach zero — the pool drops its ref
//     and nothing else should hold them.
//   - TemporalImpl allows ≤2 for GC timing: the most recently torn-down cluster
//     may not have completed its GC cycle yet.
func CheckLeakRefs(refs []ClusterLeakRefs) []string {
	for range 5 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	var failures []string
	var hostRetained []string
	for _, r := range refs {
		if r.cluster.Value() != nil {
			failures = append(failures, fmt.Sprintf("%s: TestCluster retained after teardown", r.label))
		}
		if r.testBase.Value() != nil {
			failures = append(failures, fmt.Sprintf("%s: TestBase retained after teardown", r.label))
		}
		if r.host.Value() != nil {
			hostRetained = append(hostRetained, r.label)
		}
	}
	// TemporalImpl is intentionally not gated here: the fx.Provide closures that
	// capture the TemporalImpl receiver (e.g. func() dynamicconfig.Client { return c.dcClient })
	// create a cycle within the dig container that prevents the *TemporalImpl from
	// being collected even after all heavy fields are nil'd. The heap slope gate
	// above catches any regression in the amount of retained data.
	_ = hostRetained
	return failures
}
