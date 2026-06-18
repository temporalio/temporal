package testcore

import (
	"errors"
	"fmt"
	"weak"

	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.uber.org/fx"
)

// TestEnvLeakCheck tracks one test env and verifies its stopped cluster graph is
// collectable after teardown.
type TestEnvLeakCheck struct {
	label    string
	cluster  weak.Pointer[TestCluster]
	testBase weak.Pointer[persistencetests.TestBase]
	host     weak.Pointer[TemporalImpl]
	fxApps   []weak.Pointer[fx.App]
}

// NewTestEnvLeakCheck captures weak pointers to env's key server objects. It
// must be called inside the subtest that created the env, before the subtest's
// cleanup (and therefore cluster teardown) runs.
func NewTestEnvLeakCheck(env *TestEnv, label string) TestEnvLeakCheck {
	tc := env.cluster
	fxApps := make([]weak.Pointer[fx.App], 0, len(tc.Host().fxApps))
	for _, app := range tc.Host().fxApps {
		fxApps = append(fxApps, weak.Make(app))
	}
	return TestEnvLeakCheck{
		label:    label,
		cluster:  weak.Make(tc),
		testBase: weak.Make(tc.TestBase()),
		host:     weak.Make(tc.Host()),
		fxApps:   fxApps,
	}
}

// Failures returns an error for objects that should have been collected:
//   - TestCluster and TestBase must always reach zero; the pool drops its ref
//     and nothing else should hold them.
//   - TemporalImpl must reach zero too; a retained host pins the stopped server
//     graph and any heap it still references.
//   - fx.App instances must reach zero.
func (c TestEnvLeakCheck) Failures() error {
	var failures []error
	if c.cluster.Value() != nil {
		failures = append(failures, fmt.Errorf("%s: TestCluster retained after teardown", c.label))
	}
	if c.testBase.Value() != nil {
		failures = append(failures, fmt.Errorf("%s: TestBase retained after teardown", c.label))
	}
	if c.host.Value() != nil {
		failures = append(failures, fmt.Errorf("%s: TemporalImpl retained after teardown", c.label))
	}
	for appIdx, app := range c.fxApps {
		if app.Value() != nil {
			failures = append(failures, fmt.Errorf("%s: fx app %d retained after teardown", c.label, appIdx))
		}
	}
	return errors.Join(failures...)
}
