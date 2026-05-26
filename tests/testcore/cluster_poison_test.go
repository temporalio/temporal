package testcore

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/testlogger"
)

// mockSubtestT stands in for an in-flight subtest passed to RegisterTest.
// Cleanup callbacks are queued so the test can drive them explicitly via finish().
// Failure-signalling methods are absorbed to prevent the outer testing.T from being marked failed.
type mockSubtestT struct {
	*testing.T
	mu       sync.Mutex
	cleanups []func()
	errs     []string
}

func (f *mockSubtestT) Cleanup(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, fn)
}

func (f *mockSubtestT) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, fmt.Sprintf(format, args...))
}

func (f *mockSubtestT) Fail()    {}
func (f *mockSubtestT) FailNow() {}

// finish runs registered cleanups in LIFO order, mirroring *testing.T.
func (f *mockSubtestT) finish() {
	f.mu.Lock()
	cleanups := f.cleanups
	f.cleanups = nil
	f.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

func TestSharedClusterPoison(t *testing.T) {
	// Each phase represents one acquirer's lifetime: Acquire → optional log → finish.
	// wantErrSubstring asserts on RegisterTest's cleanup t.Errorf.
	type phase struct {
		log              func(log.Logger)
		wantPoisoned     bool
		wantErrSubstring string
	}

	cases := []struct {
		name   string
		phases []phase
	}{
		{
			name: "soft assert during test fails it",
			phases: []phase{
				{
					log:              func(l log.Logger) { softassert.Fail(l, "synthetic-failure") },
					wantPoisoned:     true,
					wantErrSubstring: "synthetic-failure",
				},
			},
		},
		{
			name: "subsequent acquirer on poisoned cluster also fails",
			phases: []phase{
				{
					log:              func(l log.Logger) { softassert.Fail(l, "synthetic-failure") },
					wantPoisoned:     true,
					wantErrSubstring: "synthetic-failure",
				},
				{
					wantPoisoned:     true,
					wantErrSubstring: "poisoned",
				},
			},
		},
		{
			name: "no soft assert leaves cluster clean",
			phases: []phase{
				{
					log: func(l log.Logger) { l.Info("just a normal log") },
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &FunctionalTestBase{}
			s.t = &sharedClusterT{name: t.Name()}
			tl := testlogger.NewTestLogger(s.t, testlogger.FailOnExpectedErrorOnly)
			tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)
			s.Logger = tl

			for i, p := range tc.phases {
				sub := &mockSubtestT{T: t}
				s.RegisterTest(sub)
				if p.log != nil {
					p.log(s.Logger)
				}
				sub.finish()

				require.Equal(t, p.wantPoisoned, s.Poisoned(), "phase %d", i)
				if p.wantErrSubstring == "" {
					require.Empty(t, sub.errs, "phase %d", i)
				} else {
					require.NotEmpty(t, sub.errs, "phase %d", i)
					require.Contains(t, sub.errs[0], p.wantErrSubstring, "phase %d", i)
				}
			}
		})
	}
}
