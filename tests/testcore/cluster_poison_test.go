package testcore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/testlogger"
)

// mockOwningT is the *testing.T that the TestLogger captures in state.t.
// It overrides Fatalf/Errorf/Fail/FailNow so the logger's synchronous failTest
// path does not propagate to the real outer test we're trying to observe.
type mockOwningT struct {
	*testing.T
	failure atomic.Pointer[string]
}

func (m *mockOwningT) Errorf(format string, args ...any) {
	s := fmt.Sprintf("ERROR: "+format, args...)
	m.failure.Store(&s)
}
func (m *mockOwningT) Fail()             { s := "Fail() called"; m.failure.Store(&s) }
func (m *mockOwningT) FailNow()          { s := "FailNow() called"; m.failure.Store(&s) }
func (m *mockOwningT) Failed() bool      { return m.failure.Load() != nil }
func (m *mockOwningT) Fatal(args ...any) { s := fmt.Sprint(args...); m.failure.Store(&s) }
func (m *mockOwningT) Fatalf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	m.failure.Store(&s)
}

// mockSubtestT stands in for an in-flight subtest passed to AcquireForTest.
// It captures Cleanup callbacks into a slice (instead of registering with the
// testing framework) and captures Errorf so the test can assert on it.
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
	// wantErrSubstring == "" asserts no errs were recorded; non-empty asserts at
	// least one err containing the substring.
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
			tl := testlogger.NewTestLogger(&mockOwningT{T: t}, testlogger.FailOnExpectedErrorOnly,
				testlogger.WithWriter(&clusterLogWriter{s: s}),
				testlogger.WithoutCloseOnCleanup(),
			)
			tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)
			s.Logger = tl

			for i, p := range tc.phases {
				sub := &mockSubtestT{T: t}
				s.AcquireForTest(sub)
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
