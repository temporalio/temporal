package testlogger_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

// mockT wraps a testing.T such that we can know if a test is failed.
type mockT struct {
	*testing.T
	failure atomic.Pointer[string]
}

// Errorf implements testlogger.TestingT.
func (m *mockT) Errorf(msg string, args ...any) {
	s := fmt.Sprintf("ERROR: "+msg, args...)
	m.failure.Store(&s)
}

// Fail implements testlogger.TestingT.
func (m *mockT) Fail() {
	s := "Fail() called"
	m.failure.Store(&s)
}

// FailNow implements testlogger.TestingT.
func (m *mockT) FailNow() {
	s := "FailNow() called"
	m.failure.Store(&s)
}

// Failed implements testlogger.TestingT.
func (m *mockT) Failed() bool {
	return m.failure.Load() != nil
}

// Fatal implements testlogger.TestingT.
func (m *mockT) Fatal(args ...any) {
	s := fmt.Sprintf("FATAL: %v", args...)
	m.failure.Store(&s)
}

// Fatalf implements testlogger.TestingT.
func (m *mockT) Fatalf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	m.failure.Store(&s)
}

var _ testlogger.TestingT = (*mockT)(nil)

func assertMatches(t *testing.T, level testlogger.Level, msg string, tags []tag.Tag) {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	tl.Expect(level, msg, tags...)

	switch level {
	case testlogger.Error:
		tl.Error(msg, tags...)
	case testlogger.Fatal:
		require.Panics(t, func() {
			tl.Fatal(msg, tags...)
		})
	case testlogger.DPanic:
		require.NotPanics(t, func() {
			tl.DPanic(msg, tags...)
		})
	case testlogger.Panic:
		require.Panics(t, func() {
			tl.Panic(msg, tags...)
		})
	default:
		t.Fatalf("Unknown log level %s", level)
	}
}

func TestTestLogger_ExpectationsMatch(t *testing.T) {
	for _, level := range []testlogger.Level{testlogger.Error, testlogger.DPanic, testlogger.Panic, testlogger.Fatal} {
		t.Run(level.String()+" with tags", func(t *testing.T) {
			assertMatches(t, level, "message with tags", []tag.Tag{tag.String("key", "value")})
		})
		t.Run(level.String()+" without tags", func(t *testing.T) {
			assertMatches(t, level, "message without tags", nil)
		})
		t.Run(level.String()+" no message only tags", func(t *testing.T) {
			assertMatches(t, level, "", []tag.Tag{tag.String("key", "value")})
		})
	}

}

func TestTestLogger_InfoExpectationMatchCount(t *testing.T) {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	expected := tl.Expect(testlogger.Info, "expected info", tag.String("key", "value"))
	require.False(t, expected.Matched())
	require.Zero(t, expected.MatchCount())

	tl.Info("expected info", tag.String("key", "other"))
	require.False(t, expected.Matched())
	require.Zero(t, expected.MatchCount())

	tl.Info("expected info", tag.String("key", "value"))
	require.True(t, expected.Matched())
	require.Equal(t, int64(1), expected.MatchCount())

	tl.Info("expected info", tag.String("key", "value"))
	require.Equal(t, int64(2), expected.MatchCount())
}

func assertFails(t *testing.T, level testlogger.Level, msg string, tags []tag.Tag) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnAnyUnexpectedError)

	switch level {
	case testlogger.Error:
		tl.Error(msg, tags...)
		require.True(t, mt.Failed())
	case testlogger.Fatal:
		tl.Fatal(msg, tags...)
		require.True(t, mt.Failed())
	case testlogger.DPanic:
		tl.DPanic(msg, tags...)
		require.True(t, mt.Failed())
	case testlogger.Panic:
		tl.Panic(msg, tags...)
		require.True(t, mt.Failed())
	default:
		t.Fatalf("Unknown log level %s", level)
	}
}

func TestTestLogger_Uncaught(t *testing.T) {
	// Non-panicking levels
	for _, level := range []testlogger.Level{testlogger.Error, testlogger.DPanic} {
		t.Run(level.String()+" with tags", func(t *testing.T) {
			assertFails(t, level, "message with tags", []tag.Tag{tag.String("key", "value")})
		})
		t.Run(level.String()+" without tags", func(t *testing.T) {
			assertFails(t, level, "message without tags", nil)
		})
		t.Run(level.String()+" no message only tags", func(t *testing.T) {
			assertFails(t, level, "", []tag.Tag{tag.String("key", "value")})
		})
	}
	// Panicking levels
	for _, level := range []testlogger.Level{testlogger.Panic, testlogger.Fatal} {
		t.Run(level.String()+" with tags", func(t *testing.T) {
			require.Panics(t, func() {
				assertFails(t, level, "message with tags", []tag.Tag{tag.String("key", "value")})
			})
		})
		t.Run(level.String()+" without tags", func(t *testing.T) {
			require.Panics(t, func() {
				assertFails(t, level, "message without tags", nil)
			})
		})
		t.Run(level.String()+" no message only tags", func(t *testing.T) {
			require.Panics(t, func() {
				assertFails(t, level, "", []tag.Tag{tag.String("key", "value")})
			})
		})
	}
}

// TestTestLogger_Failure_StickyOnAnyUnexpected verifies that Failure() captures
// the first failure-worthy log in FailOnAnyUnexpectedError mode and that
// subsequent failures do not overwrite it (first-failure-wins via CAS).
func TestTestLogger_Failure_StickyOnAnyUnexpected(t *testing.T) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnAnyUnexpectedError)
	require.Nil(t, tl.Failure())

	tl.Error("first")
	first := tl.Failure()
	require.NotNil(t, first)
	require.Equal(t, testlogger.Error, first.Level)
	require.Equal(t, "first", first.Msg)
	require.NotEmpty(t, first.Stack)

	tl.Error("second")
	require.Same(t, first, tl.Failure(), "first failure should win")
}

// TestTestLogger_Failure_OnExpectedMatch is the soft-assert path: in
// FailOnExpectedErrorOnly mode, an Error matching a registered expectation
// (e.g. tag.FailedAssertion) should mark Failure().
func TestTestLogger_Failure_OnExpectedMatch(t *testing.T) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnExpectedErrorOnly)
	tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)
	require.Nil(t, tl.Failure())

	tl.Error("failed assertion: bad", tag.FailedAssertion)

	f := tl.Failure()
	require.NotNil(t, f)
	require.Equal(t, "failed assertion: bad", f.Msg)
}

// TestTestLogger_Failure_NoMatch verifies that FailOnExpectedErrorOnly remains an
// escape hatch: an Error with no matching expectation does not flip Failure().
func TestTestLogger_Failure_NoMatch(t *testing.T) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnExpectedErrorOnly)
	require.Nil(t, tl.Failure())

	tl.Error("ignored")
	require.Nil(t, tl.Failure())
}
