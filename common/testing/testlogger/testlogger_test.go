package testlogger_test

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/zap/zapcore"
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

// TestTestLogger_Failed_StickyOnAnyUnexpected verifies that Failed() captures
// the first failure-worthy log in FailOnAnyUnexpectedError mode and that
// subsequent failures do not overwrite it (first-failure-wins via CAS).
func TestTestLogger_Failed_StickyOnAnyUnexpected(t *testing.T) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnAnyUnexpectedError)
	require.Nil(t, tl.Failed())

	tl.Error("first")
	first := tl.Failed()
	require.NotNil(t, first)
	require.Equal(t, testlogger.Error, first.Level)
	require.Equal(t, "first", first.Msg)
	require.NotEmpty(t, first.Stack)

	tl.Error("second")
	require.Same(t, first, tl.Failed(), "first failure should win")
}

// TestTestLogger_Failed_OnExpectedMatch is the soft-assert path: in
// FailOnExpectedErrorOnly mode, an Error matching a registered expectation
// (e.g. tag.FailedAssertion) should mark Failed().
func TestTestLogger_Failed_OnExpectedMatch(t *testing.T) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnExpectedErrorOnly)
	tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)
	require.Nil(t, tl.Failed())

	tl.Error("failed assertion: bad", tag.FailedAssertion)

	f := tl.Failed()
	require.NotNil(t, f)
	require.Equal(t, "failed assertion: bad", f.Msg)
}

// TestTestLogger_Failed_NoMatch verifies that FailOnExpectedErrorOnly remains an
// escape hatch: an Error with no matching expectation does not flip Failed().
func TestTestLogger_Failed_NoMatch(t *testing.T) {
	mt := &mockT{T: t}
	tl := testlogger.NewTestLogger(mt, testlogger.FailOnExpectedErrorOnly)
	require.Nil(t, tl.Failed())

	tl.Error("ignored")
	require.Nil(t, tl.Failed())
}

// TestTestLogger_WithWriter verifies that WithWriter routes log output to the
// supplied WriteSyncer instead of zaptest's TestingWriter.
func TestTestLogger_WithWriter(t *testing.T) {
	var buf bytes.Buffer
	tl := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly,
		testlogger.WithWriter(zapcore.AddSync(&buf)))

	tl.Info("hello-with-writer")

	require.Contains(t, buf.String(), "hello-with-writer")
}

// TestTestLogger_DontCloseOnCleanup verifies that with DontCloseOnCleanup set,
// the logger continues to deliver logs after the original T's test (and its
// Cleanup chain) has completed. Without the option the default Cleanup(tl.Close)
// would flip state.closed and the post-cleanup Info call would be dropped.
func TestTestLogger_DontCloseOnCleanup(t *testing.T) {
	var buf bytes.Buffer

	var tl *testlogger.TestLogger
	t.Run("inner", func(inner *testing.T) {
		tl = testlogger.NewTestLogger(inner, testlogger.FailOnExpectedErrorOnly,
			testlogger.WithWriter(zapcore.AddSync(&buf)),
			testlogger.WithoutCloseOnCleanup())
		tl.Info("during-inner")
	})
	// inner's Cleanup has run by here. Without DontCloseOnCleanup tl would be closed.
	tl.Info("after-inner")

	out := buf.String()
	require.Contains(t, out, "during-inner")
	require.Contains(t, out, "after-inner")
}
