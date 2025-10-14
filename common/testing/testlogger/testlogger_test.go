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
			assertMatches(t, level, "message with tags", []tag.Tag{tag.NewStringTag("key", "value")})
		})
		t.Run(level.String()+" without tags", func(t *testing.T) {
			assertMatches(t, level, "message without tags", nil)
		})
		t.Run(level.String()+" no message only tags", func(t *testing.T) {
			assertMatches(t, level, "", []tag.Tag{tag.NewStringTag("key", "value")})
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
			assertFails(t, level, "message with tags", []tag.Tag{tag.NewStringTag("key", "value")})
		})
		t.Run(level.String()+" without tags", func(t *testing.T) {
			assertFails(t, level, "message without tags", nil)
		})
		t.Run(level.String()+" no message only tags", func(t *testing.T) {
			assertFails(t, level, "", []tag.Tag{tag.NewStringTag("key", "value")})
		})
	}
	// Panicking levels
	for _, level := range []testlogger.Level{testlogger.Panic, testlogger.Fatal} {
		t.Run(level.String()+" with tags", func(t *testing.T) {
			require.Panics(t, func() {
				assertFails(t, level, "message with tags", []tag.Tag{tag.NewStringTag("key", "value")})
			})
		})
		t.Run(level.String()+" without tags", func(t *testing.T) {
			require.Panics(t, func() {
				assertFails(t, level, "message without tags", nil)
			})
		})
		t.Run(level.String()+" no message only tags", func(t *testing.T) {
			require.Panics(t, func() {
				assertFails(t, level, "", []tag.Tag{tag.NewStringTag("key", "value")})
			})
		})
	}
}
