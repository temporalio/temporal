package await

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

type attemptFailed struct{}

// T is passed to the condition callback. It intercepts assertion failures
// so the polling loop can retry.
//
// Only use T for assertions (require.*, assert.*, t.Errorf, t.Fatal, t.FailNow).
type T struct {
	tb     testing.TB
	ctx    context.Context
	errors []string
	failed bool
}

// Context returns the await-scoped context for the current attempt.
func (t *T) Context() context.Context {
	if t.ctx != nil {
		return t.ctx
	}
	return t.tb.Context()
}

// Fail marks the current attempt as failed without stopping it.
func (t *T) Fail() {
	t.failed = true
}

// Error records an error message for reporting on timeout.
func (t *T) Error(args ...any) {
	t.Fail()
	t.errors = append(t.errors, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}

// Errorf records an error message for reporting on timeout.
func (t *T) Errorf(format string, args ...any) {
	t.Fail()
	t.errors = append(t.errors, fmt.Sprintf(format, args...))
}

// FailNow is called by require.* on failure. It stops the current attempt.
// Unlike testing.TB.FailNow(), this does NOT mark the test as failed.
func (t *T) FailNow() {
	t.Fail()
	panic(attemptFailed{})
}

// Fatal records an error message and stops this attempt.
func (t *T) Fatal(args ...any) {
	t.errors = append(t.errors, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
	t.FailNow()
}

// Fatalf records an error message and stops this attempt.
func (t *T) Fatalf(format string, args ...any) {
	t.Errorf(format, args...)
	t.FailNow()
}

// Failed reports whether this attempt has failed.
func (t *T) Failed() bool {
	return t.failed
}

// Helper marks the calling function as a test helper.
func (t *T) Helper() {
	if t.tb != nil {
		t.tb.Helper()
	}
}
