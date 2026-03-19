package testcore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noFieldsSuite struct{}

func (noFieldsSuite) TestA(t *testing.T) {}

type withArgsSuite struct{}

func (withArgsSuite) TestA(t *testing.T, name string, count int) {
	require.Equal(t, "hello", name)
	require.Equal(t, 42, count)
}

type hasFieldsSuite struct {
	x int //nolint:unused
}

func (hasFieldsSuite) TestA(t *testing.T) {}

type badNameInvalidTests struct{}

func (*badNameInvalidTests) TestA(t *testing.T) {}

type noMethodsInvalidSuite struct{}

type wrongSigInvalidSuite struct{}

func (wrongSigInvalidSuite) TestBad() {}

type nonTestMethodInvalidSuite struct{}

func (nonTestMethodInvalidSuite) TestA(t *testing.T) {}
func (nonTestMethodInvalidSuite) Helper()            {} //nolint:unused

type argMismatchSuite struct{}

func (argMismatchSuite) TestA(t *testing.T, x int) {}

type pointerReceiverSuite struct{}

func (*pointerReceiverSuite) TestA(t *testing.T) {} //nolint:unused

func TestRunSuiteAcceptsSuite(t *testing.T) {
	tests := []struct {
		name  string
		suite any
		args  []any
	}{
		{"no fields no args", noFieldsSuite{}, nil},
		{"with args", withArgsSuite{}, []any{"hello", 42}},
	}

	for _, tt := range tests {
		t.Run(tt.name+"Suite", func(t *testing.T) {
			assert.NotPanics(t, func() { RunSuite(t, tt.suite, tt.args...) }, tt.name)
		})
	}
}

func TestRunSuiteRejectsSuite(t *testing.T) {
	tests := []struct {
		name        string
		suite       any
		args        []any
		errContains string
	}{
		{"not a struct", 42, nil,
			`suite must be a struct`},
		{"struct name not ending in Suite", badNameInvalidTests{}, nil,
			`suite struct name "badNameInvalidTests" must end with "Suite"`},
		{"has fields", hasFieldsSuite{}, nil,
			`suite hasFieldsSuite must have no fields`},
		{"no Test methods", noMethodsInvalidSuite{}, nil,
			`suite noMethodsInvalidSuite has no Test* methods`},
		{"wrong method signature", wrongSigInvalidSuite{}, nil,
			`method wrongSigInvalidSuite.TestBad has wrong signature`},
		{"non-Test exported method", nonTestMethodInvalidSuite{}, nil,
			`exported method Helper that does not start with Test`},
		{"arg count mismatch", noFieldsSuite{}, []any{"extra"},
			`has wrong signature`},
		{"arg type mismatch", argMismatchSuite{}, []any{"not-an-int"},
			`parameter 1 has type int but RunSuite arg has type string`},
		{"pointer receiver", pointerReceiverSuite{}, nil,
			`has pointer-receiver methods; all methods must use value receivers`},
	}

	for _, tt := range tests {
		// All invalid suites panic during validation, before RunSuite calls
		// t.Parallel(), so it's safe to reuse the parent t here.
		msg := fmt.Sprint(catchPanic(func() { RunSuite(t, tt.suite, tt.args...) }))
		require.Contains(t, msg, tt.errContains, "case %q", tt.name)
	}
}

// catchPanic calls fn and returns the recovered panic value, or nil if no panic.
func catchPanic(fn func()) (val any) {
	defer func() { val = recover() }()
	fn()
	return nil
}
