package testcore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noFieldsSuite struct{}

func (*noFieldsSuite) TestA(t *testing.T) {}

type valueFieldsValidSuite struct {
	flag   bool
	count  int
	name   string
	ratio  float64
	ids    [3]int
	nested struct{ x int }
}

func (*valueFieldsValidSuite) TestA(t *testing.T) {}

type unexportedMapInvalidSuite struct {
	counts map[string]int //nolint:unused
}

func (*unexportedMapInvalidSuite) TestA(t *testing.T) {}

type exportedMapInvalidSuite struct {
	Data map[string]int
}

func (*exportedMapInvalidSuite) TestA(t *testing.T) {}

type sliceFieldInvalidSuite struct {
	items []int //nolint:unused
}

func (*sliceFieldInvalidSuite) TestA(t *testing.T) {}

type pointerFieldInvalidSuite struct {
	ptr *int //nolint:unused
}

func (*pointerFieldInvalidSuite) TestA(t *testing.T) {}

type noMethodsInvalidSuite struct{}

type wrongSigInvalidSuite struct{}

func (*wrongSigInvalidSuite) TestBad() {}

type badNameInvalidTests struct{}

func (*badNameInvalidTests) TestA(t *testing.T) {}

type nonTestMethodInvalidSuite struct{}

func (*nonTestMethodInvalidSuite) TestA(t *testing.T) {}
func (*nonTestMethodInvalidSuite) Helper()            {} //nolint:unused

func TestRunSuiteAcceptsSuite(t *testing.T) {
	tests := []struct {
		name  string
		suite any
	}{
		{"no fields", &noFieldsSuite{}},
		{"value-type fields", &valueFieldsValidSuite{flag: true, count: 42}},
	}

	for _, tt := range tests {
		// RunSuite calls t.Parallel() so each valid suite needs its own subtest.
		t.Run(tt.name+"Suite", func(t *testing.T) {
			assert.NotPanics(t, func() { RunSuite(t, tt.suite) }, tt.name)
		})
	}
}

func TestRunSuiteRejectsSuite(t *testing.T) {
	tests := []struct {
		name        string
		suite       any
		errContains string
	}{
		{"struct name not ending in Suite", &badNameInvalidTests{}, `suite struct name "badNameInvalidTests" must end with "Suite"`},
		{"unexported map field", &unexportedMapInvalidSuite{}, `field "counts" of type map[string]int which is not a value type`},
		{"exported map field", &exportedMapInvalidSuite{}, `field "Data" of type map[string]int which is not a value type`},
		{"slice field", &sliceFieldInvalidSuite{}, `field "items" of type []int which is not a value type`},
		{"pointer field", &pointerFieldInvalidSuite{}, `field "ptr" of type *int which is not a value type`},
		{"no Test methods", &noMethodsInvalidSuite{}, `suite noMethodsInvalidSuite has no Test* methods`},
		{"wrong method signature", &wrongSigInvalidSuite{}, `method wrongSigInvalidSuite.TestBad has wrong signature`},
		{"non-Test exported method", &nonTestMethodInvalidSuite{}, `exported method Helper that does not start with Test`},
	}

	for _, tt := range tests {
		// All invalid suites panic during validation, before RunSuite calls
		// t.Parallel(), so it's safe to reuse the parent t here.
		msg := fmt.Sprint(catchPanic(func() { RunSuite(t, tt.suite) }))
		require.Contains(t, msg, tt.errContains, "case %q", tt.name)
	}
}

// catchPanic calls fn and returns the recovered panic value, or nil if no panic.
func catchPanic(fn func()) (val any) {
	defer func() { val = recover() }()
	fn()
	return nil
}
