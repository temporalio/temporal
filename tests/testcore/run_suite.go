package testcore

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// RunSuite discovers and runs all exported Test* methods on the given struct.
// The passed in test is marked as parallel automatically.
//
// Each method is run as a subtest via t.Run.
// Methods must have the signature: func(s *MySuite) TestFoo(t *testing.T)
//
// Because subtests run in parallel, there is no suite-level Setup or Teardown.
// Each test method should create its own [TestEnv] and manage its own state.
func RunSuite(t *testing.T, suite any) {
	t.Helper()
	t.Parallel()

	v := reflect.ValueOf(suite)
	typ := v.Type()

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("RunSuite: suite must be a pointer to a struct, got %v", typ))
	}
	structType := typ.Elem()

	if !strings.HasSuffix(structType.Name(), "Suite") {
		panic(fmt.Sprintf("RunSuite: suite struct name %q must end with \"Suite\"", structType.Name()))
	}
	if !strings.HasSuffix(t.Name(), "Suite") {
		panic(fmt.Sprintf("RunSuite: test name %q must end with \"Suite\"", t.Name()))
	}

	tType := reflect.TypeOf((*testing.T)(nil))

	var testCount int
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		name := method.Name

		if !strings.HasPrefix(name, "Test") {
			// Exported method that doesn't start with "Test" — likely a mistake.
			panic(fmt.Sprintf(
				"RunSuite: suite %s has exported method %s that does not start with Test; "+
					"rename it to start with Test or make it unexported",
				structType.Name(), name,
			))
		}

		// Validate signature: must be func(receiver, *testing.T) with no returns.
		if method.Type.NumIn() != 2 || method.Type.In(1) != tType || method.Type.NumOut() != 0 {
			panic(fmt.Sprintf(
				"RunSuite: method %s.%s has wrong signature; expected func(*testing.T), got %v",
				structType.Name(), name, method.Type,
			))
		}

		testCount++
		fn := method.Func

		t.Run(name, func(t *testing.T) {
			fn.Call([]reflect.Value{v, reflect.ValueOf(t)})
		})
	}

	if testCount == 0 {
		panic(fmt.Sprintf("RunSuite: suite %s has no Test* methods", structType.Name()))
	}
}
