package testcore

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// runSuiteFixedParams is the number of fixed parameters in a Test* method signature: receiver + *testing.T.
const runSuiteFixedParams = 2

// RunSuite discovers and runs all exported Test* methods on the given struct.
// The passed in test is marked as parallel automatically.
//
// Each method is run as a subtest via t.Run.
// Methods must have the signature: func(MySuite) TestFoo(t *testing.T, args...)
// where args match the types of the extra arguments passed to RunSuite.
//
// The suite struct must have no fields. Use args to pass parameters instead.
//
// Because subtests run in parallel, there is no suite-level Setup or Teardown.
// Each test method should create its own [TestEnv] and manage its own state.
func RunSuite(t *testing.T, suite any, args ...any) {
	t.Helper()

	v, methods := validateSuite(t, suite, args)

	// Mark as parallel only after all validation has passed.
	t.Parallel()

	argVals := make([]reflect.Value, len(args))
	for i, a := range args {
		argVals[i] = reflect.ValueOf(a)
	}

	for _, method := range methods {
		t.Run(method.Name, func(t *testing.T) {
			callArgs := append([]reflect.Value{v, reflect.ValueOf(t)}, argVals...)
			method.Func.Call(callArgs)
		})
	}
}

// validateSuite checks the suite struct and its methods, panicking if any
// requirements are violated. Returns the list of valid Test* methods.
func validateSuite(t *testing.T, suite any, args []any) (reflect.Value, []reflect.Method) {
	v := reflect.ValueOf(suite)
	typ := v.Type()

	if typ.Kind() != reflect.Struct {
		panic(fmt.Sprintf("RunSuite: suite must be a struct, got %v", typ))
	}
	if !strings.HasSuffix(typ.Name(), "Suite") {
		panic(fmt.Sprintf("RunSuite: suite struct name %q must end with \"Suite\"", typ.Name()))
	}
	if !strings.HasSuffix(t.Name(), "Suite") {
		panic(fmt.Sprintf("RunSuite: test name %q must end with \"Suite\"", t.Name()))
	}
	if typ.NumField() != 0 {
		panic(fmt.Sprintf(
			"RunSuite: suite %s must have no fields; pass parameters as extra args to RunSuite instead",
			typ.Name(),
		))
	}
	ptrType := reflect.PointerTo(typ)
	if typ.NumMethod() != ptrType.NumMethod() {
		panic(fmt.Sprintf(
			"RunSuite: suite %s has pointer-receiver methods; all methods must use value receivers",
			typ.Name(),
		))
	}

	tType := reflect.TypeOf((*testing.T)(nil))
	expectedNumIn := runSuiteFixedParams + len(args)

	var methods []reflect.Method
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		name := method.Name

		if !strings.HasPrefix(name, "Test") {
			// Exported method that doesn't start with "Test" — likely a mistake.
			panic(fmt.Sprintf(
				"RunSuite: suite %s has exported method %s that does not start with Test; "+
					"rename it to start with Test or make it unexported",
				typ.Name(), name,
			))
		}

		// Validate signature: func(receiver, *testing.T, args...) with no returns.
		mt := method.Type
		if mt.NumOut() != 0 {
			panic(fmt.Sprintf(
				"RunSuite: method %s.%s must not have return values, got %v",
				typ.Name(), name, mt,
			))
		}
		if mt.NumIn() != expectedNumIn || mt.In(1) != tType {
			panic(fmt.Sprintf(
				"RunSuite: method %s.%s has wrong signature; got %v",
				typ.Name(), name, mt,
			))
		}

		// Validate that each extra arg's type is assignable to the method parameter.
		for j, a := range args {
			paramType := mt.In(runSuiteFixedParams + j)
			argType := reflect.TypeOf(a)
			if !argType.AssignableTo(paramType) {
				panic(fmt.Sprintf(
					"RunSuite: method %s.%s parameter %d has type %v but RunSuite arg has type %v",
					typ.Name(), name, j+1, paramType, argType,
				))
			}
		}

		methods = append(methods, method)
	}

	if len(methods) == 0 {
		panic(fmt.Sprintf("RunSuite: suite %s has no Test* methods", typ.Name()))
	}

	return v, methods
}
