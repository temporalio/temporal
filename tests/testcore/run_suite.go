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

	methods := validateSuite(t, suite)

	// Mark as parallel only after all validation has passed.
	t.Parallel()

	v := reflect.ValueOf(suite)
	structType := v.Type().Elem()

	for _, method := range methods {
		fn := method.Func

		// Create a shallow copy so each test method gets its own instance,
		// preventing data races if a method accidentally mutates suite fields.
		cp := reflect.New(structType)
		cp.Elem().Set(v.Elem())

		t.Run(method.Name, func(t *testing.T) {
			fn.Call([]reflect.Value{cp, reflect.ValueOf(t)})
		})
	}
}

// validateSuite checks the suite struct and its methods, panicking if any
// requirements are violated. Returns the list of valid Test* methods.
func validateSuite(t *testing.T, suite any) []reflect.Method {
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

	// Verify all fields are value types to prevent shared mutable state.
	// Since test methods run in parallel, reference types (pointers, slices,
	// maps, etc.) would be shared even across shallow copies and could
	// cause data races that the race detector may not reliably catch.
	for i := 0; i < structType.NumField(); i++ {
		f := structType.Field(i)
		if !isValueType(f.Type) {
			panic(fmt.Sprintf(
				"RunSuite: suite %s has field %q of type %v which is not a value type; "+
					"suite fields must be value types since test methods run in parallel",
				structType.Name(), f.Name, f.Type,
			))
		}
	}

	tType := reflect.TypeOf((*testing.T)(nil))
	var methods []reflect.Method

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

		methods = append(methods, method)
	}

	if len(methods) == 0 {
		panic(fmt.Sprintf("RunSuite: suite %s has no Test* methods", structType.Name()))
	}

	return methods
}

// isValueType returns true if the type is safe to shallow-copy without sharing
// mutable state: primitives, strings, arrays of value types, and structs
// composed entirely of value types.
func isValueType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String:
		return true
	case reflect.Array:
		return isValueType(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !isValueType(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
